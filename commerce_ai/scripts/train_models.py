"""Train and persist both AI models from historical order data.

Usage:
    cd commerce_ai
    python -m scripts.train_models                  # uses sample CSVs
    python -m scripts.train_models --db commerce_ai.db  # uses live SQLite

Outputs:
    models/scorer.pkl   — RealizedCommerceScorer (XGBoost)
    models/nba.pkl      — NextBestActionPolicy (RandomForest)
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path

import pandas as pd

# Ensure project root is in sys.path when run as a script
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ai.next_best_action import NextBestActionPolicy
from ai.scoring import RealizedCommerceScorer
import config as cfg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger("train_models")

MODELS_DIR = ROOT / "models"
SAMPLE_ORDERS = ROOT / "data" / "sample" / "orders.csv"
SAMPLE_INTERVENTIONS = ROOT / "data" / "sample" / "interventions.csv"


# ---------------------------------------------------------------------------
# Data loading helpers
# ---------------------------------------------------------------------------

def _load_from_csv() -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Loading orders from %s", SAMPLE_ORDERS)
    orders = pd.read_csv(SAMPLE_ORDERS)
    logger.info("Loaded %d orders", len(orders))

    if SAMPLE_INTERVENTIONS.exists():
        logger.info("Loading interventions from %s", SAMPLE_INTERVENTIONS)
        interventions = pd.read_csv(SAMPLE_INTERVENTIONS)
        logger.info("Loaded %d interventions", len(interventions))
    else:
        logger.warning("No interventions CSV found — NBA model will use synthetic labels")
        interventions = pd.DataFrame()

    return orders, interventions


def _load_from_db(db_path: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Loading orders from SQLite: %s", db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    orders = pd.read_sql_query("SELECT * FROM orders", conn)
    logger.info("Loaded %d orders", len(orders))

    interventions = pd.read_sql_query("SELECT * FROM interventions", conn)
    logger.info("Loaded %d interventions", len(interventions))
    conn.close()
    return orders, interventions


# ---------------------------------------------------------------------------
# NBA label generation
# ---------------------------------------------------------------------------

def _generate_nba_labels(
    orders: pd.DataFrame, interventions: pd.DataFrame
) -> pd.DataFrame:
    """Join orders to their best intervention outcome, or synthesise labels.

    For orders with a logged intervention that ended in a positive outcome,
    use that intervention as the label. For all other risky orders, use a
    rule-based heuristic:
      - address_quality < 0.5  → address_enrichment_outreach
      - COD + high rto_score   → cod_to_prepaid_outreach
      - extreme rto_score      → verification
      - otherwise              → no_action
    """
    risky = orders[orders["rto_score"] > cfg.RISK_THRESHOLD].copy()

    if not interventions.empty and "order_id" in interventions.columns:
        # Take the latest logged intervention per order with a positive outcome
        positive = interventions[interventions["outcome"].isin(["completed", "upgraded", "cancelled"])]
        best = (
            positive.sort_values("executed_at", ascending=False)
            .drop_duplicates(subset=["order_id"])
            [["order_id", "intervention_type"]]
        )
        risky = risky.merge(best, on="order_id", how="left")
    else:
        risky["intervention_type"] = None

    # Fill missing labels with heuristic rules
    def _rule(row):
        if pd.notna(row.get("intervention_type")):
            return row["intervention_type"]
        if row.get("address_quality", 1.0) < cfg.ADDRESS_QUALITY_THRESHOLD:
            return "address_enrichment_outreach"
        if row.get("payment_mode") == "COD" and row.get("rto_score", 0) > 0.6:
            return "cod_to_prepaid_outreach"
        if row.get("rto_score", 0) > 0.75:
            return "verification"
        return "no_action"

    risky["intervention_type"] = risky.apply(_rule, axis=1)
    return risky


# ---------------------------------------------------------------------------
# Main training routine
# ---------------------------------------------------------------------------

def train(db_path: str | None = None) -> None:
    MODELS_DIR.mkdir(exist_ok=True)

    # 1. Load data
    if db_path:
        orders, interventions = _load_from_db(db_path)
    else:
        orders, interventions = _load_from_csv()

    if orders.empty:
        logger.error("No order data found — cannot train models")
        sys.exit(1)

    # 2. Train RealizedCommerceScorer (XGBoost)
    logger.info("Training RealizedCommerceScorer …")
    scorer = RealizedCommerceScorer()
    scorer.train(orders)
    scorer_path = str(MODELS_DIR / "scorer.pkl")
    scorer.save(scorer_path)
    logger.info("Saved scorer → %s", scorer_path)

    # 3. Build NBA training set
    logger.info("Building NBA training set …")
    nba_data = _generate_nba_labels(orders, interventions)
    if nba_data.empty:
        logger.warning("No risky orders found — NBA model will use fallback scoring only")
    else:
        logger.info("NBA training set: %d rows", len(nba_data))

        # 4. Train NextBestActionPolicy (RandomForest)
        logger.info("Training NextBestActionPolicy …")
        nba = NextBestActionPolicy()
        nba.train(nba_data)
        nba_path = str(MODELS_DIR / "nba.pkl")
        nba.save(nba_path)
        logger.info("Saved NBA model → %s", nba_path)

    logger.info("Training complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Commerce AI models")
    parser.add_argument(
        "--db",
        metavar="PATH",
        default=None,
        help="Path to SQLite DB. Omit to use sample CSVs.",
    )
    args = parser.parse_args()
    train(db_path=args.db)
