"""CSV/JSON data loading with malformed row handling and summary reporting."""

from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Tuple


@dataclass
class LoadResult:
    """Result of loading a single data file into a table."""

    table: str
    loaded: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Required / optional field definitions per table
# ---------------------------------------------------------------------------

_MERCHANT_FIELDS = ("merchant_id", "name")

_WAREHOUSE_NODE_FIELDS = ("node_id", "merchant_id", "city", "state", "pincode")

_ORDER_REQUIRED_FIELDS = (
    "order_id",
    "merchant_id",
    "customer_ucid",
    "category",
    "price_band",
    "payment_mode",
    "origin_node",
    "destination_pincode",
    "destination_cluster",
    "address_quality",
    "rto_score",
    "delivery_outcome",
    "created_at",
)

_INTERVENTION_FIELDS = (
    "intervention_id",
    "order_id",
    "merchant_id",
    "intervention_type",
    "action_owner",
    "initiated_by",
    "executed_at",
)

_INTERVENTION_OPTIONAL = ("confidence_score", "outcome", "completed_at")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _read_rows(file_path: str) -> list[dict]:
    """Read rows from a CSV or JSON file based on extension."""
    p = Path(file_path)
    ext = p.suffix.lower()
    if ext == ".csv":
        with open(p, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)
    elif ext == ".json":
        with open(p, encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            raise ValueError("JSON file must contain a top-level array")
    else:
        raise ValueError(f"Unsupported file extension: {ext}")


def _validate_required(row: dict, required: tuple[str, ...], row_num: int) -> list[str]:
    """Return a list of error strings for missing or empty required fields."""
    errors: list[str] = []
    for field_name in required:
        val = row.get(field_name)
        if val is None or (isinstance(val, str) and val.strip() == ""):
            errors.append(f"Row {row_num}: missing field '{field_name}'")
    return errors


def _to_float(value: str | None, field_name: str, row_num: int) -> tuple[float | None, str | None]:
    """Try to convert a value to float. Returns (value, error_string | None)."""
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return None, f"Row {row_num}: missing field '{field_name}'"
    if isinstance(value, (int, float)):
        return float(value), None
    try:
        return float(value), None
    except (ValueError, TypeError):
        return None, f"Row {row_num}: invalid type for '{field_name}' (expected number, got '{value}')"


def _to_bool(value: str | None, default: bool = True) -> bool:
    """Convert a string value to bool."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return value.strip().lower() in ("true", "1", "yes")


# ---------------------------------------------------------------------------
# Public loaders
# ---------------------------------------------------------------------------


def load_merchants(db: sqlite3.Connection, csv_path: str) -> LoadResult:
    """Load merchants from a CSV or JSON file."""
    result = LoadResult(table="merchants")
    rows = _read_rows(csv_path)

    for i, row in enumerate(rows, start=1):
        errs = _validate_required(row, _MERCHANT_FIELDS, i)
        if errs:
            result.skipped += 1
            result.errors.extend(errs)
            continue

        try:
            db.execute(
                "INSERT OR REPLACE INTO merchants (merchant_id, name) VALUES (?, ?)",
                (row["merchant_id"].strip(), row["name"].strip()),
            )
            result.loaded += 1
        except sqlite3.Error as exc:
            result.skipped += 1
            result.errors.append(f"Row {i}: database error — {exc}")

    db.commit()
    return result


def load_warehouse_nodes(db: sqlite3.Connection, csv_path: str) -> LoadResult:
    """Load warehouse nodes from a CSV or JSON file."""
    result = LoadResult(table="warehouse_nodes")
    rows = _read_rows(csv_path)

    for i, row in enumerate(rows, start=1):
        errs = _validate_required(row, _WAREHOUSE_NODE_FIELDS, i)
        if errs:
            result.skipped += 1
            result.errors.extend(errs)
            continue

        is_active = _to_bool(row.get("is_active"))

        try:
            db.execute(
                "INSERT OR REPLACE INTO warehouse_nodes "
                "(node_id, merchant_id, city, state, pincode, is_active) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    row["node_id"].strip(),
                    row["merchant_id"].strip(),
                    row["city"].strip(),
                    row["state"].strip(),
                    row["pincode"].strip(),
                    is_active,
                ),
            )
            result.loaded += 1
        except sqlite3.Error as exc:
            result.skipped += 1
            result.errors.append(f"Row {i}: database error — {exc}")

    db.commit()
    return result



def load_orders(db: sqlite3.Connection, csv_path: str) -> LoadResult:
    """Load orders from a CSV or JSON file.

    Required fields: order_id, merchant_id, customer_ucid, category, price_band,
    payment_mode, origin_node, destination_pincode, destination_cluster,
    address_quality, rto_score, delivery_outcome, created_at.
    Optional: shipping_mode (default "surface").
    """
    result = LoadResult(table="orders")
    rows = _read_rows(csv_path)

    for i, row in enumerate(rows, start=1):
        errs = _validate_required(row, _ORDER_REQUIRED_FIELDS, i)
        if errs:
            result.skipped += 1
            result.errors.extend(errs)
            continue

        # Validate numeric fields
        addr_q, addr_err = _to_float(row["address_quality"], "address_quality", i)
        rto, rto_err = _to_float(row["rto_score"], "rto_score", i)

        type_errors = [e for e in (addr_err, rto_err) if e is not None]
        if type_errors:
            result.skipped += 1
            result.errors.extend(type_errors)
            continue

        shipping_mode = (row.get("shipping_mode") or "surface").strip() or "surface"

        try:
            db.execute(
                "INSERT OR REPLACE INTO orders "
                "(order_id, merchant_id, customer_ucid, category, price_band, "
                "payment_mode, origin_node, destination_pincode, destination_cluster, "
                "address_quality, rto_score, delivery_outcome, shipping_mode, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row["order_id"].strip(),
                    row["merchant_id"].strip(),
                    row["customer_ucid"].strip(),
                    row["category"].strip(),
                    row["price_band"].strip(),
                    row["payment_mode"].strip(),
                    row["origin_node"].strip(),
                    row["destination_pincode"].strip(),
                    row["destination_cluster"].strip(),
                    addr_q,
                    rto,
                    row["delivery_outcome"].strip(),
                    shipping_mode,
                    row["created_at"].strip(),
                ),
            )
            result.loaded += 1
        except sqlite3.Error as exc:
            result.skipped += 1
            result.errors.append(f"Row {i}: database error — {exc}")

    db.commit()
    return result


def load_interventions(db: sqlite3.Connection, csv_path: str) -> LoadResult:
    """Load interventions from a CSV or JSON file."""
    result = LoadResult(table="interventions")
    rows = _read_rows(csv_path)

    for i, row in enumerate(rows, start=1):
        errs = _validate_required(row, _INTERVENTION_FIELDS, i)
        if errs:
            result.skipped += 1
            result.errors.extend(errs)
            continue

        # Optional numeric field
        conf_score: float | None = None
        raw_conf = row.get("confidence_score")
        if raw_conf is not None and str(raw_conf).strip() != "":
            conf_score, conf_err = _to_float(raw_conf, "confidence_score", i)
            if conf_err:
                result.skipped += 1
                result.errors.append(conf_err)
                continue

        outcome = row.get("outcome")
        if outcome is not None:
            outcome = outcome.strip() or None
        completed_at = row.get("completed_at")
        if completed_at is not None:
            completed_at = completed_at.strip() or None

        try:
            db.execute(
                "INSERT OR REPLACE INTO interventions "
                "(intervention_id, order_id, merchant_id, intervention_type, "
                "action_owner, initiated_by, confidence_score, outcome, "
                "executed_at, completed_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row["intervention_id"].strip(),
                    row["order_id"].strip(),
                    row["merchant_id"].strip(),
                    row["intervention_type"].strip(),
                    row["action_owner"].strip(),
                    row["initiated_by"].strip(),
                    conf_score,
                    outcome,
                    row["executed_at"].strip(),
                    completed_at,
                ),
            )
            result.loaded += 1
        except sqlite3.Error as exc:
            result.skipped += 1
            result.errors.append(f"Row {i}: database error — {exc}")

    db.commit()
    return result


# ---------------------------------------------------------------------------
# Aggregate loader
# ---------------------------------------------------------------------------

_FILE_LOADERS = {
    "merchants": load_merchants,
    "warehouse_nodes": load_warehouse_nodes,
    "orders": load_orders,
    "interventions": load_interventions,
}


def load_all(db: sqlite3.Connection, csv_dir: str) -> dict[str, LoadResult]:
    """Load all recognised data files from *csv_dir*.

    Looks for files named ``merchants.csv``, ``warehouse_nodes.csv``,
    ``orders.csv``, ``interventions.csv`` (or ``.json`` variants).
    Returns a dict mapping table name → LoadResult.
    """
    dir_path = Path(csv_dir)
    results: dict[str, LoadResult] = {}

    for table_name, loader_fn in _FILE_LOADERS.items():
        file_path: Path | None = None
        for ext in (".csv", ".json"):
            candidate = dir_path / f"{table_name}{ext}"
            if candidate.exists():
                file_path = candidate
                break

        if file_path is None:
            continue

        results[table_name] = loader_fn(db, str(file_path))

    _print_summary(results)
    return results


def _print_summary(results: dict[str, LoadResult]) -> None:
    """Print a human-readable summary of all load results."""
    print("\n=== Data Load Summary ===")
    for table, res in results.items():
        status = "OK" if res.skipped == 0 else "WARNINGS"
        print(f"  {table}: {res.loaded} loaded, {res.skipped} skipped [{status}]")
        for err in res.errors:
            print(f"    ⚠ {err}")
    print("=========================\n")
