"""Realized Commerce Score model using XGBoost.

Predicts delivery success probability for order cohorts based on
categorical and continuous features from historical order data.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier

logger = logging.getLogger(__name__)

FEATURE_COLS = [
    "category",
    "price_band",
    "payment_mode",
    "origin_node",
    "destination_cluster",
    "address_quality",
]

CATEGORICAL_COLS = [
    "category",
    "price_band",
    "payment_mode",
    "origin_node",
    "destination_cluster",
]


class RealizedCommerceScorer:
    """XGBoost-based model predicting delivery success probability for order cohorts."""

    def __init__(self, model_path: str | None = None) -> None:
        """Load model from path if provided, otherwise start with no model."""
        self.model: XGBClassifier | None = None
        self.encoders: dict[str, LabelEncoder] = {}
        if model_path is not None:
            self.load(model_path)

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def train(self, orders_df: pd.DataFrame) -> None:
        """Train the XGBoost model on historical order data.

        Features: category, price_band, payment_mode, origin_node,
                  destination_cluster, address_quality
        Target: 1 if delivery_outcome == 'delivered', 0 otherwise
        """
        df = orders_df.copy()

        # Build binary target
        df["target"] = (df["delivery_outcome"] == "delivered").astype(int)

        # Fit label encoders for categorical columns
        self.encoders = {}
        for col in CATEGORICAL_COLS:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].astype(str))
            self.encoders[col] = le

        X = df[FEATURE_COLS].values
        y = df["target"].values

        self.model = XGBClassifier(
            objective="binary:logistic",
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            eval_metric="logloss",
        )
        self.model.fit(X, y)

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict(self, cohort_features: dict) -> float:
        """Predict delivery success probability for a single cohort.

        Args:
            cohort_features: dict with keys matching FEATURE_COLS.

        Returns:
            float between 0 and 1 (realized commerce score).
            Returns 0.5 fallback if model is not trained or category is unseen.
        """
        if self.model is None:
            return 0.5

        try:
            row = self._encode_features(cohort_features)
        except _UnseenCategoryError:
            return 0.5

        prob = float(self.model.predict_proba(row)[0, 1])
        return float(np.clip(prob, 0.0, 1.0))

    # ------------------------------------------------------------------
    # Ranking
    # ------------------------------------------------------------------

    def rank_cohorts(self, cohorts: list[dict]) -> list[dict]:
        """Score and rank multiple cohorts by realized commerce score descending.

        Each dict in *cohorts* must contain the feature keys plus an
        ``order_count`` field.

        Returns list of dicts with cohort_key, realized_commerce_score,
        is_low_confidence, order_count — sorted descending by score.
        """
        results: list[dict] = []
        for cohort in cohorts:
            order_count = cohort.get("order_count", 0)
            score = self.predict(cohort)
            low_conf = self.is_low_confidence(order_count)

            # If unseen category triggered fallback, also mark low confidence
            if score == 0.5 and self.model is not None:
                low_conf = True

            results.append(
                {
                    "cohort_key": {
                        k: cohort[k]
                        for k in [
                            "category",
                            "price_band",
                            "payment_mode",
                            "origin_node",
                            "destination_cluster",
                        ]
                        if k in cohort
                    },
                    "realized_commerce_score": score,
                    "is_low_confidence": low_conf,
                    "order_count": order_count,
                }
            )

        results.sort(key=lambda r: r["realized_commerce_score"], reverse=True)
        return results

    # ------------------------------------------------------------------
    # Confidence check
    # ------------------------------------------------------------------

    @staticmethod
    def is_low_confidence(order_count: int, min_orders: int = 50) -> bool:
        """Return True if cohort has fewer than *min_orders* historical orders."""
        return order_count < min_orders

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, model_path: str) -> None:
        """Save model and encoders to disk using joblib."""
        import joblib

        Path(model_path).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump({"model": self.model, "encoders": self.encoders}, model_path)

    def load(self, model_path: str) -> None:
        """Load model and encoders from disk using joblib."""
        import joblib

        data = joblib.load(model_path)
        self.model = data["model"]
        self.encoders = data["encoders"]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _encode_features(self, cohort_features: dict) -> np.ndarray:
        """Encode a single feature dict into a 2-D numpy array for prediction.

        Raises _UnseenCategoryError if any categorical value was not seen
        during training.
        """
        encoded: list[Any] = []
        for col in FEATURE_COLS:
            val = cohort_features.get(col)
            if col in CATEGORICAL_COLS:
                le = self.encoders.get(col)
                if le is None:
                    raise _UnseenCategoryError(col, val)
                str_val = str(val)
                if str_val not in le.classes_:
                    raise _UnseenCategoryError(col, str_val)
                encoded.append(le.transform([str_val])[0])
            else:
                encoded.append(float(val))  # type: ignore[arg-type]
        return np.array([encoded])


class _UnseenCategoryError(Exception):
    """Raised when a categorical value was not seen during training."""

    def __init__(self, column: str, value: Any) -> None:
        super().__init__(f"Unseen value '{value}' for column '{column}'")
        self.column = column
        self.value = value
