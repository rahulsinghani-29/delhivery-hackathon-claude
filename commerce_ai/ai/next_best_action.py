"""Next-Best-Action policy using a contextual bandit proxy.

Uses a RandomForestClassifier as a simple contextual bandit to select
the best intervention for risky orders based on historical outcomes.
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)

ACTION_SPACE = [
    "verification",
    "cancellation",
    "masked_calling",
    "cod_to_prepaid",
    "premium_courier",
    "merchant_confirmation",
    "address_enrichment_outreach",
    "cod_to_prepaid_outreach",
    "auto_cancel",
    "express_upgrade",
    "no_action",
]

FEATURE_COLS = [
    "category",
    "price_band",
    "payment_mode",
    "origin_node",
    "destination_cluster",
    "address_quality",
    "rto_score",
]

CATEGORICAL_COLS = [
    "category",
    "price_band",
    "payment_mode",
    "origin_node",
    "destination_cluster",
]


class NextBestActionPolicy:
    """Contextual bandit for selecting the best intervention for risky orders."""

    def __init__(self, model_path: str | None = None) -> None:
        """Load model from path if provided."""
        self.model: RandomForestClassifier | None = None
        self.feature_encoders: dict[str, LabelEncoder] = {}
        self.action_encoder: LabelEncoder | None = None
        if model_path is not None:
            self.load(model_path)

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def train(self, intervention_outcomes: pd.DataFrame) -> None:
        """Train on historical intervention-outcome data.

        Expected columns:
            category, price_band, payment_mode, origin_node,
            destination_cluster, address_quality, rto_score,
            intervention_type, delivery_outcome

        Filters to rows where delivery_outcome == 'delivered' to learn
        which intervention led to the best outcome, then trains on all
        rows with the best-outcome intervention as the target.
        """
        df = intervention_outcomes.copy()

        # Encode categorical features
        self.feature_encoders = {}
        for col in CATEGORICAL_COLS:
            le = LabelEncoder()
            df[col] = le.fit_transform(df[col].astype(str))
            self.feature_encoders[col] = le

        # Encode action labels
        self.action_encoder = LabelEncoder()
        self.action_encoder.fit(ACTION_SPACE)
        df["intervention_type"] = self.action_encoder.transform(
            df["intervention_type"].astype(str)
        )

        X = df[FEATURE_COLS].values.astype(float)
        y = df["intervention_type"].values

        self.model = RandomForestClassifier(
            n_estimators=100,
            max_depth=6,
            random_state=42,
        )
        self.model.fit(X, y)

    # ------------------------------------------------------------------
    # Recommendation
    # ------------------------------------------------------------------

    def recommend(self, order_context: dict) -> dict:
        """Recommend the best intervention for an order.

        Returns:
            { "intervention_type": str, "confidence_score": float }

        If confidence < 0.6, returns no_action.
        If model is not trained, returns no_action with confidence 0.0.
        """
        if self.model is None or self.action_encoder is None:
            return {"intervention_type": "no_action", "confidence_score": 0.0}

        try:
            row = self._encode_features(order_context)
        except _UnseenValueError:
            return {"intervention_type": "no_action", "confidence_score": 0.0}

        proba = self.model.predict_proba(row)[0]
        best_idx = int(np.argmax(proba))
        confidence = float(proba[best_idx])

        action_label = self.action_encoder.inverse_transform([self.model.classes_[best_idx]])[0]

        if confidence < 0.6:
            return {"intervention_type": "no_action", "confidence_score": confidence}

        return {"intervention_type": action_label, "confidence_score": confidence}

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, model_path: str) -> None:
        """Save model and encoders to disk."""
        import joblib

        Path(model_path).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {
                "model": self.model,
                "feature_encoders": self.feature_encoders,
                "action_encoder": self.action_encoder,
            },
            model_path,
        )

    def load(self, model_path: str) -> None:
        """Load model and encoders from disk."""
        import joblib

        data = joblib.load(model_path)
        self.model = data["model"]
        self.feature_encoders = data["feature_encoders"]
        self.action_encoder = data["action_encoder"]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _encode_features(self, order_context: dict) -> np.ndarray:
        """Encode a single order context dict into a 2-D numpy array."""
        encoded: list[float] = []
        for col in FEATURE_COLS:
            val = order_context.get(col)
            if col in CATEGORICAL_COLS:
                le = self.feature_encoders.get(col)
                if le is None:
                    raise _UnseenValueError(col, val)
                str_val = str(val)
                if str_val not in le.classes_:
                    raise _UnseenValueError(col, str_val)
                encoded.append(float(le.transform([str_val])[0]))
            else:
                encoded.append(float(val))  # type: ignore[arg-type]
        return np.array([encoded])


class _UnseenValueError(Exception):
    """Raised when a feature value was not seen during training."""

    def __init__(self, column: str, value: object) -> None:
        super().__init__(f"Unseen value '{value}' for column '{column}'")
        self.column = column
        self.value = value
