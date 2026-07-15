from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.mixture import GaussianMixture
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


@dataclass
class RegimeModel:
    columns: tuple[str, ...] = ("vix_level", "trm_realized_vol_20", "carry_to_risk")
    random_state: int = 20260715

    def __post_init__(self) -> None:
        self.pipeline = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
                ("gmm", GaussianMixture(n_components=3, covariance_type="full", random_state=self.random_state)),
            ]
        )
        self.label_map: dict[int, str] = {}

    def fit(self, features: pd.DataFrame) -> "RegimeModel":
        X = features.loc[:, list(self.columns)]
        self.pipeline.fit(X)
        labels = self.pipeline.predict(X)
        stress_score = pd.to_numeric(features.get("vix_level"), errors="coerce").fillna(0)
        means = pd.DataFrame({"label": labels, "stress": stress_score}).groupby("label")["stress"].mean()
        ordered = list(means.sort_values().index)
        names = ["carry_risk_on", "neutral", "stress_carry_unwind"]
        self.label_map = dict(zip(ordered, names, strict=True))
        return self

    def predict(self, features: pd.DataFrame) -> pd.DataFrame:
        labels = self.pipeline.predict(features.loc[:, list(self.columns)])
        probabilities = self.pipeline.predict_proba(features.loc[:, list(self.columns)])
        output = pd.DataFrame(index=features.index)
        output["regime"] = [self.label_map.get(int(label), f"regime_{label}") for label in labels]
        for component in range(probabilities.shape[1]):
            name = self.label_map.get(component, f"regime_{component}")
            output[f"prob_{name}"] = probabilities[:, component]
        return output
