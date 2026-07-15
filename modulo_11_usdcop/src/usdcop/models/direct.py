from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.linear_model import ElasticNetCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


@dataclass
class DirectElasticNetForecaster:
    horizons: tuple[int, ...]
    random_state: int = 20260715
    models: dict[int, Pipeline] = field(default_factory=dict)
    feature_names: list[str] = field(default_factory=list)

    def fit(self, X: pd.DataFrame, targets: pd.DataFrame) -> "DirectElasticNetForecaster":
        self.feature_names = list(X.columns)
        for horizon in self.horizons:
            target_column = f"target_log_return_{horizon}d"
            if target_column not in targets:
                raise KeyError(target_column)
            valid = targets[target_column].notna()
            if valid.sum() < 100:
                raise ValueError(f"Insufficient rows for {target_column}: {valid.sum()}")
            pipeline = Pipeline(
                [
                    ("imputer", SimpleImputer(strategy="median")),
                    ("scale", StandardScaler()),
                    (
                        "model",
                        ElasticNetCV(
                            l1_ratio=[0.05, 0.15, 0.35, 0.65, 0.9],
                            alphas=np.logspace(-5, -1, 30),
                            cv=5,
                            max_iter=20000,
                            random_state=self.random_state,
                        ),
                    ),
                ]
            )
            pipeline.fit(X.loc[valid, self.feature_names], targets.loc[valid, target_column])
            self.models[horizon] = pipeline
        return self

    def predict_log_returns(self, X: pd.DataFrame) -> pd.DataFrame:
        if not self.models:
            raise RuntimeError("Model has not been fitted")
        predictions = {
            f"pred_log_return_{horizon}d": model.predict(X[self.feature_names])
            for horizon, model in self.models.items()
        }
        return pd.DataFrame(predictions, index=X.index)
