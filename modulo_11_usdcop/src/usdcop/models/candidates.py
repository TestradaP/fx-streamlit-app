from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from sklearn.decomposition import PCA
from sklearn.ensemble import ExtraTreesRegressor, HistGradientBoostingRegressor
from sklearn.feature_selection import SelectKBest, f_regression
from sklearn.impute import SimpleImputer
from sklearn.linear_model import ElasticNetCV, HuberRegressor, RidgeCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, StandardScaler


CANDIDATE_NAMES = (
    "elastic_net",
    "ridge",
    "huber",
    "gradient_boosting",
    "pca_ridge",
    "extra_trees",
    "quantile_boosting",
    "regime_ridge",
)


def _ridge_pipeline(feature_count: int, splits: int = 5) -> Pipeline:
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scale", StandardScaler()),
            ("select", SelectKBest(f_regression, k=min(40, feature_count))),
            (
                "model",
                RidgeCV(
                    alphas=np.logspace(-3, 3, 25),
                    cv=TimeSeriesSplit(n_splits=splits),
                    scoring="neg_mean_absolute_error",
                ),
            ),
        ]
    )


@dataclass
class RegimeRidgeRegressor:
    feature_count: int
    minimum_regime_rows: int = 250
    global_model: Pipeline | None = None
    regime_models: dict[str, Pipeline] = field(default_factory=dict)

    @staticmethod
    def _regimes(X: pd.DataFrame) -> pd.Series:
        vix = (
            pd.to_numeric(X["vix_level"], errors="coerce")
            if "vix_level" in X
            else pd.Series(np.nan, index=X.index)
        )
        return pd.cut(
            vix,
            bins=[-np.inf, 15, 25, np.inf],
            labels=["calm", "normal", "stress"],
        ).astype("string")

    def fit(self, X: pd.DataFrame, y: pd.Series) -> "RegimeRidgeRegressor":
        self.global_model = _ridge_pipeline(self.feature_count).fit(X, y)
        regimes = self._regimes(X)
        for regime in ("calm", "normal", "stress"):
            selected = regimes.eq(regime)
            if int(selected.sum()) >= self.minimum_regime_rows:
                self.regime_models[regime] = _ridge_pipeline(
                    self.feature_count, splits=4
                ).fit(X.loc[selected], y.loc[selected])
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        if self.global_model is None:
            raise RuntimeError("Regime model has not been fitted")
        output = self.global_model.predict(X)
        regimes = self._regimes(X)
        for regime, model in self.regime_models.items():
            selected = regimes.eq(regime).to_numpy()
            if selected.any():
                output[selected] = model.predict(X.loc[selected])
        return output


@dataclass
class DirectCandidateForecaster:
    horizons: tuple[int, ...]
    random_state: int = 20260715
    models: dict[str, dict[int, Any]] = field(default_factory=dict)
    feature_names: list[str] = field(default_factory=list)
    quantile_models: dict[int, dict[float, Pipeline]] = field(default_factory=dict)

    def _quantile_pipeline(self, feature_count: int, quantile: float) -> Pipeline:
        return Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("select", SelectKBest(f_regression, k=min(40, feature_count))),
                (
                    "model",
                    HistGradientBoostingRegressor(
                        loss="quantile",
                        quantile=quantile,
                        learning_rate=0.04,
                        max_iter=250,
                        max_leaf_nodes=15,
                        min_samples_leaf=30,
                        l2_regularization=1.0,
                        early_stopping=False,
                        random_state=self.random_state,
                    ),
                ),
            ]
        )

    def _pipelines(self, feature_count: int) -> dict[str, Pipeline]:
        selected = min(40, feature_count)
        time_cv = TimeSeriesSplit(n_splits=5)
        return {
            "elastic_net": Pipeline(
                [
                    ("imputer", SimpleImputer(strategy="median")),
                    ("scale", StandardScaler()),
                    (
                        "model",
                        ElasticNetCV(
                            l1_ratio=[0.05, 0.15, 0.35, 0.65, 0.9],
                            alphas=np.logspace(-4, -1, 25),
                            cv=time_cv,
                            max_iter=50000,
                            random_state=self.random_state,
                        ),
                    ),
                ]
            ),
            "ridge": _ridge_pipeline(feature_count),
            "huber": Pipeline(
                [
                    ("imputer", SimpleImputer(strategy="median")),
                    ("scale", RobustScaler()),
                    ("select", SelectKBest(f_regression, k=selected)),
                    (
                        "model",
                        HuberRegressor(
                            epsilon=1.35,
                            alpha=0.001,
                            max_iter=3000,
                            tol=1e-6,
                        ),
                    ),
                ]
            ),
            "gradient_boosting": Pipeline(
                [
                    ("imputer", SimpleImputer(strategy="median")),
                    ("select", SelectKBest(f_regression, k=selected)),
                    (
                        "model",
                        HistGradientBoostingRegressor(
                            learning_rate=0.05,
                            max_iter=250,
                            max_leaf_nodes=15,
                            min_samples_leaf=30,
                            l2_regularization=0.5,
                            early_stopping=False,
                            random_state=self.random_state,
                        ),
                    ),
                ]
            ),
            "pca_ridge": Pipeline(
                [
                    ("imputer", SimpleImputer(strategy="median")),
                    ("scale", StandardScaler()),
                    ("pca", PCA(n_components=0.95, svd_solver="full")),
                    (
                        "model",
                        RidgeCV(
                            alphas=np.logspace(-3, 3, 25),
                            cv=TimeSeriesSplit(n_splits=5),
                            scoring="neg_mean_absolute_error",
                        ),
                    ),
                ]
            ),
            "extra_trees": Pipeline(
                [
                    ("imputer", SimpleImputer(strategy="median")),
                    ("select", SelectKBest(f_regression, k=selected)),
                    (
                        "model",
                        ExtraTreesRegressor(
                            n_estimators=200,
                            max_depth=8,
                            min_samples_leaf=12,
                            max_features=0.7,
                            n_jobs=-1,
                            random_state=self.random_state,
                        ),
                    ),
                ]
            ),
            "quantile_boosting": self._quantile_pipeline(feature_count, 0.50),
        }

    def fit(self, X: pd.DataFrame, targets: pd.DataFrame) -> "DirectCandidateForecaster":
        self.feature_names = list(X.columns)
        self.models = {name: {} for name in CANDIDATE_NAMES}
        self.quantile_models = {}
        for horizon in self.horizons:
            target_column = f"target_log_return_{horizon}d"
            if target_column not in targets:
                raise KeyError(target_column)
            valid = targets[target_column].notna()
            if int(valid.sum()) < 100:
                raise ValueError(f"Insufficient rows for {target_column}: {valid.sum()}")
            horizon_X = X.loc[valid, self.feature_names]
            horizon_y = targets.loc[valid, target_column]
            pipelines: dict[str, Any] = self._pipelines(len(self.feature_names))
            pipelines["regime_ridge"] = RegimeRidgeRegressor(len(self.feature_names))
            for name, pipeline in pipelines.items():
                pipeline.fit(horizon_X, horizon_y)
                self.models[name][horizon] = pipeline
            self.quantile_models[horizon] = {}
            for quantile in (0.10, 0.90):
                quantile_model = self._quantile_pipeline(
                    len(self.feature_names), quantile
                )
                quantile_model.fit(horizon_X, horizon_y)
                self.quantile_models[horizon][quantile] = quantile_model
        return self

    def predict_all(self, X: pd.DataFrame) -> pd.DataFrame:
        if not self.models:
            raise RuntimeError("Candidate models have not been fitted")
        output: dict[str, np.ndarray] = {}
        for name, horizon_models in self.models.items():
            for horizon, model in horizon_models.items():
                output[f"{name}_pred_log_return_{horizon}d"] = model.predict(
                    X[self.feature_names]
                )
        return pd.DataFrame(output, index=X.index)

    def predict_quantiles(self, X: pd.DataFrame) -> pd.DataFrame:
        if not self.quantile_models:
            raise RuntimeError("Quantile models have not been fitted")
        output: dict[str, np.ndarray] = {}
        for horizon, models in self.quantile_models.items():
            low = models[0.10].predict(X[self.feature_names])
            high = models[0.90].predict(X[self.feature_names])
            output[f"pred_log_return_p10_{horizon}d"] = np.minimum(low, high)
            output[f"pred_log_return_p90_{horizon}d"] = np.maximum(low, high)
        return pd.DataFrame(output, index=X.index)

    def predict_log_returns(
        self, X: pd.DataFrame, model_name: str = "elastic_net"
    ) -> pd.DataFrame:
        if model_name not in self.models:
            raise KeyError(f"Unknown candidate model: {model_name}")
        predictions = {
            f"pred_log_return_{horizon}d": pipeline.predict(X[self.feature_names])
            for horizon, pipeline in self.models[model_name].items()
        }
        return pd.DataFrame(predictions, index=X.index)
