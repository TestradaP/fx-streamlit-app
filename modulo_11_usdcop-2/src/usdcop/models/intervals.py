from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class ConformalInterval:
    coverage: float = 0.90

    def radius(self, residuals: pd.Series | np.ndarray) -> float:
        values = np.abs(np.asarray(residuals, dtype=float))
        values = values[np.isfinite(values)]
        if values.size < 30:
            raise ValueError("At least 30 residuals are required")
        quantile = min(1.0, np.ceil((values.size + 1) * self.coverage) / values.size)
        return float(np.quantile(values, quantile, method="higher"))

    def apply(self, prediction: pd.Series, residuals: pd.Series | np.ndarray) -> pd.DataFrame:
        radius = self.radius(residuals)
        return pd.DataFrame(
            {
                "median": prediction,
                "lower": prediction - radius,
                "upper": prediction + radius,
            },
            index=prediction.index,
        )
