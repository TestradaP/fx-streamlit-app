from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


def forecast_chart(frame: pd.DataFrame) -> go.Figure:
    data = frame.sort_values("horizon_days")
    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=data["horizon_days"],
            y=data["spot_random_walk"],
            mode="lines+markers",
            name="Spot sin cambio",
            line={"dash": "dot"},
        )
    )
    figure.add_trace(
        go.Scatter(
            x=data["horizon_days"],
            y=data["forward_anchor"],
            mode="lines+markers",
            name="Ancla teorica de carry",
        )
    )
    if "median" in data and data["median"].notna().any():
        figure.add_trace(
            go.Scatter(
                x=data["horizon_days"],
                y=data["median"],
                mode="lines+markers",
                name="Mediana del modelo",
                line={"width": 4},
            )
        )
        if {"p10", "p90"}.issubset(data.columns) and data[["p10", "p90"]].notna().all(axis=None):
            figure.add_trace(go.Scatter(x=data["horizon_days"], y=data["p90"], mode="lines", line={"width": 0}, showlegend=False))
            figure.add_trace(
                go.Scatter(
                    x=data["horizon_days"],
                    y=data["p10"],
                    mode="lines",
                    fill="tonexty",
                    name="P10-P90",
                    line={"width": 0},
                )
            )
    figure.update_layout(
        title="USD/COP por horizonte",
        xaxis_title="Dias calendario",
        yaxis_title="COP por USD",
        hovermode="x unified",
        margin={"l": 30, "r": 20, "t": 55, "b": 30},
        legend={"orientation": "h", "y": 1.1},
    )
    return figure
