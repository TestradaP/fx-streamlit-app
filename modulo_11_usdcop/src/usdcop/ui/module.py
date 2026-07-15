from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from usdcop.config import resolve_paths
from usdcop.ui.charts import forecast_chart


AUTOMATED_MODEL_STATUSES = {
    "MODEL_ACTIVE_AUTOMATED_DAILY",
    "MODEL_ACTIVE_BENCHMARK_GATED",
    "VALIDATED_BENCHMARK_FALLBACK",
    "MODEL_TRAINED_PENDING_FORMAL_APPROVAL",  # Legacy published forecast.
}

DRIVER_GROUP_LABELS = {
    "rates_and_carry": "Tasas y carry",
    "global_risk": "Riesgo global",
    "external_flows": "Flujos externos",
    "domestic_macro": "Macro local",
    "technical_fx": "Dinámica USD/COP",
    "other": "Otros",
    "base_model": "Base del modelo",
}


def _load_forecast(paths) -> pd.DataFrame:
    live = paths.output_root / "latest_forecasts.csv"
    reference = paths.project_root / "data" / "reference" / "benchmark_forecasts_2026-07-15.csv"

    if live.exists():
        return pd.read_csv(live)

    if reference.exists():
        return pd.read_csv(reference)

    raise FileNotFoundError(
        f"No se encontró ningún archivo de pronósticos.\n"
        f"Ruta Live buscada: {live.resolve()}\n"
        f"Ruta Reference buscada: {reference.resolve()}"
    )


def _load_quality_snapshot(paths) -> dict | None:
    snapshot_path = paths.output_root / "data_quality_latest.json"
    if not snapshot_path.exists():
        return None
    value = json.loads(snapshot_path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else None


def _load_drivers(paths) -> pd.DataFrame:
    driver_path = paths.output_root / "forecast_drivers.csv"
    if not driver_path.exists():
        return pd.DataFrame()
    return pd.read_csv(driver_path)


def _load_validation(paths) -> dict | None:
    validation_path = paths.output_root / "model_validation.json"
    if not validation_path.exists():
        return None
    value = json.loads(validation_path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else None


def _load_monitor(paths) -> dict | None:
    monitor_path = paths.output_root / "model_monitor.json"
    if not monitor_path.exists():
        return None
    value = json.loads(monitor_path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else None


def _load_registry(paths) -> dict | None:
    registry_path = paths.output_root / "champion_registry.json"
    if not registry_path.exists():
        return None
    value = json.loads(registry_path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else None


def _forecast_review_id(forecast: pd.DataFrame) -> str:
    first = forecast.iloc[0]
    return "|".join(
        str(first.get(column, ""))
        for column in ("generated_at", "model_version", "as_of_date")
    )


def _format_cop(value: float | int | None) -> str:
    if value is None or pd.isna(value):
        return "N/D"
    return f"{float(value):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def render_module(project_root: str | Path | None = None) -> None:
    paths = resolve_paths(project_root)
    st.title("Modulo 11 | Inteligencia cambiaria USD/COP")
    st.caption("Soporte probabilistico para caja y coberturas. No constituye recomendacion de inversion.")

    forecast = _load_forecast(paths)
    drivers = _load_drivers(paths)
    quality_snapshot = _load_quality_snapshot(paths)
    validation = _load_validation(paths)
    monitor = _load_monitor(paths)
    registry = _load_registry(paths)
    point_validation_passed = bool(
        validation and validation.get("point_forecast_validation_passed")
    )
    status = str(forecast.get("status", pd.Series(["UNKNOWN"])).iloc[0])
    review_id = _forecast_review_id(forecast)
    approval = st.session_state.get("usdcop_daily_approval", {})
    is_daily_approved = approval.get("forecast_id") == review_id
    if status == "BENCHMARK_ONLY_NOT_TRAINED":
        st.warning(
            "Modo benchmark: se muestran spot y ancla teorica de carry. "
            "No hay una salida de modelo aprobada; la interfaz no rellena valores faltantes."
        )
    elif status in AUTOMATED_MODEL_STATUSES and is_daily_approved:
        st.success("Datos del pronóstico diario revisados en esta sesión.")
    elif status == "VALIDATED_BENCHMARK_FALLBACK":
        st.warning(
            "Los challengers no superaron los umbrales de validación. Se publica automáticamente "
            "el benchmark conservador de spot sin cambio con intervalos calibrados."
        )
    elif status in AUTOMATED_MODEL_STATUSES:
        st.warning(
            "Pronóstico automático disponible, pendiente de su revisión diaria de datos. "
            "Revise las fuentes en Frescura y gobierno antes de usarlo."
        )
    elif status == "MODEL_RESEARCH_VALIDATION_FAILED":
        st.error(
            "Modelo en estado de investigación: produjo resultados, pero no superó la validación "
            "comparativa fuera de muestra."
        )
    elif status.startswith("BENCHMARK_ONLY"):
        st.warning(f"Estado del pronóstico: {status}")
    else:
        st.success(f"Estado del modelo: {status}")

    first = forecast.iloc[0]
    cols = st.columns(4)
    cols[0].metric("Spot / TRM", _format_cop(first.get("spot_random_walk")))
    cols[1].metric("Carry 30d", _format_cop(forecast.loc[forecast.horizon_days.eq(30), "forward_anchor"].iloc[0]))
    cols[2].metric("Horizontes", "15 / 30 / 45 / 60")
    cols[3].metric("Corte", str(first.get("as_of_date", "N/D")))

    st.markdown("## Pronóstico final publicado por horizonte")
    if point_validation_passed:
        st.success("El modelo superó los criterios cuantitativos configurados en el backtest.")
    elif status == "VALIDATED_BENCHMARK_FALLBACK":
        st.warning(
            "Pronóstico operativo conservador: el sistema rechazó los modelos más complejos y "
            "seleccionó spot sin cambio."
        )
    else:
        st.error(
            "Resultado experimental: el modelo no ha superado el benchmark de spot sin cambio "
            "en el backtest disponible. No está aprobado para conclusiones académicas confirmatorias."
        )
    sorted_forecast = forecast.sort_values("horizon_days")
    forecast_cards = st.columns(len(sorted_forecast))
    for card, (_, row) in zip(forecast_cards, sorted_forecast.iterrows()):
        model_value = row.get("median")
        spot_value = row.get("spot_random_walk")
        if pd.notna(model_value) and pd.notna(spot_value):
            change = float(model_value) - float(spot_value)
            change_pct = change / float(spot_value) * 100
            delta = f"{change:+,.2f} COP ({change_pct:+.2f}%)"
        else:
            delta = None
        card.metric(
            f"{int(row['horizon_days'])} días",
            f"$ {_format_cop(model_value)}",
            delta=delta,
            help=f"Fecha objetivo: {row.get('target_date', 'N/D')}",
        )
        card.caption(f"Fecha objetivo: {row.get('target_date', 'N/D')}")
        if pd.notna(row.get("p10")) and pd.notna(row.get("p90")):
            card.caption(
                f"Rango P10–P90: {_format_cop(row.get('p10'))} – {_format_cop(row.get('p90'))}"
            )
        if pd.notna(row.get("probability_up")):
            card.caption(f"Prob. de subida: {float(row.get('probability_up')):.1%}")
        card.caption(f"Método seleccionado: {row.get('selected_model', 'N/D')}")
    st.caption(
        "El método se selecciona por horizonte mediante reglas fuera de muestra y puede volver "
        "automáticamente a spot sin cambio. La revisión diaria de datos no valida un modelo."
    )

    tabs = st.tabs(["Pronostico", "Drivers", "Backtest", "Escenarios", "Frescura y gobierno"])
    with tabs[0]:
        if status in AUTOMATED_MODEL_STATUSES and not is_daily_approved:
            st.info("Vista preliminar: los valores todavía no tienen su aprobación diaria.")
        st.plotly_chart(forecast_chart(forecast), width="stretch")
        display_columns = [
            column for column in [
                "horizon_days", "target_date", "spot_random_walk", "forward_anchor",
                "median", "p10", "p90", "probability_up", "selected_model", "status"
            ] if column in forecast.columns
        ]
        st.dataframe(forecast[display_columns], width="stretch", hide_index=True)
        st.caption(
            "El ancla teorica usa diferenciales de tasas y no equivale a una cotizacion NDF ejecutable. "
            "La curva de mercado debe incorporarse mediante el adaptador opcional."
        )

    with tabs[1]:
        st.subheader("Contribuciones y regimen")
        if drivers.empty:
            if status == "VALIDATED_BENCHMARK_FALLBACK":
                st.info(
                    "No se muestran drivers porque el método seleccionado es spot sin cambio. "
                    "Los modelos con variables explicativas fueron rechazados por la validación."
                )
            else:
                st.warning(
                    "Aún no existe el archivo de drivers. Ejecute nuevamente el workflow diario "
                    "para publicar la explicabilidad del modelo."
                )
        else:
            horizons = sorted(drivers["horizon_days"].dropna().astype(int).unique())
            default_horizon = horizons.index(30) if 30 in horizons else 0
            horizon = st.selectbox(
                "Horizonte del pronóstico",
                horizons,
                index=default_horizon,
                format_func=lambda value: f"{value} días",
            )
            selected = drivers.loc[drivers["horizon_days"].eq(horizon)].copy()
            selected["driver_group_label"] = selected["driver_group"].map(
                DRIVER_GROUP_LABELS
            ).fillna(selected["driver_group"])

            feature_rows = selected.loc[selected["feature"].ne("intercept")].copy()
            feature_rows["absolute_contribution"] = feature_rows[
                "contribution_cop_approx"
            ].abs()
            top = feature_rows.nlargest(12, "absolute_contribution").sort_values(
                "contribution_cop_approx"
            )
            if top.empty:
                st.info(
                    "El modelo no produjo contribuciones distintas de cero para este horizonte."
                )
            else:
                figure = px.bar(
                    top,
                    x="contribution_cop_approx",
                    y="feature",
                    orientation="h",
                    color="direction",
                    color_discrete_map={
                        "up": "#16803c",
                        "down": "#c63c3c",
                        "neutral": "#777777",
                    },
                    hover_data={
                        "driver_group_label": True,
                        "feature_value": ":.4f",
                        "standardized_value": ":.3f",
                        "coefficient": ":.6f",
                        "contribution_cop_approx": ":.2f",
                    },
                    labels={
                        "contribution_cop_approx": "Contribución aproximada (COP)",
                        "feature": "Variable",
                        "direction": "Dirección",
                        "driver_group_label": "Grupo",
                    },
                )
                figure.add_vline(x=0, line_width=1, line_color="#777777")
                figure.update_layout(legend_title_text="Impacto sobre USD/COP")
                st.plotly_chart(figure, width="stretch")

            grouped = (
                feature_rows.groupby("driver_group_label", as_index=False)[
                    ["contribution_log_return", "contribution_cop_approx"]
                ]
                .sum()
                .sort_values("contribution_cop_approx", ascending=False)
                .rename(
                    columns={
                        "driver_group_label": "Grupo",
                        "contribution_log_return": "Contribución log-return",
                        "contribution_cop_approx": "Impacto aproximado COP",
                    }
                )
            )
            st.markdown("#### Efecto agregado por grupo")
            st.dataframe(
                grouped,
                width="stretch",
                hide_index=True,
                column_config={
                    "Contribución log-return": st.column_config.NumberColumn(
                        format="%.6f"
                    ),
                    "Impacto aproximado COP": st.column_config.NumberColumn(
                        format="$ %.2f"
                    ),
                },
            )
            intercept = selected.loc[
                selected["feature"].eq("intercept"), "contribution_cop_approx"
            ]
            if not intercept.empty:
                st.caption(
                    f"Componente base del modelo: {_format_cop(intercept.iloc[0])} COP."
                )
            if status in AUTOMATED_MODEL_STATUSES and not is_daily_approved:
                st.info(
                    "Explicabilidad preliminar: revise y apruebe los datos diarios antes de usarla."
                )
            st.caption(
                "Las barras son efectos contrafactuales locales al reemplazar una variable por su "
                "valor central de entrenamiento; no son efectos causales ni necesariamente aditivos. "
                "El modelo actual no incluye variables explícitas de política/fiscal ni microestructura."
            )

    with tabs[2]:
        st.subheader("Validacion walk-forward")
        metrics_path = paths.output_root / "backtest_metrics.csv"
        if metrics_path.exists():
            metrics = pd.read_csv(metrics_path)
            if point_validation_passed:
                st.success("El pronóstico puntual cumple los criterios cuantitativos configurados.")
            elif status == "VALIDATED_BENCHMARK_FALLBACK":
                st.warning(
                    "Los challengers fueron rechazados; el benchmark permanece como método operativo."
                )
            else:
                st.error(
                    "Validación no aprobada: los challengers no superan consistentemente al "
                    "benchmark de spot sin cambio."
                )
            if registry:
                registry_rows = []
                for horizon, selection in registry.get("horizons", {}).items():
                    calibration = selection.get("calibration", {})
                    registry_rows.append(
                        {
                            "Horizonte": int(horizon),
                            "Método seleccionado": selection.get("selected_model", "N/D"),
                            "Fallback": selection.get("fallback_used", False),
                            "Cobertura P10-P90 observada": (
                                float(calibration.get("empirical_coverage")) * 100
                                if calibration.get("empirical_coverage") is not None
                                else None
                            ),
                            "Observaciones de cobertura": calibration.get(
                                "coverage_test_observations"
                            ),
                        }
                    )
                st.markdown("#### Registro champion por horizonte")
                st.dataframe(
                    pd.DataFrame(registry_rows),
                    width="stretch",
                    hide_index=True,
                    column_config={
                        "Cobertura P10-P90 observada": st.column_config.NumberColumn(
                            format="%.1%%"
                        )
                    },
                )
            model_metrics = metrics.loc[
                ~metrics["model"].isin(["random_walk", "carry"])
            ].copy()
            model_metrics["directional_accuracy"] *= 100
            model_metrics["positive_window_share"] *= 100
            summary_columns = [
                "horizon_days",
                "model",
                "observations",
                "mae_cop",
                "rmse_cop",
                "directional_accuracy",
                "skill_vs_random_walk_pct",
                "loss_difference_ci_high_cop",
                "positive_window_share",
                "qualifies",
            ]
            st.dataframe(
                model_metrics[summary_columns],
                width="stretch",
                hide_index=True,
                column_config={
                    "horizon_days": "Horizonte (días)",
                    "observations": "Observaciones OOS",
                    "model": "Challenger",
                    "mae_cop": st.column_config.NumberColumn("MAE (COP)", format="%.2f"),
                    "rmse_cop": st.column_config.NumberColumn("RMSE (COP)", format="%.2f"),
                    "directional_accuracy": st.column_config.NumberColumn(
                        "Acierto direccional", format="%.1%%"
                    ),
                    "skill_vs_random_walk_pct": st.column_config.NumberColumn(
                        "Mejora vs. spot", format="%.1f%%"
                    ),
                    "loss_difference_ci_high_cop": st.column_config.NumberColumn(
                        "Límite superior diferencia MAE", format="%.2f"
                    ),
                    "positive_window_share": st.column_config.NumberColumn(
                        "Ventanas con mejora", format="%.1f%%"
                    ),
                    "qualifies": "Clasifica",
                },
            )
            comparison = px.line(
                metrics,
                x="horizon_days",
                y="mae_cop",
                color="model",
                markers=True,
                labels={
                    "horizon_days": "Horizonte (días)",
                    "mae_cop": "MAE (COP)",
                    "model": "Modelo",
                },
                title="Error absoluto fuera de muestra (menor es mejor)",
            )
            st.plotly_chart(comparison, width="stretch")
            st.caption(
                "Diseño: ventana expansiva, bloques de 100 observaciones y purga de 60 días "
                "calendario. La selección de hiperparámetros usa TimeSeriesSplit."
            )
            if validation and validation.get("academic_blockers"):
                st.markdown("#### Pendientes para uso académico confirmatorio")
                for blocker in validation["academic_blockers"]:
                    st.write(f"- {blocker}")
        else:
            st.warning("No existe todavía un backtest publicado.")

    with tabs[3]:
        st.subheader("Escenarios de caja")
        exposure = st.number_input("Exposicion futura en USD", min_value=0.0, value=100000.0, step=10000.0)
        hedge = st.slider("Porcentaje cubierto", min_value=0, max_value=100, value=50)
        st.write(f"Exposicion no cubierta: USD {exposure * (1 - hedge / 100):,.0f}")
        st.caption("La politica de hedge debe aprobarse fuera del modelo y parametrizar su funcion de perdida.")

    with tabs[4]:
        st.subheader("Frescura de datos y gobierno")
        if quality_snapshot:
            st.caption(f"Control generado: {quality_snapshot.get('generated_at', 'N/D')}")
            quality_rows = pd.DataFrame(quality_snapshot.get("quality", []))
            if quality_rows.empty:
                st.warning("El control diario no contiene series evaluadas.")
            else:
                if "messages" in quality_rows:
                    quality_rows["messages"] = quality_rows["messages"].apply(
                        lambda value: ", ".join(value) if isinstance(value, list) else str(value or "")
                    )
                columns = [
                    column
                    for column in (
                        "series",
                        "passed",
                        "rows",
                        "latest_value",
                        "age_days",
                        "messages",
                    )
                    if column in quality_rows
                ]
                st.dataframe(quality_rows[columns], width="stretch", hide_index=True)

            optional_failures = quality_snapshot.get("failed", [])
            if optional_failures:
                st.warning(
                    "Fuentes opcionales no disponibles: "
                    + ", ".join(str(item.get("series", "unknown")) for item in optional_failures)
                )
        else:
            st.warning(
                "Aún no existe el control de calidad publicado. Ejecute el workflow diario actualizado."
            )

        st.markdown("#### Monitoreo del modelo")
        if monitor:
            monitor_columns = st.columns(3)
            monitor_columns[0].metric(
                "Variables evaluadas", monitor.get("evaluated_features", 0)
            )
            monitor_columns[1].metric(
                "Fuera de rango histórico",
                len(monitor.get("outside_training_range", [])),
            )
            monitor_columns[2].metric(
                "Proporción fuera de rango",
                f"{float(monitor.get('outside_ratio', 0)):.1%}",
            )
            if monitor.get("severe"):
                st.error("Deriva severa detectada: el sistema debe usar el benchmark.")
            elif monitor.get("outside_training_range"):
                st.warning(
                    "Variables para vigilar: "
                    + ", ".join(monitor.get("outside_training_range", []))
                )
            else:
                st.success("No se detectó deriva material en las variables de entrada.")
        else:
            st.caption("El monitor de deriva se publicará con la próxima ejecución diaria.")

        required_failures = [
            item
            for item in (quality_snapshot or {}).get("failed", [])
            if not str(item.get("series", "")).startswith("dane:")
        ]
        quality_ready = bool(quality_snapshot) and not required_failures
        model_ready = forecast["median"].notna().all()

        st.markdown("#### Revisión diaria de datos")
        if is_daily_approved:
            st.success(
                f"Datos revisados por {approval.get('reviewer', 'usuario')} "
                f"el {approval.get('approved_at', 'N/D')}."
            )
            if st.button("Revocar aprobación de esta sesión"):
                st.session_state.pop("usdcop_daily_approval", None)
                st.rerun()
        else:
            reviewed = st.checkbox(
                "Confirmo que revisé la selección, frescura y alertas de los datos.",
                disabled=not (quality_ready and model_ready),
            )
            if st.button(
                "Confirmar revisión de datos",
                type="primary",
                disabled=not (quality_ready and model_ready and reviewed),
            ):
                st.session_state.usdcop_daily_approval = {
                    "forecast_id": review_id,
                    "reviewer": st.session_state.get("authenticated_user", "usuario"),
                    "approved_at": pd.Timestamp.now(tz="UTC").isoformat(),
                }
                st.rerun()
            if not quality_ready:
                st.caption("La aprobación se habilita cuando el control diario no tiene fallas requeridas.")
            elif not model_ready:
                st.caption("La aprobación se habilita cuando el pronóstico automático contiene medianas.")

        st.markdown(
            "Esta confirmación corresponde únicamente a la calidad y selección de datos de la sesión. "
            "No constituye aprobación estadística, académica ni financiera del modelo."
        )
