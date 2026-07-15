from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st

from usdcop.config import resolve_paths
from usdcop.ui.charts import forecast_chart


AUTOMATED_MODEL_STATUSES = {
    "MODEL_ACTIVE_AUTOMATED_DAILY",
    "MODEL_TRAINED_PENDING_FORMAL_APPROVAL",  # Legacy published forecast.
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
    quality_snapshot = _load_quality_snapshot(paths)
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
        st.success("Pronóstico diario revisado y aprobado para uso en esta sesión.")
    elif status in AUTOMATED_MODEL_STATUSES:
        st.warning(
            "Pronóstico automático disponible, pendiente de su revisión diaria de datos. "
            "Apruébelo en Frescura y gobierno antes de usarlo."
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

    tabs = st.tabs(["Pronostico", "Drivers", "Backtest", "Escenarios", "Frescura y gobierno"])
    with tabs[0]:
        if status in AUTOMATED_MODEL_STATUSES and not is_daily_approved:
            st.info("Vista preliminar: los valores todavía no tienen su aprobación diaria.")
        st.plotly_chart(forecast_chart(forecast), use_container_width=True)
        display_columns = [
            column for column in [
                "horizon_days", "target_date", "spot_random_walk", "forward_anchor",
                "median", "p10", "p90", "status"
            ] if column in forecast.columns
        ]
        st.dataframe(forecast[display_columns], use_container_width=True, hide_index=True)
        st.caption(
            "El ancla teorica usa diferenciales de tasas y no equivale a una cotizacion NDF ejecutable. "
            "La curva de mercado debe incorporarse mediante el adaptador opcional."
        )

    with tabs[1]:
        st.subheader("Contribuciones y regimen")
        st.info(
            "Esta vista se habilita cuando existen artefactos aprobados de explicabilidad. "
            "Debe mostrar contribuciones de carry, riesgo global, flujos externos, fiscal/politico y microestructura."
        )

    with tabs[2]:
        st.subheader("Validacion walk-forward")
        metrics_path = paths.output_root / "backtest_metrics.csv"
        if metrics_path.exists():
            st.dataframe(pd.read_csv(metrics_path), use_container_width=True, hide_index=True)
        else:
            st.warning("No existe un backtest aprobado. No se muestran metricas simuladas.")

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
                    for column in ("series", "passed", "rows", "age_days", "messages")
                    if column in quality_rows
                ]
                st.dataframe(quality_rows[columns], use_container_width=True, hide_index=True)

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

        required_failures = [
            item
            for item in (quality_snapshot or {}).get("failed", [])
            if not str(item.get("series", "")).startswith("dane:")
        ]
        quality_ready = bool(quality_snapshot) and not required_failures
        model_ready = status in AUTOMATED_MODEL_STATUSES and forecast["median"].notna().all()

        st.markdown("#### Aprobación diaria")
        if is_daily_approved:
            st.success(
                f"Aprobado por {approval.get('reviewer', 'usuario')} "
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
                "Aprobar pronóstico diario",
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
            "La aprobación corresponde únicamente al pronóstico publicado y a esta sesión. "
            "Un nuevo pronóstico diario requiere una nueva revisión."
        )
