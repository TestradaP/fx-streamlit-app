from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from usdcop.config import resolve_paths
from usdcop.data.repository import SeriesRepository
from usdcop.ui.charts import forecast_chart


def _load_forecast(paths) -> pd.DataFrame:
    from pathlib import Path
    
    # 1. Encontrar la raíz de 'modulo_11_usdcop'
    # Subimos 4 niveles (padre de src/ que es index parents[3])
    raiz_modulo = Path(__file__).resolve().parents[3]
    
    # 2. Definir las rutas físicas absolutas
    live = raiz_modulo / "outputs" / "latest_forecasts.csv"
    reference = raiz_modulo / "data" / "reference" / "benchmark_forecasts_2026-07-15.csv"
    
    # 3. Cargar el archivo que esté disponible
    if live.exists():
        return pd.read_csv(live)
        
    if reference.exists():
        return pd.read_csv(reference)
        
    # Plan C de emergencia extrema si nada existe
    raise FileNotFoundError(
        f"No se encontró ningún archivo de pronósticos.\n"
        f"Ruta Live buscada: {live.resolve()}\n"
        f"Ruta Reference buscada: {reference.resolve()}"
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
    status = str(forecast.get("status", pd.Series(["UNKNOWN"])).iloc[0])
    if status == "BENCHMARK_ONLY_NOT_TRAINED":
        st.warning(
            "Modo benchmark: se muestran spot y ancla teorica de carry. "
            "No hay una salida de modelo aprobada; la interfaz no rellena valores faltantes."
        )
    elif "PENDING" in status:
        st.info(f"Estado del modelo: {status}. Revise el tab de gobierno antes de uso operativo.")
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
        repository = SeriesRepository(paths.storage_root)
        registry = repository.registry()
        if registry.empty:
            st.warning("No hay series persistidas en el store local. Ejecute scripts/update_daily.py.")
        else:
            st.dataframe(registry, use_container_width=True, hide_index=True)
        st.markdown(
            "**Controles minimos:** fechas de publicacion, vintages, alertas de rezago, "
            "version del modelo, champion/challenger, aprobacion humana y bitacora de cambios."
        )
