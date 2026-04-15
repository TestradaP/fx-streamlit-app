from __future__ import annotations

from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st
from matplotlib.ticker import FuncFormatter


# =========================
# CONFIGURACIÓN
# =========================
TRM_API_DATASETS = [
    "32sa-8pi3",  # Histórico TRM
    "dit9-nnvp",  # TRM desde enero 2022
]
DATOS_GOV_BASE = "https://www.datos.gov.co/resource"


# =========================
# FORMATO
# =========================
def formato_pesos(x, pos):
    return f"${x:,.0f}"


def formato_pesos_decimales(x, pos):
    return f"${x:,.2f}"


# =========================
# VALIDACIONES Y CARGA
# =========================
def validar_columnas_facturas(df: pd.DataFrame) -> None:
    columnas_requeridas = {
        "factura",
        "cliente",
        "fecha_factura",
        "moneda",
        "valor_usd",
        "saldo_usd",
        "trm_inicial",
    }
    faltantes = columnas_requeridas - set(df.columns)
    if faltantes:
        raise ValueError(
            f"Faltan columnas obligatorias en facturas_abiertas.xlsx: {sorted(faltantes)}"
        )


def validar_columnas_monetizaciones(df: pd.DataFrame) -> None:
    columnas_requeridas = {"fecha", "factura", "monto_usd"}
    faltantes = columnas_requeridas - set(df.columns)
    if faltantes:
        raise ValueError(
            f"Faltan columnas obligatorias en monetizaciones.xlsx: {sorted(faltantes)}"
        )


def cargar_facturas(archivo) -> pd.DataFrame:
    df = pd.read_excel(archivo)
    validar_columnas_facturas(df)

    df["fecha_factura"] = pd.to_datetime(df["fecha_factura"], errors="coerce")
    if df["fecha_factura"].isna().any():
        raise ValueError("Hay fechas inválidas en 'fecha_factura'.")

    for col in ["valor_usd", "saldo_usd", "trm_inicial"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if df[["valor_usd", "saldo_usd", "trm_inicial"]].isna().any().any():
        raise ValueError(
            "Hay valores no numéricos o vacíos en 'valor_usd', 'saldo_usd' o 'trm_inicial'."
        )

    df = df.copy()
    df["moneda"] = df["moneda"].astype(str).str.upper().str.strip()
    df["factura"] = df["factura"].astype(str).str.strip()

    df = df[(df["moneda"] == "USD") & (df["saldo_usd"] > 0)].copy()

    if df.empty:
        raise ValueError("No quedaron facturas activas en USD con saldo_usd > 0.")

    return df


def cargar_monetizaciones(archivo) -> pd.DataFrame:
    if archivo is None:
        return pd.DataFrame(columns=["fecha", "factura", "monto_usd"])

    df = pd.read_excel(archivo)
    validar_columnas_monetizaciones(df)

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    if df["fecha"].isna().any():
        raise ValueError("Hay fechas inválidas en monetizaciones.xlsx.")

    df["factura"] = df["factura"].astype(str).str.strip()
    df["monto_usd"] = pd.to_numeric(df["monto_usd"], errors="coerce")

    if df["monto_usd"].isna().any():
        raise ValueError("Hay valores inválidos en 'monto_usd' de monetizaciones.xlsx.")

    if (df["monto_usd"] < 0).any():
        raise ValueError("No se permiten monetizaciones negativas.")

    return df.sort_values(["factura", "fecha"]).reset_index(drop=True)


# =========================
# TRM AUTOMÁTICA
# =========================
def convertir_trm_a_float(valor):
    if pd.isna(valor):
        return None

    s = str(valor).strip().replace("$", "").replace(" ", "")

    if "," in s and "." in s:
        s = s.replace(",", "")
        return float(s)

    if s.count(",") == 1 and s.count(".") >= 1:
        s = s.replace(".", "").replace(",", ".")
        return float(s)

    if "," in s and "." not in s:
        s = s.replace(",", ".")

    return float(s)


@st.cache_data(show_spinner=False)
def descargar_trm_historica(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    errores = []

    for dataset_id in TRM_API_DATASETS:
        try:
            df_trm = descargar_trm_desde_datos_abiertos(
                dataset_id=dataset_id,
                start_date=start_date,
                end_date=end_date,
            )

            if df_trm.empty:
                raise ValueError(f"El dataset {dataset_id} devolvió 0 filas.")

            fecha_max = df_trm["fecha"].max().normalize()

            if fecha_max >= end_date.normalize() - pd.Timedelta(days=3):
                return df_trm

            errores.append(
                f"{dataset_id}: llegó hasta {fecha_max.date()}, no hasta {end_date.date()}"
            )

        except Exception as e:
            errores.append(f"{dataset_id}: {e}")

    raise ValueError(
        "No fue posible descargar TRM actualizada automáticamente.\n"
        + "\n".join(errores)
    )


def descargar_trm_desde_datos_abiertos(
    dataset_id: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    start_iso = start_date.strftime("%Y-%m-%dT00:00:00")
    end_iso = end_date.strftime("%Y-%m-%dT23:59:59")

    url = f"{DATOS_GOV_BASE}/{dataset_id}.json"
    params = {
        "$select": "valor,vigenciadesde,vigenciahasta",
        "$where": (
            f"vigenciadesde <= '{end_iso}' "
            f"AND vigenciahasta >= '{start_iso}'"
        ),
        "$order": "vigenciadesde ASC",
        "$limit": 50000,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if not data:
        return pd.DataFrame(columns=["fecha", "trm"])

    df = pd.DataFrame(data)

    columnas_esperadas = {"valor", "vigenciadesde", "vigenciahasta"}
    faltantes = columnas_esperadas - set(df.columns)
    if faltantes:
        raise ValueError(
            f"El dataset {dataset_id} no trajo las columnas esperadas: {sorted(faltantes)}"
        )

    df["valor"] = df["valor"].apply(convertir_trm_a_float)
    df["vigenciadesde"] = pd.to_datetime(df["vigenciadesde"], errors="coerce")
    df["vigenciahasta"] = pd.to_datetime(df["vigenciahasta"], errors="coerce")

    if df[["valor", "vigenciadesde", "vigenciahasta"]].isna().any().any():
        raise ValueError(f"El dataset {dataset_id} trajo valores inválidos.")

    registros = []

    for _, row in df.iterrows():
        desde = row["vigenciadesde"].normalize()
        hasta = row["vigenciahasta"].normalize()
        trm = float(row["valor"])

        rango = pd.date_range(start=desde, end=hasta, freq="D")
        for fecha in rango:
            if start_date.normalize() <= fecha <= end_date.normalize():
                registros.append({"fecha": fecha, "trm": trm})

    df_expandido = pd.DataFrame(registros)

    if df_expandido.empty:
        return pd.DataFrame(columns=["fecha", "trm"])

    df_expandido = (
        df_expandido
        .drop_duplicates(subset=["fecha"], keep="last")
        .sort_values("fecha")
        .reset_index(drop=True)
    )

    return df_expandido


# =========================
# CÁLCULOS
# =========================
def validar_monetizaciones_vs_facturas(
    df_facturas: pd.DataFrame, df_monetizaciones: pd.DataFrame
) -> pd.DataFrame:
    if df_monetizaciones.empty:
        return pd.DataFrame()

    facturas_validas = set(df_facturas["factura"])
    facturas_monetizadas = set(df_monetizaciones["factura"])

    no_encontradas = facturas_monetizadas - facturas_validas
    if no_encontradas:
        raise ValueError(
            f"Hay monetizaciones para facturas que no existen en facturas_abiertas.xlsx: {sorted(no_encontradas)}"
        )

    monet_por_factura = (
        df_monetizaciones.groupby("factura", as_index=False)["monto_usd"].sum()
    )

    base = df_facturas[["factura", "valor_usd", "saldo_usd"]].copy()
    control = base.merge(monet_por_factura, on="factura", how="left").fillna(0)

    excedidas = control[control["monto_usd"] > control["valor_usd"]]
    if not excedidas.empty:
        raise ValueError(
            "Hay facturas donde la monetización total supera el valor_usd original:\n"
            f"{excedidas[['factura', 'valor_usd', 'monto_usd']]}"
        )

    control["saldo_esperado"] = control["valor_usd"] - control["monto_usd"]
    control["diferencia_vs_saldo_actual"] = control["saldo_esperado"] - control["saldo_usd"]

    return control


def calcular_resumen_actual(
    df_facturas: pd.DataFrame,
    df_monetizaciones: pd.DataFrame,
    trm_actual: float,
) -> pd.DataFrame:
    monet_por_factura = (
        df_monetizaciones.groupby("factura", as_index=False)["monto_usd"].sum()
        if not df_monetizaciones.empty
        else pd.DataFrame(columns=["factura", "monto_usd"])
    )

    detalle = df_facturas.copy().merge(
        monet_por_factura.rename(columns={"monto_usd": "monetizado_usd"}),
        on="factura",
        how="left",
    )
    detalle["monetizado_usd"] = detalle["monetizado_usd"].fillna(0.0)

    detalle["saldo_vivo_actual_usd"] = detalle["saldo_usd"]
    detalle["valor_inicial_cop"] = detalle["saldo_vivo_actual_usd"] * detalle["trm_inicial"]
    detalle["valor_actual_cop"] = detalle["saldo_vivo_actual_usd"] * trm_actual
    detalle["diferencia_cambio_acumulada"] = (
        detalle["valor_actual_cop"] - detalle["valor_inicial_cop"]
    )

    return detalle


def construir_saldos_diarios(
    df_facturas: pd.DataFrame,
    df_monetizaciones: pd.DataFrame,
    fechas: pd.Series,
) -> pd.DataFrame:
    registros = []

    monetizaciones_por_factura = {
        factura: grupo.sort_values("fecha")
        for factura, grupo in df_monetizaciones.groupby("factura")
    } if not df_monetizaciones.empty else {}

    for _, fila in df_facturas.iterrows():
        factura = fila["factura"]
        fecha_factura = fila["fecha_factura"].normalize()
        saldo_inicial = float(fila["valor_usd"])
        trm_inicial = float(fila["trm_inicial"])
        cliente = fila["cliente"]

        monet_fact = monetizaciones_por_factura.get(
            factura,
            pd.DataFrame(columns=["fecha", "factura", "monto_usd"])
        ).copy()

        if not monet_fact.empty:
            monet_fact["fecha"] = monet_fact["fecha"].dt.normalize()

        for fecha in fechas:
            if fecha < fecha_factura:
                saldo_vivo = 0.0
            else:
                monetizado_acumulado = 0.0
                if not monet_fact.empty:
                    monetizado_acumulado = monet_fact.loc[
                        monet_fact["fecha"] <= fecha, "monto_usd"
                    ].sum()

                saldo_vivo = max(saldo_inicial - monetizado_acumulado, 0.0)

            registros.append(
                {
                    "fecha": fecha,
                    "factura": factura,
                    "cliente": cliente,
                    "saldo_vivo_usd": saldo_vivo,
                    "trm_inicial": trm_inicial,
                }
            )

    return pd.DataFrame(registros)


def construir_serie_total(
    df_facturas: pd.DataFrame,
    df_monetizaciones: pd.DataFrame,
    df_trm: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    fechas = df_trm["fecha"].sort_values().reset_index(drop=True)
    detalle_diario = construir_saldos_diarios(df_facturas, df_monetizaciones, fechas)
    detalle_diario = detalle_diario.merge(df_trm, on="fecha", how="left")

    detalle_diario["valor_diario_cop"] = (
        detalle_diario["saldo_vivo_usd"] * detalle_diario["trm"]
    )
    detalle_diario["valor_base_cop"] = (
        detalle_diario["saldo_vivo_usd"] * detalle_diario["trm_inicial"]
    )
    detalle_diario["diferencia_cambio_diaria_factura"] = (
        detalle_diario["valor_diario_cop"] - detalle_diario["valor_base_cop"]
    )

    serie_total = (
        detalle_diario.groupby("fecha", as_index=False)
        .agg(
            trm=("trm", "first"),
            saldo_total_usd=("saldo_vivo_usd", "sum"),
            valor_total_cop=("valor_diario_cop", "sum"),
            valor_base_total_cop=("valor_base_cop", "sum"),
            diferencia_cambio_total=("diferencia_cambio_diaria_factura", "sum"),
        )
        .sort_values("fecha")
        .reset_index(drop=True)
    )

    serie_total["variacion_diaria_trm"] = serie_total["trm"].diff()
    serie_total["diferencia_cambio_diaria_aprox"] = serie_total["diferencia_cambio_total"].diff()

    return serie_total, detalle_diario


def construir_serie_factura(
    fila_factura: pd.Series,
    df_monetizaciones: pd.DataFrame,
    df_trm: pd.DataFrame,
) -> pd.DataFrame:
    factura = str(fila_factura["factura"]).strip()
    cliente = fila_factura["cliente"]
    fecha_factura = pd.to_datetime(fila_factura["fecha_factura"]).normalize()
    valor_usd = float(fila_factura["valor_usd"])
    trm_inicial = float(fila_factura["trm_inicial"])

    monet_fact = df_monetizaciones[df_monetizaciones["factura"] == factura].copy()
    if not monet_fact.empty:
        monet_fact["fecha"] = pd.to_datetime(monet_fact["fecha"]).dt.normalize()
        monet_fact = monet_fact.sort_values("fecha")

    fechas = df_trm["fecha"].sort_values().reset_index(drop=True)
    registros = []

    for fecha in fechas:
        if fecha < fecha_factura:
            saldo_vivo = 0.0
            monetizado_acumulado = 0.0
        else:
            monetizado_acumulado = 0.0
            if not monet_fact.empty:
                monetizado_acumulado = monet_fact.loc[
                    monet_fact["fecha"] <= fecha, "monto_usd"
                ].sum()

            saldo_vivo = max(valor_usd - monetizado_acumulado, 0.0)

        registros.append(
            {
                "fecha": fecha,
                "factura": factura,
                "cliente": cliente,
                "saldo_vivo_usd": saldo_vivo,
                "monetizado_acumulado_usd": monetizado_acumulado,
                "trm_inicial": trm_inicial,
            }
        )

    serie_factura = pd.DataFrame(registros).merge(df_trm, on="fecha", how="left")
    serie_factura["valor_cop"] = serie_factura["saldo_vivo_usd"] * serie_factura["trm"]
    serie_factura["valor_base_cop"] = serie_factura["saldo_vivo_usd"] * serie_factura["trm_inicial"]
    serie_factura["diferencia_cambio_acumulada"] = (
        serie_factura["valor_cop"] - serie_factura["valor_base_cop"]
    )
    serie_factura["diferencia_cambio_dia"] = serie_factura["diferencia_cambio_acumulada"].diff()

    return serie_factura


# =========================
# EXPORTACIÓN
# =========================
def exportar_resultados_excel(
    detalle_actual: pd.DataFrame,
    serie_total: pd.DataFrame,
    detalle_diario: pd.DataFrame,
) -> bytes:
    output = BytesIO()

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        detalle_actual.to_excel(writer, sheet_name="detalle_actual", index=False)
        serie_total.to_excel(writer, sheet_name="serie_total", index=False)
        detalle_diario.to_excel(writer, sheet_name="detalle_diario", index=False)

    output.seek(0)
    return output.getvalue()


# =========================
# GRÁFICOS
# =========================
def fig_trm(serie_total: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(serie_total["fecha"], serie_total["trm"], linewidth=2)
    ax.set_title("Evolución diaria de la TRM")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("TRM (COP por USD)")
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos_decimales))
    fig.tight_layout()
    return fig


def fig_valor_cartera(serie_total: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(serie_total["fecha"], serie_total["valor_total_cop"], linewidth=2)
    ax.set_title("Valor total de la cartera en COP")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("Valor total cartera (COP)")
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
    fig.tight_layout()
    return fig


def fig_trm_y_diferencia_cambio(
    serie_total: pd.DataFrame,
    diferencia_total_actual: float,
):
    fig, ax1 = plt.subplots(figsize=(15, 8))

    linea1 = ax1.plot(
        serie_total["fecha"],
        serie_total["trm"],
        label="TRM",
        linewidth=2,
        color="tab:blue",
    )
    ax1.set_xlabel("Fecha")
    ax1.set_ylabel("TRM (COP por USD)", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")
    ax1.yaxis.set_major_formatter(FuncFormatter(formato_pesos_decimales))
    ax1.grid(True, alpha=0.3)

    ax2 = ax1.twinx()
    linea2 = ax2.plot(
        serie_total["fecha"],
        serie_total["diferencia_cambio_total"],
        label="Diferencia en cambio total",
        linestyle="--",
        linewidth=2.5,
        color="tab:red",
        zorder=3,
    )

    ax2.axhline(y=0, linestyle=":", linewidth=1.5, color="black", alpha=0.8)
    ax2.fill_between(
        serie_total["fecha"],
        serie_total["diferencia_cambio_total"],
        0,
        where=serie_total["diferencia_cambio_total"] >= 0,
        alpha=0.12,
        color="tab:green",
        interpolate=True,
    )
    ax2.fill_between(
        serie_total["fecha"],
        serie_total["diferencia_cambio_total"],
        0,
        where=serie_total["diferencia_cambio_total"] < 0,
        alpha=0.12,
        color="tab:red",
        interpolate=True,
    )

    ax2.set_ylabel("Diferencia en cambio total (COP)", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")
    ax2.yaxis.set_major_formatter(FuncFormatter(formato_pesos))

    plt.title("TRM y diferencia en cambio total")

    lineas = linea1 + linea2
    etiquetas = [linea.get_label() for linea in lineas]
    ax1.legend(lineas, etiquetas, loc="upper left")

    texto_resumen = f"Diferencia en cambio total al día: ${diferencia_total_actual:,.2f}"
    fig.text(
        0.5,
        0.02,
        texto_resumen,
        ha="center",
        va="center",
        fontsize=11,
        bbox=dict(boxstyle="round,pad=0.5", facecolor="white", edgecolor="black"),
    )

    fig.tight_layout(rect=[0, 0.06, 1, 1])
    return fig


def fig_pnl_cambiario(serie_total: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(15, 6))
    ax.bar(
        serie_total["fecha"],
        serie_total["diferencia_cambio_diaria_aprox"].fillna(0),
        width=1.0,
    )
    ax.axhline(0, color="black", linewidth=1)
    ax.set_title("P&L diario por diferencia en cambio")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("COP")
    ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    return fig


def fig_factura_individual(
    serie_factura: pd.DataFrame,
    factura: str,
    diferencia_acumulada: float,
    diferencia_dia: float,
):
    fig, ax1 = plt.subplots(figsize=(15, 8))

    linea1 = ax1.plot(
        serie_factura["fecha"],
        serie_factura["trm"],
        label="TRM",
        linewidth=2,
        color="tab:blue",
    )
    ax1.set_xlabel("Fecha")
    ax1.set_ylabel("TRM (COP por USD)", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")
    ax1.yaxis.set_major_formatter(FuncFormatter(formato_pesos_decimales))
    ax1.grid(True, alpha=0.3)

    ax2 = ax1.twinx()
    linea2 = ax2.plot(
        serie_factura["fecha"],
        serie_factura["diferencia_cambio_acumulada"],
        label="Dif. cambio acumulada",
        linestyle="--",
        linewidth=2.5,
        color="tab:red",
    )

    barras = ax2.bar(
        serie_factura["fecha"],
        serie_factura["diferencia_cambio_dia"].fillna(0),
        alpha=0.20,
        label="Dif. cambio del día",
    )

    ax2.axhline(y=0, linestyle=":", linewidth=1.5, color="black", alpha=0.8)
    ax2.set_ylabel("Diferencia en cambio (COP)", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")
    ax2.yaxis.set_major_formatter(FuncFormatter(formato_pesos))

    plt.title(f"Detalle de diferencia en cambio - Factura {factura}")

    lineas = linea1 + linea2
    etiquetas = [linea.get_label() for linea in lineas]
    ax1.legend(lineas + [barras], etiquetas + ["Dif. cambio del día"], loc="upper left")

    texto_resumen = (
        f"Dif. cambio acumulada: ${diferencia_acumulada:,.2f}   |   "
        f"Dif. cambio del día: ${diferencia_dia:,.2f}"
    )
    fig.text(
        0.5,
        0.02,
        texto_resumen,
        ha="center",
        va="center",
        fontsize=11,
        bbox=dict(boxstyle="round,pad=0.5", facecolor="white", edgecolor="black"),
    )

    fig.tight_layout(rect=[0, 0.06, 1, 1])
    return fig


# =========================
# APP
# =========================
st.set_page_config(page_title="Diferencia en cambio", layout="wide")
st.title("Diferencia en cambio - cartera en USD")

st.markdown("Sube los archivos y ejecuta el cálculo.")

facturas_file = st.file_uploader(
    "Facturas abiertas",
    type=["xlsx"],
    key="facturas",
)

monetizaciones_file = st.file_uploader(
    "Monetizaciones",
    type=["xlsx"],
    key="monetizaciones",
)

if facturas_file is not None:
    try:
        df_facturas = cargar_facturas(facturas_file)
        df_monetizaciones = cargar_monetizaciones(monetizaciones_file)

        control = validar_monetizaciones_vs_facturas(df_facturas, df_monetizaciones)

        fecha_inicial = df_facturas["fecha_factura"].min().normalize()
        fecha_final = pd.Timestamp.today().normalize()

        with st.spinner("Descargando TRM histórica..."):
            df_trm = descargar_trm_historica(fecha_inicial, fecha_final)

        trm_actual = float(df_trm["trm"].iloc[-1])

        detalle_actual = calcular_resumen_actual(df_facturas, df_monetizaciones, trm_actual)
        serie_total, detalle_diario = construir_serie_total(
            df_facturas, df_monetizaciones, df_trm
        )

        diferencia_total_actual = detalle_actual["diferencia_cambio_acumulada"].sum()
        valor_total_actual = detalle_actual["valor_actual_cop"].sum()
        saldo_total_actual_usd = detalle_actual["saldo_vivo_actual_usd"].sum()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("TRM actual", f"${trm_actual:,.2f}")
        c2.metric("Saldo total USD", f"${saldo_total_actual_usd:,.2f}")
        c3.metric("Valor total COP", f"${valor_total_actual:,.2f}")
        c4.metric("Dif. cambio acumulada", f"${diferencia_total_actual:,.2f}")

        with st.expander("Control de consistencia monetizaciones vs saldo actual"):
            if control.empty:
                st.info("No se cargaron monetizaciones.")
            else:
                st.dataframe(control, use_container_width=True)

        tab1, tab2, tab3, tab4 = st.tabs(
            ["Resumen", "Gráficas", "Factura individual", "Descarga"]
        )

        with tab1:
            st.subheader("Detalle actual por factura")
            st.dataframe(detalle_actual, use_container_width=True)

            st.subheader("Serie total")
            st.dataframe(serie_total.tail(30), use_container_width=True)

            st.caption(
                f"TRM desde {df_trm['fecha'].min().date()} hasta {df_trm['fecha'].max().date()}"
            )

        with tab2:
            st.subheader("Gráficas generales")
            st.pyplot(fig_trm(serie_total), clear_figure=True)
            st.pyplot(fig_valor_cartera(serie_total), clear_figure=True)
            st.pyplot(
                fig_trm_y_diferencia_cambio(serie_total, diferencia_total_actual),
                clear_figure=True,
            )
            st.pyplot(fig_pnl_cambiario(serie_total), clear_figure=True)

        with tab3:
            st.subheader("Consulta por factura")
            factura_sel = st.selectbox(
                "Selecciona una factura",
                options=sorted(df_facturas["factura"].astype(str).unique().tolist()),
            )

            if factura_sel:
                fila_factura = df_facturas[df_facturas["factura"] == factura_sel].iloc[0]
                serie_factura = construir_serie_factura(
                    fila_factura,
                    df_monetizaciones,
                    df_trm,
                )

                ultimo = serie_factura.iloc[-1]
                penultimo = serie_factura.iloc[-2] if len(serie_factura) > 1 else ultimo

                diferencia_acumulada = float(ultimo["diferencia_cambio_acumulada"])
                diferencia_dia = float(
                    ultimo["diferencia_cambio_acumulada"]
                    - penultimo["diferencia_cambio_acumulada"]
                )
                saldo_actual_usd = float(ultimo["saldo_vivo_usd"])
                valor_liquidacion_hoy = saldo_actual_usd * trm_actual

                a1, a2, a3 = st.columns(3)
                a1.metric("Saldo actual USD", f"${saldo_actual_usd:,.2f}")
                a2.metric("Dif. cambio acumulada", f"${diferencia_acumulada:,.2f}")
                a3.metric("Dif. cambio del día", f"${diferencia_dia:,.2f}")

                st.write(
                    {
                        "factura": factura_sel,
                        "cliente": fila_factura["cliente"],
                        "valor_original_usd": float(fila_factura["valor_usd"]),
                        "trm_actual": trm_actual,
                        "valor_liquidacion_hoy_cop": valor_liquidacion_hoy,
                    }
                )

                st.pyplot(
                    fig_factura_individual(
                        serie_factura,
                        factura_sel,
                        diferencia_acumulada,
                        diferencia_dia,
                    ),
                    clear_figure=True,
                )

        with tab4:
            st.subheader("Descargar resultados")
            excel_bytes = exportar_resultados_excel(
                detalle_actual,
                serie_total,
                detalle_diario,
            )

            st.download_button(
                label="Descargar resultado_diferencia_cambio.xlsx",
                data=excel_bytes,
                file_name="resultado_diferencia_cambio.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

    except Exception as e:
        st.error(f"Error: {e}")
else:
    st.info("Sube al menos el archivo de facturas para comenzar.")