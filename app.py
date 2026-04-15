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
SPREAD_POR_DEFECTO = 0.02


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
    }
    faltantes = columnas_requeridas - set(df.columns)
    if faltantes:
        raise ValueError(
            f"Faltan columnas obligatorias en facturas_abiertas.xlsx: {sorted(faltantes)}"
        )


def validar_columnas_monetizaciones(df: pd.DataFrame) -> None:
    columnas_requeridas = {"fecha", "factura", "monto_usd", "tasa_monetizacion"}
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

    df["valor_usd"] = pd.to_numeric(df["valor_usd"], errors="coerce")
    if df["valor_usd"].isna().any():
        raise ValueError("Hay valores inválidos en 'valor_usd'.")

    df = df.copy()
    df["moneda"] = df["moneda"].astype(str).str.upper().str.strip()
    df["factura"] = df["factura"].astype(str).str.strip()
    df["cliente"] = df["cliente"].astype(str).str.strip()

    df = df[(df["moneda"] == "USD") & (df["valor_usd"] > 0)].copy()

    if df.empty:
        raise ValueError("No quedaron facturas activas en USD con valor_usd > 0.")

    return df.reset_index(drop=True)


def cargar_monetizaciones(archivo) -> pd.DataFrame:
    if archivo is None:
        return pd.DataFrame(columns=["fecha", "factura", "monto_usd", "tasa_monetizacion"])

    df = pd.read_excel(archivo)
    validar_columnas_monetizaciones(df)

    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    if df["fecha"].isna().any():
        raise ValueError("Hay fechas inválidas en monetizaciones.xlsx.")

    df["factura"] = df["factura"].astype(str).str.strip()
    df["monto_usd"] = pd.to_numeric(df["monto_usd"], errors="coerce")
    df["tasa_monetizacion"] = pd.to_numeric(df["tasa_monetizacion"], errors="coerce")

    if df["monto_usd"].isna().any():
        raise ValueError("Hay valores inválidos en 'monto_usd'.")
    if df["tasa_monetizacion"].isna().any():
        raise ValueError("Hay valores inválidos en 'tasa_monetizacion'.")
    if (df["monto_usd"] < 0).any():
        raise ValueError("No se permiten monetizaciones negativas.")
    if (df["tasa_monetizacion"] <= 0).any():
        raise ValueError("No se permiten tasas de monetización <= 0.")

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


def asignar_trm_factura(
    df_facturas: pd.DataFrame,
    df_trm: pd.DataFrame,
) -> pd.DataFrame:
    facturas = df_facturas.copy()
    trm = df_trm.copy()

    facturas["fecha_factura_norm"] = pd.to_datetime(facturas["fecha_factura"]).dt.normalize()
    trm["fecha"] = pd.to_datetime(trm["fecha"]).dt.normalize()

    trm = (
        trm[["fecha", "trm"]]
        .drop_duplicates(subset=["fecha"], keep="last")
        .sort_values("fecha")
        .reset_index(drop=True)
    )

    facturas = facturas.sort_values("fecha_factura_norm").reset_index(drop=True)

    facturas = pd.merge_asof(
        facturas,
        trm,
        left_on="fecha_factura_norm",
        right_on="fecha",
        direction="backward",
    )

    if facturas["trm"].isna().any():
        faltantes = facturas.loc[facturas["trm"].isna(), ["factura", "fecha_factura"]]
        raise ValueError(
            "No fue posible asignar TRM factura a algunas facturas:\n"
            f"{faltantes}"
        )

    facturas = facturas.rename(columns={"trm": "trm_factura"})
    facturas = facturas.drop(columns=["fecha", "fecha_factura_norm"])

    return facturas


# =========================
# CÁLCULOS
# =========================
def validar_monetizaciones_vs_facturas(
    df_facturas: pd.DataFrame,
    df_monetizaciones: pd.DataFrame
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

    control = df_facturas[["factura", "valor_usd"]].merge(
        monet_por_factura, on="factura", how="left"
    ).fillna(0)

    control["saldo_vivo_calculado"] = control["valor_usd"] - control["monto_usd"]

    excedidas = control[control["saldo_vivo_calculado"] < -1e-9]
    if not excedidas.empty:
        raise ValueError(
            "Hay facturas donde la monetización total supera el valor_usd original:\n"
            f"{excedidas[['factura', 'valor_usd', 'monto_usd', 'saldo_vivo_calculado']]}"
        )

    control["saldo_vivo_calculado"] = control["saldo_vivo_calculado"].clip(lower=0)

    return control


def separar_monetizaciones_factura(
    fila_factura: pd.Series,
    df_monetizaciones: pd.DataFrame,
):
    factura = str(fila_factura["factura"]).strip()
    fecha_factura = pd.to_datetime(fila_factura["fecha_factura"]).normalize()

    monet = df_monetizaciones[df_monetizaciones["factura"] == factura].copy()

    if monet.empty:
        anticipos = monet.copy()
        post = monet.copy()
    else:
        monet["fecha"] = pd.to_datetime(monet["fecha"]).dt.normalize()
        anticipos = monet[monet["fecha"] < fecha_factura].copy()
        post = monet[monet["fecha"] >= fecha_factura].copy()

    return anticipos, post


def calcular_resumen_actual(
    df_facturas: pd.DataFrame,
    df_monetizaciones: pd.DataFrame,
    trm_actual: float,
    trm_ayer: float,
    spread: float,
) -> pd.DataFrame:
    registros = []

    for _, fila in df_facturas.iterrows():
        factura = fila["factura"]
        cliente = fila["cliente"]
        fecha_factura = pd.to_datetime(fila["fecha_factura"]).normalize()
        valor_usd = float(fila["valor_usd"])
        trm_factura = float(fila["trm_factura"])

        anticipos, post = separar_monetizaciones_factura(fila, df_monetizaciones)

        anticipo_previo_total_usd = float(anticipos["monto_usd"].sum()) if not anticipos.empty else 0.0
        abonos_post_factura_total_usd = float(post["monto_usd"].sum()) if not post.empty else 0.0

        saldo_vivo_actual_usd = max(valor_usd - anticipo_previo_total_usd - abonos_post_factura_total_usd, 0.0)

        # Diferencia realizada por anticipos:
        # se compara TRM factura vs tasa de monetización del anticipo
        dif_anticipos = 0.0
        if not anticipos.empty:
            dif_anticipos = float(
                ((trm_factura - anticipos["tasa_monetizacion"]) * anticipos["monto_usd"]).sum()
            )

        # Diferencia realizada post-factura:
        # se compara tasa de monetización vs TRM factura
        dif_realizada_post = 0.0
        if not post.empty:
            dif_realizada_post = float(
                ((post["tasa_monetizacion"] - trm_factura) * post["monto_usd"]).sum()
            )

        # Diferencia no realizada:
        dif_no_realizada = saldo_vivo_actual_usd * (trm_actual - trm_factura)

        # Diferencia del día y escenarios
        dif_dia_base = saldo_vivo_actual_usd * (trm_actual - trm_ayer)
        trm_plus = trm_actual * (1 + spread)
        trm_minus = trm_actual * (1 - spread)
        dif_dia_plus = saldo_vivo_actual_usd * (trm_plus - trm_ayer)
        dif_dia_minus = saldo_vivo_actual_usd * (trm_minus - trm_ayer)

        dif_total = dif_anticipos + dif_realizada_post + dif_no_realizada

        registros.append(
            {
                "factura": factura,
                "cliente": cliente,
                "fecha_factura": fecha_factura,
                "valor_usd": valor_usd,
                "trm_factura": trm_factura,
                "anticipo_previo_total_usd": anticipo_previo_total_usd,
                "abonos_post_factura_total_usd": abonos_post_factura_total_usd,
                "saldo_vivo_actual_usd": saldo_vivo_actual_usd,
                "dif_anticipos": dif_anticipos,
                "dif_realizada_post": dif_realizada_post,
                "dif_no_realizada": dif_no_realizada,
                "dif_total": dif_total,
                "dif_dia_base": dif_dia_base,
                "dif_dia_plus_2pct": dif_dia_plus,
                "dif_dia_minus_2pct": dif_dia_minus,
            }
        )

    return pd.DataFrame(registros)


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
        fecha_factura = pd.to_datetime(fila["fecha_factura"]).normalize()
        valor_usd = float(fila["valor_usd"])
        trm_factura = float(fila["trm_factura"])
        cliente = fila["cliente"]

        monet_fact = monetizaciones_por_factura.get(
            factura,
            pd.DataFrame(columns=["fecha", "factura", "monto_usd", "tasa_monetizacion"])
        ).copy()

        if not monet_fact.empty:
            monet_fact["fecha"] = pd.to_datetime(monet_fact["fecha"]).dt.normalize()

        anticipos = monet_fact[monet_fact["fecha"] < fecha_factura].copy() if not monet_fact.empty else monet_fact.copy()
        post = monet_fact[monet_fact["fecha"] >= fecha_factura].copy() if not monet_fact.empty else monet_fact.copy()

        anticipo_previo_usd = float(anticipos["monto_usd"].sum()) if not anticipos.empty else 0.0
        dif_anticipos = 0.0
        if not anticipos.empty:
            dif_anticipos = float(
                ((trm_factura - anticipos["tasa_monetizacion"]) * anticipos["monto_usd"]).sum()
            )

        for fecha in fechas:
            if fecha < fecha_factura:
                saldo_vivo = 0.0
                abonos_post_factura_usd = 0.0
                dif_realizada_post = 0.0
                dif_no_realizada = 0.0
                dif_total = 0.0
            else:
                if not post.empty:
                    post_hasta_fecha = post[post["fecha"] <= fecha].copy()
                else:
                    post_hasta_fecha = post.copy()

                abonos_post_factura_usd = float(post_hasta_fecha["monto_usd"].sum()) if not post_hasta_fecha.empty else 0.0

                dif_realizada_post = 0.0
                if not post_hasta_fecha.empty:
                    dif_realizada_post = float(
                        ((post_hasta_fecha["tasa_monetizacion"] - trm_factura) * post_hasta_fecha["monto_usd"]).sum()
                    )

                saldo_vivo = max(valor_usd - anticipo_previo_usd - abonos_post_factura_usd, 0.0)
                trm_del_dia = float(fechas_trm_map.loc[fecha]) if fecha in fechas_trm_map.index else None
                dif_no_realizada = saldo_vivo * (trm_del_dia - trm_factura) if trm_del_dia is not None else 0.0
                dif_total = dif_anticipos + dif_realizada_post + dif_no_realizada

            registros.append(
                {
                    "fecha": fecha,
                    "factura": factura,
                    "cliente": cliente,
                    "valor_usd": valor_usd,
                    "trm_factura": trm_factura,
                    "anticipo_previo_usd": anticipo_previo_usd,
                    "abonos_post_factura_usd": abonos_post_factura_usd,
                    "saldo_vivo_usd": saldo_vivo,
                    "dif_anticipos": dif_anticipos if fecha >= fecha_factura else 0.0,
                    "dif_realizada_post": dif_realizada_post,
                    "dif_no_realizada": dif_no_realizada,
                    "dif_total": dif_total,
                }
            )

    return pd.DataFrame(registros)


def construir_serie_total(
    df_facturas: pd.DataFrame,
    df_monetizaciones: pd.DataFrame,
    df_trm: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    global fechas_trm_map
    fechas_trm_map = (
        df_trm[["fecha", "trm"]]
        .drop_duplicates(subset=["fecha"], keep="last")
        .set_index("fecha")["trm"]
    )

    fechas = df_trm["fecha"].sort_values().reset_index(drop=True)
    detalle_diario = construir_saldos_diarios(df_facturas, df_monetizaciones, fechas)
    detalle_diario = detalle_diario.merge(df_trm, on="fecha", how="left")

    serie_total = (
        detalle_diario.groupby("fecha", as_index=False)
        .agg(
            trm=("trm", "first"),
            saldo_total_usd=("saldo_vivo_usd", "sum"),
            dif_anticipos_total=("dif_anticipos", "sum"),
            dif_realizada_post_total=("dif_realizada_post", "sum"),
            dif_no_realizada_total=("dif_no_realizada", "sum"),
            dif_total=("dif_total", "sum"),
        )
        .sort_values("fecha")
        .reset_index(drop=True)
    )

    serie_total["dif_dia_base"] = serie_total["dif_no_realizada_total"].diff().fillna(0)
    serie_total["variacion_diaria_trm"] = serie_total["trm"].diff()

    return serie_total, detalle_diario


def construir_serie_factura(
    fila_factura: pd.Series,
    df_monetizaciones: pd.DataFrame,
    df_trm: pd.DataFrame,
) -> pd.DataFrame:
    factura = str(fila_factura["factura"]).strip()
    df_facturas_una = pd.DataFrame([fila_factura])
    detalle, _ = construir_serie_total(df_facturas_una, df_monetizaciones, df_trm)

    # reconstrucción detallada
    global fechas_trm_map
    fechas_trm_map = (
        df_trm[["fecha", "trm"]]
        .drop_duplicates(subset=["fecha"], keep="last")
        .set_index("fecha")["trm"]
    )

    fechas = df_trm["fecha"].sort_values().reset_index(drop=True)
    detalle_diario = construir_saldos_diarios(df_facturas_una, df_monetizaciones, fechas)
    detalle_diario = detalle_diario.merge(df_trm, on="fecha", how="left")

    serie_factura = detalle_diario[detalle_diario["factura"] == factura].copy()
    serie_factura["dif_dia_base"] = serie_factura["dif_no_realizada"].diff().fillna(0)

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


def fig_saldo_vivo(serie_total: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(serie_total["fecha"], serie_total["saldo_total_usd"], linewidth=2)
    ax.set_title("Saldo vivo total en USD")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("USD")
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
    fig.tight_layout()
    return fig


def fig_trm_y_diferencia_total(
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
        serie_total["dif_total"],
        label="Diferencia total",
        linestyle="--",
        linewidth=2.5,
        color="tab:red",
        zorder=3,
    )

    ax2.axhline(y=0, linestyle=":", linewidth=1.5, color="black", alpha=0.8)
    ax2.set_ylabel("Diferencia total (COP)", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")
    ax2.yaxis.set_major_formatter(FuncFormatter(formato_pesos))

    plt.title("TRM y diferencia en cambio total")

    lineas = linea1 + linea2
    etiquetas = [linea.get_label() for linea in lineas]
    ax1.legend(lineas, etiquetas, loc="upper left")

    texto_resumen = f"Diferencia total al día: ${diferencia_total_actual:,.2f}"
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


def fig_pnl_dia(serie_total: pd.DataFrame):
    fig, ax = plt.subplots(figsize=(15, 6))
    ax.bar(
        serie_total["fecha"],
        serie_total["dif_dia_base"].fillna(0),
        width=1.0,
    )
    ax.axhline(0, color="black", linewidth=1)
    ax.set_title("Diferencia del día sobre saldo vivo")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("COP")
    ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    return fig


def fig_factura_individual(
    serie_factura: pd.DataFrame,
    factura: str,
    dif_total_actual: float,
    dif_dia_base: float,
    df_monetizaciones: pd.DataFrame,
    fila_factura: pd.Series,
):
    fig, ax1 = plt.subplots(figsize=(15, 8))

    # =====================
    # LINEA TRM
    # =====================
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

    # =====================
    # LINEA DIFERENCIA
    # =====================
    ax2 = ax1.twinx()

    linea2 = ax2.plot(
        serie_factura["fecha"],
        serie_factura["dif_total"],
        label="Diferencia total",
        linestyle="--",
        linewidth=2.5,
        color="tab:red",
    )

    # barras diferencia del día
    barras = ax2.bar(
        serie_factura["fecha"],
        serie_factura["dif_dia_base"].fillna(0),
        alpha=0.20,
        label="Dif. del día",
    )

    ax2.axhline(y=0, linestyle=":", linewidth=1.5, color="black", alpha=0.8)
    ax2.set_ylabel("Diferencia (COP)", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")
    ax2.yaxis.set_major_formatter(FuncFormatter(formato_pesos))

    # =====================
    # MONETIZACIONES (PUNTOS)
    # =====================
    monet = df_monetizaciones[df_monetizaciones["factura"] == factura].copy()

    if not monet.empty:
        monet["fecha"] = pd.to_datetime(monet["fecha"]).dt.normalize()
        fecha_factura = pd.to_datetime(fila_factura["fecha_factura"]).normalize()

        anticipos = monet[monet["fecha"] < fecha_factura]
        post = monet[monet["fecha"] >= fecha_factura]

        # anticipos (verde)
        if not anticipos.empty:
            y_vals = serie_factura.set_index("fecha").loc[anticipos["fecha"], "trm"].values
            ax1.scatter(
                anticipos["fecha"],
                y_vals,
                color="green",
                s=60,
                label="Anticipos",
                zorder=5,
            )

            # etiquetas
            for i, row in anticipos.iterrows():
                ax1.annotate(
                    f"{row['monto_usd']:,.0f}\n{row['tasa_monetizacion']:,.0f}",
                    (row["fecha"], serie_factura.set_index("fecha").loc[row["fecha"], "trm"]),
                    textcoords="offset points",
                    xytext=(0, 10),
                    ha="center",
                    fontsize=8,
                    color="green",
                )

        # post factura (naranja)
        if not post.empty:
            y_vals = serie_factura.set_index("fecha").loc[post["fecha"], "trm"].values
            ax1.scatter(
                post["fecha"],
                y_vals,
                color="orange",
                s=60,
                label="Post-factura",
                zorder=5,
            )

            for i, row in post.iterrows():
                ax1.annotate(
                    f"{row['monto_usd']:,.0f}\n{row['tasa_monetizacion']:,.0f}",
                    (row["fecha"], serie_factura.set_index("fecha").loc[row["fecha"], "trm"]),
                    textcoords="offset points",
                    xytext=(0, -15),
                    ha="center",
                    fontsize=8,
                    color="orange",
                )

    # =====================
    # LEYENDA Y TITULO
    # =====================
    plt.title(f"Detalle de diferencia en cambio - Factura {factura}")

    lineas = linea1 + linea2
    etiquetas = [linea.get_label() for linea in lineas]
    ax1.legend(lineas + [barras], etiquetas + ["Dif. del día"], loc="upper left")

    # resumen abajo
    texto_resumen = (
        f"Dif. total: ${dif_total_actual:,.2f}   |   "
        f"Dif. del día: ${dif_dia_base:,.2f}"
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
st.caption("VERSION NUEVA - SIN SALDO_USD - TASA_MONETIZACION")
st.markdown("Sube los archivos y ejecuta el cálculo.")

spread = st.sidebar.number_input(
    "Spread bancario para escenarios del día",
    min_value=0.0,
    max_value=0.20,
    value=SPREAD_POR_DEFECTO,
    step=0.005,
    format="%.3f",
)

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

        fecha_inicial = df_facturas["fecha_factura"].min().normalize()
        fecha_final = pd.Timestamp.today().normalize()

        with st.spinner("Descargando TRM histórica..."):
            df_trm = descargar_trm_historica(fecha_inicial, fecha_final)

        df_facturas = asignar_trm_factura(df_facturas, df_trm)
        control = validar_monetizaciones_vs_facturas(df_facturas, df_monetizaciones)

        trm_actual = float(df_trm["trm"].iloc[-1])
        trm_ayer = float(df_trm["trm"].iloc[-2]) if len(df_trm) > 1 else trm_actual

        detalle_actual = calcular_resumen_actual(
            df_facturas,
            df_monetizaciones,
            trm_actual,
            trm_ayer,
            spread,
        )

        serie_total, detalle_diario = construir_serie_total(
            df_facturas,
            df_monetizaciones,
            df_trm
        )

        dif_total_actual = float(detalle_actual["dif_total"].sum())
        saldo_total_actual_usd = float(detalle_actual["saldo_vivo_actual_usd"].sum())
        dif_realizada_total = float((detalle_actual["dif_anticipos"] + detalle_actual["dif_realizada_post"]).sum())
        dif_no_realizada_total = float(detalle_actual["dif_no_realizada"].sum())
        dif_dia_base_total = float(detalle_actual["dif_dia_base"].sum())
        dif_dia_plus_total = float(detalle_actual["dif_dia_plus_2pct"].sum())
        dif_dia_minus_total = float(detalle_actual["dif_dia_minus_2pct"].sum())

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("TRM actual", f"${trm_actual:,.2f}")
        c2.metric("Saldo vivo total USD", f"${saldo_total_actual_usd:,.2f}")
        c3.metric("Dif. realizada total", f"${dif_realizada_total:,.2f}")
        c4.metric("Dif. no realizada total", f"${dif_no_realizada_total:,.2f}")

        c5, c6, c7 = st.columns(3)
        c5.metric("Dif. día base", f"${dif_dia_base_total:,.2f}")
        c6.metric(f"Dif. día +{spread:.1%}", f"${dif_dia_plus_total:,.2f}")
        c7.metric(f"Dif. día -{spread:.1%}", f"${dif_dia_minus_total:,.2f}")

        with st.expander("Control monetizaciones vs valor original"):
            if control.empty:
                st.info("No se cargaron monetizaciones.")
            else:
                st.dataframe(control, use_container_width=True)

        tab1, tab2, tab3, tab4 = st.tabs(
            ["Resumen", "Gráficas", "Factura individual", "Descarga"]
        )

        with tab1:
            st.subheader("Detalle actual por factura")
            st.dataframe(
                detalle_actual[
                    [
                        "factura",
                        "cliente",
                        "fecha_factura",
                        "valor_usd",
                        "trm_factura",
                        "anticipo_previo_total_usd",
                        "abonos_post_factura_total_usd",
                        "saldo_vivo_actual_usd",
                        "dif_anticipos",
                        "dif_realizada_post",
                        "dif_no_realizada",
                        "dif_total",
                        "dif_dia_base",
                        "dif_dia_plus_2pct",
                        "dif_dia_minus_2pct",
                    ]
                ],
                use_container_width=True,
            )

            st.subheader("Serie total")
            st.dataframe(serie_total.tail(30), use_container_width=True)

            st.caption(
                f"TRM desde {df_trm['fecha'].min().date()} hasta {df_trm['fecha'].max().date()}"
            )

        with tab2:
            st.subheader("Gráficas generales")
            st.pyplot(fig_trm(serie_total), clear_figure=True)
            st.pyplot(fig_saldo_vivo(serie_total), clear_figure=True)
            st.pyplot(fig_trm_y_diferencia_total(serie_total, dif_total_actual), clear_figure=True)
            st.pyplot(fig_pnl_dia(serie_total), clear_figure=True)

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
                saldo_vivo_actual = float(ultimo["saldo_vivo_usd"])
                dif_total_fact = float(ultimo["dif_total"])
                dif_dia_fact = float(ultimo["dif_dia_base"])

                dif_dia_plus_fact = saldo_vivo_actual * ((trm_actual * (1 + spread)) - trm_ayer)
                dif_dia_minus_fact = saldo_vivo_actual * ((trm_actual * (1 - spread)) - trm_ayer)

                a1, a2, a3, a4 = st.columns(4)
                a1.metric("Saldo vivo USD", f"${saldo_vivo_actual:,.2f}")
                a2.metric("Dif. total", f"${dif_total_fact:,.2f}")
                a3.metric("Dif. día base", f"${dif_dia_fact:,.2f}")
                a4.metric(f"Dif. día ±{spread:.1%}", f"+ ${dif_dia_plus_fact:,.2f} / - ${dif_dia_minus_fact:,.2f}")

                st.write(
                    {
                        "factura": factura_sel,
                        "cliente": fila_factura["cliente"],
                        "fecha_factura": fila_factura["fecha_factura"],
                        "valor_original_usd": float(fila_factura["valor_usd"]),
                        "trm_factura": float(fila_factura["trm_factura"]),
                        "trm_actual": trm_actual,
                    }
                )

                st.pyplot(
                    fig_factura_individual(
                    serie_factura,
                    factura_sel,
                    dif_total_fact,
                    dif_dia_fact,
                    df_monetizaciones,
                    fila_factura,
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