from __future__ import annotations

import json
import os
from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter

# =========================
# CONFIGURACIÓN DE PÁGINA (Debe ser el primer comando)
# =========================
st.set_page_config(page_title="Dashboard Financiero", layout="wide")

# =========================
# CONFIGURACIÓN GENERAL
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
def formato_pesos(x, pos=None):
    return f"${x:,.0f}"

def formato_pesos_decimales(x, pos=None):
    return f"${x:,.2f}"


# =========================
# VALIDACIONES Y CARGA (Para Diferencia en Cambio)
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

    # Permite fechas vacías (NaT) para los anticipos sin factura
    df["fecha_factura"] = pd.to_datetime(df["fecha_factura"], errors="coerce")

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
    df["valor"] = df["valor"].apply(convertir_trm_a_float)
    df["vigenciadesde"] = pd.to_datetime(df["vigenciadesde"], errors="coerce")
    df["vigenciahasta"] = pd.to_datetime(df["vigenciahasta"], errors="coerce")

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

def asignar_trm_factura(df_facturas: pd.DataFrame, df_trm: pd.DataFrame) -> pd.DataFrame:
    facturas = df_facturas.copy()
    
    # Separar facturas con fecha (normales) y sin fecha (Anticipos puros)
    facturas["fecha_factura_norm"] = pd.to_datetime(facturas["fecha_factura"]).dt.normalize()
    df_con = facturas.dropna(subset=['fecha_factura_norm']).copy()
    df_sin = facturas[facturas['fecha_factura_norm'].isna()].copy()
    
    trm = df_trm.copy()
    trm["fecha"] = pd.to_datetime(trm["fecha"]).dt.normalize()
    trm = trm[["fecha", "trm"]].drop_duplicates(subset=["fecha"], keep="last").sort_values("fecha").reset_index(drop=True)
    
    if not df_con.empty:
        df_con = df_con.sort_values("fecha_factura_norm").reset_index(drop=True)
        df_con = pd.merge_asof(
            df_con, trm, left_on="fecha_factura_norm", right_on="fecha", direction="backward"
        )
        df_con = df_con.rename(columns={"trm": "trm_factura"}).drop(columns=["fecha"])
    
    df_sin["trm_factura"] = None
    
    facturas_final = pd.concat([df_con, df_sin], ignore_index=True).drop(columns=["fecha_factura_norm"])
    return facturas_final


# =========================
# CÁLCULOS
# =========================
def validar_monetizaciones_vs_facturas(df_facturas: pd.DataFrame, df_monetizaciones: pd.DataFrame) -> pd.DataFrame:
    if df_monetizaciones.empty:
        return pd.DataFrame()

    facturas_validas = set(df_facturas["factura"])
    facturas_monetizadas = set(df_monetizaciones["factura"])
    no_encontradas = facturas_monetizadas - facturas_validas
    if no_encontradas:
        raise ValueError(f"Hay monetizaciones para facturas que no existen: {sorted(no_encontradas)}")

    monet_por_factura = df_monetizaciones.groupby("factura", as_index=False)["monto_usd"].sum()
    control = df_facturas[["factura", "valor_usd"]].merge(monet_por_factura, on="factura", how="left").fillna(0)
    control["saldo_vivo_calculado"] = control["valor_usd"] - control["monto_usd"]
    control["saldo_vivo_calculado"] = control["saldo_vivo_calculado"].clip(lower=0)
    return control

def calcular_resumen_actual(
    df_facturas: pd.DataFrame, df_monetizaciones: pd.DataFrame,
    trm_actual: float, trm_ayer: float, spread: float
) -> pd.DataFrame:
    registros = []
    for _, fila in df_facturas.iterrows():
        factura_str = str(fila["factura"]).strip()
        es_sin_factura = pd.isna(fila["fecha_factura"])
        
        monet = df_monetizaciones[df_monetizaciones["factura"] == factura_str].copy()
        
        if es_sin_factura:
            anticipo_usd = float(monet["monto_usd"].sum()) if not monet.empty else 0.0
            abono_usd = 0.0
            saldo = max(float(fila["valor_usd"]) - anticipo_usd, 0.0)
            
            dif_anticipos = 0.0
            dif_realizada_post = 0.0
            dif_no_realizada = 0.0
            dif_total = 0.0
            dif_dia_base = 0.0
            dif_dia_plus_2pct = 0.0
            dif_dia_minus_2pct = 0.0
        else:
            fecha_factura = pd.to_datetime(fila["fecha_factura"]).normalize()
            if not monet.empty:
                monet["fecha"] = pd.to_datetime(monet["fecha"]).dt.normalize()
            
            anticipos = monet[monet["fecha"] < fecha_factura].copy() if not monet.empty else monet.copy()
            post = monet[monet["fecha"] >= fecha_factura].copy() if not monet.empty else monet.copy()
            
            anticipo_usd = float(anticipos["monto_usd"].sum()) if not anticipos.empty else 0.0
            abono_usd = float(post["monto_usd"].sum()) if not post.empty else 0.0
            saldo = max(float(fila["valor_usd"]) - anticipo_usd - abono_usd, 0.0)
            
            trm_fac = float(fila["trm_factura"]) if pd.notnull(fila["trm_factura"]) else trm_actual
            
            dif_anticipos = float(((trm_fac - anticipos["tasa_monetizacion"]) * anticipos["monto_usd"]).sum()) if not anticipos.empty else 0.0
            dif_realizada_post = float(((post["tasa_monetizacion"] - trm_fac) * post["monto_usd"]).sum()) if not post.empty else 0.0
            dif_no_realizada = saldo * (trm_actual - trm_fac)
            
            dif_total = dif_anticipos + dif_realizada_post + dif_no_realizada
            dif_dia_base = saldo * (trm_actual - trm_ayer)
            dif_dia_plus_2pct = saldo * (trm_actual * (1 + spread) - trm_ayer)
            dif_dia_minus_2pct = saldo * (trm_actual * (1 - spread) - trm_ayer)

        registros.append({
            "factura": fila["factura"],
            "cliente": fila["cliente"],
            "fecha_factura": pd.to_datetime(fila["fecha_factura"]).normalize() if not es_sin_factura else None,
            "valor_usd": float(fila["valor_usd"]),
            "trm_factura": fila["trm_factura"],
            "anticipo_previo_total_usd": anticipo_usd,
            "abonos_post_factura_total_usd": abono_usd,
            "saldo_vivo_actual_usd": saldo,
            "dif_anticipos": dif_anticipos,
            "dif_realizada_post": dif_realizada_post,
            "dif_no_realizada": dif_no_realizada,
            "dif_total": dif_total,
            "dif_dia_base": dif_dia_base,
            "dif_dia_plus_2pct": dif_dia_plus_2pct,
            "dif_dia_minus_2pct": dif_dia_minus_2pct,
        })
    return pd.DataFrame(registros)

def construir_saldos_diarios(
    df_facturas: pd.DataFrame, df_monetizaciones: pd.DataFrame,
    fechas: pd.Series, fechas_trm_map: pd.Series
) -> pd.DataFrame:
    registros = []
    monetizaciones_por_factura = {f: g.sort_values("fecha") for f, g in df_monetizaciones.groupby("factura")} if not df_monetizaciones.empty else {}

    for _, fila in df_facturas.iterrows():
        # Ignorar facturas sin fecha en la serie de tiempo
        if pd.isna(fila["fecha_factura"]):
            continue

        factura = fila["factura"]
        fecha_factura = pd.to_datetime(fila["fecha_factura"]).normalize()
        valor_usd = float(fila["valor_usd"])
        trm_factura = float(fila["trm_factura"])
        
        monet_fact = monetizaciones_por_factura.get(factura, pd.DataFrame(columns=["fecha", "factura", "monto_usd", "tasa_monetizacion"])).copy()
        
        if not monet_fact.empty:
            monet_fact["fecha"] = pd.to_datetime(monet_fact["fecha"]).dt.normalize()
            
        anticipos = monet_fact[monet_fact["fecha"] < fecha_factura].copy() if not monet_fact.empty else monet_fact.copy()
        post = monet_fact[monet_fact["fecha"] >= fecha_factura].copy() if not monet_fact.empty else monet_fact.copy()
        anticipo_previo_usd = float(anticipos["monto_usd"].sum()) if not anticipos.empty else 0.0
        
        dif_anticipos = float(((trm_factura - anticipos["tasa_monetizacion"]) * anticipos["monto_usd"]).sum()) if not anticipos.empty else 0.0

        for fecha in fechas:
            if fecha < fecha_factura:
                saldo_vivo = 0.0
                abonos_post_factura_usd = 0.0
                dif_realizada_post = 0.0
                dif_no_realizada = 0.0
            else:
                post_hasta_fecha = post[post["fecha"] <= fecha].copy() if not post.empty else post.copy()
                abonos_post_factura_usd = float(post_hasta_fecha["monto_usd"].sum()) if not post_hasta_fecha.empty else 0.0
                dif_realizada_post = float(((post_hasta_fecha["tasa_monetizacion"] - trm_factura) * post_hasta_fecha["monto_usd"]).sum()) if not post_hasta_fecha.empty else 0.0
                saldo_vivo = max(valor_usd - anticipo_previo_usd - abonos_post_factura_usd, 0.0)
                trm_del_dia = float(fechas_trm_map.loc[fecha]) if fecha in fechas_trm_map.index else None
                dif_no_realizada = saldo_vivo * (trm_del_dia - trm_factura) if trm_del_dia is not None else 0.0

            registros.append({
                "fecha": fecha, "factura": factura, "cliente": fila["cliente"],
                "valor_usd": valor_usd, "trm_factura": trm_factura,
                "anticipo_previo_usd": anticipo_previo_usd, "abonos_post_factura_usd": abonos_post_factura_usd,
                "saldo_vivo_usd": saldo_vivo, "dif_anticipos": dif_anticipos if fecha >= fecha_factura else 0.0,
                "dif_realizada_post": dif_realizada_post, "dif_no_realizada": dif_no_realizada,
                "dif_total": (dif_anticipos if fecha >= fecha_factura else 0.0) + dif_realizada_post + dif_no_realizada,
            })
    return pd.DataFrame(registros)

def construir_serie_total(df_facturas: pd.DataFrame, df_monetizaciones: pd.DataFrame, df_trm: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    fechas_trm_map = df_trm[["fecha", "trm"]].drop_duplicates(subset=["fecha"], keep="last").set_index("fecha")["trm"]
    fechas = df_trm["fecha"].sort_values().reset_index(drop=True)
    detalle_diario = construir_saldos_diarios(df_facturas, df_monetizaciones, fechas, fechas_trm_map)
    
    if detalle_diario.empty:
        return pd.DataFrame(), pd.DataFrame()

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
        ).sort_values("fecha").reset_index(drop=True)
    )
    serie_total["dif_dia_base"] = serie_total["dif_no_realizada_total"].diff().fillna(0)
    serie_total["variacion_diaria_trm"] = serie_total["trm"].diff()
    return serie_total, detalle_diario

def construir_serie_factura(fila_factura: pd.Series, df_monetizaciones: pd.DataFrame, df_trm: pd.DataFrame) -> pd.DataFrame:
    df_facturas_una = pd.DataFrame([fila_factura])
    fechas_trm_map = df_trm[["fecha", "trm"]].drop_duplicates(subset=["fecha"], keep="last").set_index("fecha")["trm"]
    fechas = df_trm["fecha"].sort_values().reset_index(drop=True)
    detalle_diario = construir_saldos_diarios(df_facturas_una, df_monetizaciones, fechas, fechas_trm_map)
    detalle_diario = detalle_diario.merge(df_trm, on="fecha", how="left")
    detalle_diario["dif_dia_base"] = detalle_diario["dif_no_realizada"].diff().fillna(0)
    return detalle_diario

# =========================
# EXPORTACIÓN Y GRÁFICOS
# =========================
def exportar_resultados_excel(detalle_actual, serie_total, detalle_diario) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        detalle_actual.to_excel(writer, sheet_name="detalle_actual", index=False)
        if not serie_total.empty:
            serie_total.to_excel(writer, sheet_name="serie_total", index=False)
        if not detalle_diario.empty:
            detalle_diario.to_excel(writer, sheet_name="detalle_diario", index=False)
    output.seek(0)
    return output.getvalue()

def fig_trm(serie_total):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(serie_total["fecha"], serie_total["trm"], linewidth=2)
    ax.set_title("Evolución diaria de la TRM")
    ax.set_ylabel("TRM (COP por USD)")
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos_decimales))
    fig.tight_layout()
    return fig

def fig_saldo_vivo(serie_total):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(serie_total["fecha"], serie_total["saldo_total_usd"], linewidth=2)
    ax.set_title("Saldo vivo total en USD")
    ax.set_ylabel("USD")
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
    fig.tight_layout()
    return fig

def fig_trm_y_diferencia_total(serie_total, diferencia_total_actual):
    fig, ax1 = plt.subplots(figsize=(15, 8))
    linea1 = ax1.plot(serie_total["fecha"], serie_total["trm"], label="TRM", linewidth=2, color="tab:blue")
    ax1.set_ylabel("TRM (COP por USD)", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")
    ax1.yaxis.set_major_formatter(FuncFormatter(formato_pesos_decimales))
    ax1.grid(True, alpha=0.3)
    
    ax2 = ax1.twinx()
    linea2 = ax2.plot(serie_total["fecha"], serie_total["dif_total"], label="Diferencia total", linestyle="--", linewidth=2.5, color="tab:red", zorder=3)
    ax2.axhline(y=0, linestyle=":", linewidth=1.5, color="black", alpha=0.8)
    ax2.set_ylabel("Diferencia total (COP)", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")
    ax2.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
    
    plt.title("TRM y diferencia en cambio total")
    lineas = linea1 + linea2
    etiquetas = [l.get_label() for l in lineas]
    ax1.legend(lineas, etiquetas, loc="upper left")
    
    texto_resumen = f"Diferencia total al día: ${diferencia_total_actual:,.2f}"
    fig.text(0.5, 0.02, texto_resumen, ha="center", va="center", fontsize=11, bbox=dict(boxstyle="round,pad=0.5", facecolor="white", edgecolor="black"))
    fig.tight_layout(rect=[0, 0.06, 1, 1])
    return fig

def fig_pnl_dia(serie_total):
    fig, ax = plt.subplots(figsize=(15, 6))
    ax.bar(serie_total["fecha"], serie_total["dif_dia_base"].fillna(0), width=1.0)
    ax.axhline(0, color="black", linewidth=1)
    ax.set_title("Diferencia del día sobre saldo vivo")
    ax.set_ylabel("COP")
    ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    return fig

def _preparar_puntos_monetizacion(serie_factura, df_monetizaciones, factura, fila_factura):
    monet = df_monetizaciones[df_monetizaciones["factura"] == factura].copy()
    if monet.empty:
        return monet.copy(), monet.copy()
    monet["fecha"] = pd.to_datetime(monet["fecha"]).dt.normalize()
    fecha_factura = pd.to_datetime(fila_factura["fecha_factura"]).normalize()
    curva_trm = serie_factura[["fecha", "trm"]].drop_duplicates(subset=["fecha"], keep="last").sort_values("fecha").reset_index(drop=True)
    monet = monet.sort_values("fecha").reset_index(drop=True)
    monet_plot = pd.merge_asof(monet, curva_trm, on="fecha", direction="backward")
    monet_plot["trm"] = monet_plot["trm"].ffill().bfill()
    monet_plot["tipo_mov"] = monet_plot["fecha"].apply(lambda x: "Anticipo" if x < fecha_factura else "Post-factura")
    monet_plot["dif_mov"] = monet_plot.apply(
        lambda r: (fila_factura["trm_factura"] - r["tasa_monetizacion"]) * r["monto_usd"] if r["tipo_mov"] == "Anticipo" else (r["tasa_monetizacion"] - fila_factura["trm_factura"]) * r["monto_usd"], axis=1
    )
    return monet_plot[monet_plot["tipo_mov"] == "Anticipo"].copy(), monet_plot[monet_plot["tipo_mov"] == "Post-factura"].copy()

def _escalar_tamano_puntos(montos, min_size=60, max_size=260):
    if montos.empty: return pd.Series(dtype=float)
    m_min, m_max = float(montos.min()), float(montos.max())
    if abs(m_max - m_min) < 1e-9: return pd.Series([140.0] * len(montos), index=montos.index)
    return min_size + (montos - m_min) * (max_size - min_size) / (m_max - m_min)

def fig_factura_individual(serie_factura, factura, dif_total_actual, dif_dia_base, df_monetizaciones, fila_factura, mostrar_etiquetas=True, umbral_etiqueta_usd=0.0):
    fig, ax1 = plt.subplots(figsize=(15, 8))
    ax1.plot(serie_factura["fecha"], serie_factura["trm"], label="TRM", linewidth=2, color="tab:blue")
    ax1.set_ylabel("TRM (COP por USD)", color="tab:blue")
    ax1.tick_params(axis="y", labelcolor="tab:blue")
    ax1.yaxis.set_major_formatter(FuncFormatter(formato_pesos_decimales))
    ax1.grid(True, alpha=0.3)
    ax2 = ax1.twinx()
    ax2.plot(serie_factura["fecha"], serie_factura["dif_total"], label="Diferencia total", linestyle="--", linewidth=2.5, color="tab:red")
    ax2.bar(serie_factura["fecha"], serie_factura["dif_dia_base"].fillna(0), alpha=0.20, label="Dif. del día")
    ax2.axhline(y=0, linestyle=":", linewidth=1.5, color="black", alpha=0.8)
    ax2.set_ylabel("Diferencia (COP)", color="tab:red")
    ax2.yaxis.set_major_formatter(FuncFormatter(formato_pesos))

    anticipos_plot, post_plot = _preparar_puntos_monetizacion(serie_factura, df_monetizaciones, factura, fila_factura)

    if not anticipos_plot.empty:
        anticipos_plot["size"] = _escalar_tamano_puntos(anticipos_plot["monto_usd"])
        anticipos_plot["color"] = anticipos_plot["dif_mov"].apply(lambda x: "green" if x >= 0 else "red")
        ax1.scatter(anticipos_plot["fecha"], anticipos_plot["trm"], s=anticipos_plot["size"], c=anticipos_plot["color"], alpha=0.85, edgecolors="black", linewidths=0.6, zorder=5)
        if mostrar_etiquetas:
            for _, row in anticipos_plot[anticipos_plot["monto_usd"] >= umbral_etiqueta_usd].iterrows():
                ax1.annotate(f"A | {row['monto_usd']:,.0f}\n{row['tasa_monetizacion']:,.0f}", (row["fecha"], row["trm"]), textcoords="offset points", xytext=(0, 10), ha="center", fontsize=8, color=row["color"])

    if not post_plot.empty:
        post_plot["size"] = _escalar_tamano_puntos(post_plot["monto_usd"])
        post_plot["color"] = post_plot["dif_mov"].apply(lambda x: "green" if x >= 0 else "red")
        ax1.scatter(post_plot["fecha"], post_plot["trm"], s=post_plot["size"], c=post_plot["color"], marker="s", alpha=0.85, edgecolors="black", linewidths=0.6, zorder=5)
        if mostrar_etiquetas:
            for _, row in post_plot[post_plot["monto_usd"] >= umbral_etiqueta_usd].iterrows():
                ax1.annotate(f"P | {row['monto_usd']:,.0f}\n{row['tasa_monetizacion']:,.0f}", (row["fecha"], row["trm"]), textcoords="offset points", xytext=(0, -18), ha="center", fontsize=8, color=row["color"])

    plt.title(f"Detalle de diferencia en cambio - Factura {factura}")
    legend_elements = [
        Line2D([0], [0], color="tab:blue", lw=2, label="TRM"),
        Line2D([0], [0], color="tab:red", lw=2.5, ls="--", label="Diferencia total"),
        Line2D([0], [0], marker="o", color="w", label="Anticipo", markerfacecolor="gray", markeredgecolor="black", markersize=8),
        Line2D([0], [0], marker="s", color="w", label="Post-factura", markerfacecolor="gray", markeredgecolor="black", markersize=8),
        Line2D([0], [0], marker="o", color="w", label="Ganancia", markerfacecolor="green", markeredgecolor="black", markersize=8),
        Line2D([0], [0], marker="o", color="w", label="Pérdida", markerfacecolor="red", markeredgecolor="black", markersize=8),
    ]
    ax1.legend(handles=legend_elements, loc="upper left")
    texto_resumen = f"Dif. total: ${dif_total_actual:,.2f}   |   Dif. del día: ${dif_dia_base:,.2f}"
    fig.text(0.5, 0.02, texto_resumen, ha="center", va="center", fontsize=11, bbox=dict(boxstyle="round,pad=0.5", facecolor="white", edgecolor="black"))
    fig.tight_layout(rect=[0, 0.06, 1, 1])
    return fig


# =========================
# APP 1: DIFERENCIA EN CAMBIO (MAIN)
# =========================
def app_diferencia_cambio():
    st.title("Diferencia en cambio - cartera en USD")
    st.markdown("Sube los archivos y ejecuta el cálculo.")

    spread = st.sidebar.number_input(
        "Spread bancario para escenarios del día",
        min_value=0.0, max_value=0.20, value=SPREAD_POR_DEFECTO, step=0.005, format="%.3f",
    )

    facturas_file = st.file_uploader("Facturas abiertas", type=["xlsx"], key="facturas")
    monetizaciones_file = st.file_uploader("Monetizaciones", type=["xlsx"], key="monetizaciones")

    if facturas_file is not None:
        try:
            df_facturas = cargar_facturas(facturas_file)
            df_monetizaciones = cargar_monetizaciones(monetizaciones_file)
            
            fechas_validas = df_facturas["fecha_factura"].dropna()
            fecha_inicial = fechas_validas.min().normalize() if not fechas_validas.empty else pd.Timestamp.today().normalize()
            fecha_final = pd.Timestamp.today().normalize()

            with st.spinner("Descargando TRM histórica..."):
                df_trm = descargar_trm_historica(fecha_inicial, fecha_final)

            df_facturas = asignar_trm_factura(df_facturas, df_trm)
            control = validar_monetizaciones_vs_facturas(df_facturas, df_monetizaciones)

            trm_actual = float(df_trm["trm"].iloc[-1])
            trm_ayer = float(df_trm["trm"].iloc[-2]) if len(df_trm) > 1 else trm_actual

            detalle_actual = calcular_resumen_actual(df_facturas, df_monetizaciones, trm_actual, trm_ayer, spread)
            serie_total, detalle_diario = construir_serie_total(df_facturas, df_monetizaciones, df_trm)

            df_sin_factura = detalle_actual[detalle_actual["fecha_factura"].isna()]
            df_facturadas = detalle_actual[detalle_actual["fecha_factura"].notna()]

            st.markdown("---")
            
            if not df_sin_factura.empty:
                st.markdown("### 🚧 Proyectos / Anticipos sin Facturar")
                st.markdown("Facturas sin fecha de generación. Se contabiliza su saldo pero no generan diferencia en cambio aún.")
                s1, s2, s3 = st.columns(3)
                s1.metric("Valor Total Proyectos", f"${df_sin_factura['valor_usd'].sum():,.2f}")
                s2.metric("Total Pagado (Anticipos)", f"${df_sin_factura['anticipo_previo_total_usd'].sum():,.2f}")
                s3.metric("Saldo Vivo sin Facturar", f"${df_sin_factura['saldo_vivo_actual_usd'].sum():,.2f}")
                st.dataframe(df_sin_factura[["factura", "cliente", "valor_usd", "anticipo_previo_total_usd", "saldo_vivo_actual_usd"]], use_container_width=True)

            st.markdown("### 📊 Facturación y Diferencia en Cambio")
            
            if not df_facturadas.empty:
                dif_total_actual = float(df_facturadas["dif_total"].sum())
                saldo_total_actual_usd = float(df_facturadas["saldo_vivo_actual_usd"].sum())
                dif_realizada_total = float((df_facturadas["dif_anticipos"] + df_facturadas["dif_realizada_post"]).sum())
                dif_no_realizada_total = float(df_facturadas["dif_no_realizada"].sum())
                dif_dia_base_total = float(df_facturadas["dif_dia_base"].sum())
                dif_dia_plus_total = float(df_facturadas["dif_dia_plus_2pct"].sum())
                dif_dia_minus_total = float(df_facturadas["dif_dia_minus_2pct"].sum())

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
                    if control.empty: st.info("No se cargaron monetizaciones.")
                    else: st.dataframe(control, use_container_width=True)

                tab1, tab2, tab3, tab4 = st.tabs(["Resumen", "Gráficas", "Factura individual", "Descarga"])

                with tab1:
                    st.subheader("Detalle actual por factura")
                    st.dataframe(df_facturadas, use_container_width=True)
                    st.subheader("Serie total")
                    if not serie_total.empty:
                        st.dataframe(serie_total.tail(30), use_container_width=True)
                        st.caption(f"TRM desde {df_trm['fecha'].min().date()} hasta {df_trm['fecha'].max().date()}")

                with tab2:
                    st.subheader("Gráficas generales")
                    if not serie_total.empty:
                        st.pyplot(fig_trm(serie_total), clear_figure=True)
                        st.pyplot(fig_saldo_vivo(serie_total), clear_figure=True)
                        st.pyplot(fig_trm_y_diferencia_total(serie_total, dif_total_actual), clear_figure=True)
                        st.pyplot(fig_pnl_dia(serie_total), clear_figure=True)

                with tab3:
                    st.subheader("Consulta por factura")
                    factura_sel = st.selectbox("Selecciona una factura", options=sorted(df_facturadas["factura"].astype(str).unique().tolist()))
                    if factura_sel:
                        fila_factura = df_facturadas[df_facturadas["factura"] == factura_sel].iloc[0]
                        col_opt1, col_opt2 = st.columns(2)
                        with col_opt1: mostrar_etiquetas = st.checkbox("Mostrar etiquetas de monetizaciones", value=True)
                        with col_opt2: umbral_etiqueta_usd = st.number_input("Etiquetar solo movimientos desde USD", min_value=0.0, step=500.0)

                        serie_factura = construir_serie_factura(fila_factura, df_monetizaciones, df_trm)
                        if not serie_factura.empty:
                            ultimo = serie_factura.iloc[-1]
                            st.pyplot(fig_factura_individual(
                                serie_factura, factura_sel, float(ultimo["dif_total"]), float(ultimo["dif_dia_base"]),
                                df_monetizaciones, fila_factura, mostrar_etiquetas=mostrar_etiquetas, umbral_etiqueta_usd=umbral_etiqueta_usd
                            ), clear_figure=True)

                with tab4:
                    st.subheader("Descargar resultados")
                    excel_bytes = exportar_resultados_excel(detalle_actual, serie_total, detalle_diario)
                    st.download_button("Descargar resultado_diferencia_cambio.xlsx", data=excel_bytes, file_name="resultado_diferencia_cambio.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.info("No hay facturas con fecha registrada para calcular diferencia en cambio.")

        except Exception as e:
            st.error(f"Error: {e}")
    else:
        st.info("Sube al menos el archivo de facturas para comenzar.")


# =========================
# APP 2: FACTURAS DE COMPRAS Y PAGOS
# =========================
def procesar_compras_dataframe(compras_files):
    if not compras_files: return pd.DataFrame()
    dfs = []
    for file in compras_files:
        df_temp = pd.read_excel(file, header=0, engine="openpyxl")
        
        if isinstance(df_temp.columns, pd.MultiIndex):
            df_temp.columns = ["_".join([str(i) for i in col if str(i) != "nan"]) for col in df_temp.columns]
            
        df_temp.columns = [str(col).strip().upper().replace(" ", "_") for col in df_temp.columns]
        df_temp = df_temp.loc[:, ~df_temp.columns.duplicated()]
        
        rename_map = {"GENERACION": "GENERACIO", "FECHA_GENERACION": "GENERACIO"}
        df_temp = df_temp.rename(columns=rename_map)
        df_temp = df_temp.loc[:, ~df_temp.columns.duplicated()]

        columnas_requeridas = {"PROVEEDOR", "VALOR", "FACTURA", "GENERACIO", "VENCIMIENTO"}
        if not columnas_requeridas.issubset(df_temp.columns):
            st.warning(f"⚠️ El archivo '{file.name}' no tiene las columnas necesarias y será omitido.")
            continue

        df_temp = df_temp.dropna(subset=["GENERACIO"])
        df_temp = df_temp[df_temp["GENERACIO"].astype(str).str.strip() != ""]
        df_temp = df_temp.dropna(subset=["PROVEEDOR"])
        df_temp["PROVEEDOR"] = df_temp["PROVEEDOR"].astype(str).str.strip()
        
        # Formato Latino
        df_temp["GENERACIO"] = pd.to_datetime(df_temp["GENERACIO"], errors="coerce", dayfirst=True)
        df_temp["VENCIMIENTO"] = pd.to_datetime(df_temp["VENCIMIENTO"], errors="coerce", dayfirst=True)
        df_temp["VALOR"] = pd.to_numeric(df_temp["VALOR"], errors="coerce")
        df_temp = df_temp.dropna(subset=["GENERACIO", "VENCIMIENTO", "VALOR"])
        dfs.append(df_temp)
    
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def app_facturas_compras():
    st.title("📊 Dashboard de Facturas de Compras")
    st.markdown("Analiza tus proveedores y genera tu archivo de pagos a realizar.")

    compras_files = st.file_uploader(
        "Archivo(s) de Facturas de Compras",
        type=["xlsx", "xlsm", "xls"],
        key="compras_files",
        accept_multiple_files=True
    )

    if not compras_files:
        st.info("👆 Sube tus archivos Excel para comenzar el análisis.")
        return

    try:
        df_all = procesar_compras_dataframe(compras_files)

        if df_all.empty:
            st.error("❌ Ninguno de los archivos subidos contenía datos válidos.")
            return

        hoy = pd.Timestamp.today().normalize()
        df_all["MES_GENERACION"] = df_all["GENERACIO"].dt.strftime('%Y-%m')
        df_all["DIAS_CREDITO"] = (df_all["VENCIMIENTO"] - df_all["GENERACIO"]).dt.days
        df_all["VENCIDA"] = df_all["VENCIMIENTO"] < hoy
        df_all["DIAS_VENCIDA"] = (hoy - df_all["VENCIMIENTO"]).dt.days.clip(lower=0)
        df_all["DIAS_PARA_VENCER"] = (df_all["VENCIMIENTO"] - hoy).dt.days
        df_all["PROXIMO_A_VENCER"] = (df_all["DIAS_PARA_VENCER"] >= 1) & (df_all["DIAS_PARA_VENCER"] <= 7)

        def clasificar_riesgo(row):
            if row["VENCIDA"]:
                if row["DIAS_VENCIDA"] > 30: return "🔴 Vencida Crítica (>30d)"
                else: return "🟠 Vencida Reciente (1-30d)"
            else:
                if row["PROXIMO_A_VENCER"]: return "🟡 Próximo a Vencer (1-7d)"
                else: return "🟢 Al día (>7d)"

        df_all["RIESGO"] = df_all.apply(clasificar_riesgo, axis=1)

        tab1, tab2 = st.tabs(["📊 Análisis y Dashboard", "💸 Planeador de Pagos"])

        with tab1:
            st.subheader("📅 Rango de Análisis")
            meses_disponibles = sorted(df_all["MES_GENERACION"].unique())
            meses_sel = st.multiselect("Selecciona los meses a analizar:", options=meses_disponibles, default=meses_disponibles)
            
            if not meses_sel:
                st.warning("Selecciona al menos un mes para ver datos.")
                return

            df_filtrado = df_all[df_all["MES_GENERACION"].isin(meses_sel)].copy()

            st.subheader("🌐 Resumen de Cartera (Meses seleccionados)")
            saldo_total = df_filtrado["VALOR"].sum()
            saldo_al_dia = df_filtrado[df_filtrado["RIESGO"] == "🟢 Al día (>7d)"]["VALOR"].sum()
            saldo_proximo = df_filtrado[df_filtrado["RIESGO"] == "🟡 Próximo a Vencer (1-7d)"]["VALOR"].sum()
            saldo_venc_reciente = df_filtrado[df_filtrado["RIESGO"] == "🟠 Vencida Reciente (1-30d)"]["VALOR"].sum()
            saldo_venc_critico = df_filtrado[df_filtrado["RIESGO"] == "🔴 Vencida Crítica (>30d)"]["VALOR"].sum()

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("💰 Saldo Total", f"${saldo_total:,.0f}")
            c2.metric("🟢 Al Día", f"${saldo_al_dia:,.0f}")
            c3.metric("🟡 Próx. Vencer", f"${saldo_proximo:,.0f}")
            c4.metric("🟠 Venc. Reciente", f"${saldo_venc_reciente:,.0f}")
            c5.metric("🔴 Venc. Crítica", f"${saldo_venc_critico:,.0f}")

            fig, ax = plt.subplots(figsize=(12, 4))
            categorias = ["Total", "Al día", "Próx. a Vencer", "Venc. Reciente", "Venc. Crítica"]
            valores = [saldo_total, saldo_al_dia, saldo_proximo, saldo_venc_reciente, saldo_venc_critico]
            colores = ["#4A90E2", "#50E3C2", "#F5A623", "#F57C00", "#E15554"]

            bars = ax.bar(categorias, valores, color=colores, alpha=0.85)
            ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
            ax.set_title("Distribución de Saldos por Edades (General)", fontsize=14)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)

            for bar in bars:
                yval = bar.get_height()
                if yval > 0:
                    ax.text(bar.get_x() + bar.get_width()/2, yval, f'${yval:,.0f}', ha='center', va='bottom', fontsize=10, fontweight='bold')

            st.pyplot(fig, clear_figure=True)
            st.markdown("---")

            st.subheader("📌 Análisis por Grupo de Proveedores")
            proveedores_disponibles = sorted(df_filtrado["PROVEEDOR"].unique())
            proveedores_sel = st.multiselect("Selecciona uno o varios proveedores para agrupar:", options=proveedores_disponibles)

            if proveedores_sel:
                df_prov = df_filtrado[df_filtrado["PROVEEDOR"].isin(proveedores_sel)].copy()
                p_total = df_prov["VALOR"].sum()
                p_al_dia = df_prov[df_prov["RIESGO"] == "🟢 Al día (>7d)"]["VALOR"].sum()
                p_proximo = df_prov[df_prov["RIESGO"] == "🟡 Próximo a Vencer (1-7d)"]["VALOR"].sum()
                p_venc_reciente = df_prov[df_prov["RIESGO"] == "🟠 Vencida Reciente (1-30d)"]["VALOR"].sum()
                p_venc_critico = df_prov[df_prov["RIESGO"] == "🔴 Vencida Crítica (>30d)"]["VALOR"].sum()

                st.write(f"**Resumen del grupo seleccionado ({len(proveedores_sel)} proveedores):**")
                p1, p2, p3, p4, p5 = st.columns(5)
                p1.metric("Total Grupo", f"${p_total:,.0f}")
                p2.metric("Al día", f"${p_al_dia:,.0f}")
                p3.metric("Próx. Vencer", f"${p_proximo:,.0f}")
                p4.metric("Venc. Reciente", f"${p_venc_reciente:,.0f}")
                p5.metric("Venc. Crítica", f"${p_venc_critico:,.0f}")

                alerta_60_vencidas = df_prov[(df_prov["DIAS_CREDITO"] < 60) & df_prov["VENCIDA"]]
                if not alerta_60_vencidas.empty:
                    st.error("⚠️ **Atención: Hay facturas vencidas con crédito menor a 60 días en este grupo.** \n\n *(Revisar negociación con los proveedores correspondientes).*")

                st.write("**Detalle de facturas activas del grupo:**")
                riesgos_grupo = sorted(df_prov["RIESGO"].unique())
                riesgos_sel = st.multiselect("🔍 Filtrar tabla por Estado de Riesgo:", options=riesgos_grupo, default=riesgos_grupo)
                df_mostrar = df_prov[df_prov["RIESGO"].isin(riesgos_sel)]

                columnas_mostrar = ["PROVEEDOR", "FACTURA", "VALOR", "GENERACIO", "VENCIMIENTO", "DIAS_CREDITO", "DIAS_VENCIDA", "RIESGO"]
                st.dataframe(
                    df_mostrar[columnas_mostrar]
                    .sort_values(["RIESGO", "VENCIMIENTO"])
                    .style.format({
                        "VALOR": "${:,.2f}",
                        "GENERACIO": lambda t: t.strftime("%Y-%m-%d") if pd.notnull(t) else "",
                        "VENCIMIENTO": lambda t: t.strftime("%Y-%m-%d") if pd.notnull(t) else ""
                    }),
                    use_container_width=True
                )
            else:
                st.info("💡 Selecciona proveedores en la casilla de arriba para ver su detalle agrupado.")

        # =========================
        # PLANEADOR DE PAGOS
        # =========================
        with tab2:
            st.subheader("Generador de Archivo de Pagos")
            st.write("Selecciona las facturas que vas a cancelar. Luego descarga el archivo para usarlo en el Flujo de Caja.")
            
            df_pagos = df_all.copy()
            df_pagos.insert(0, "PAGAR", False)
            
            edited_df = st.data_editor(
                df_pagos[["PAGAR", "PROVEEDOR", "FACTURA", "VALOR", "VENCIMIENTO", "RIESGO", "GENERACIO"]],
                column_config={
                    "PAGAR": st.column_config.CheckboxColumn("Seleccionar", default=False),
                    "VALOR": st.column_config.NumberColumn(format="$%d")
                },
                disabled=["PROVEEDOR", "FACTURA", "VALOR", "VENCIMIENTO", "RIESGO", "GENERACIO"],
                hide_index=True,
                use_container_width=True
            )

            facturas_seleccionadas = edited_df[edited_df["PAGAR"]]

            if not facturas_seleccionadas.empty:
                st.success(f"Has seleccionado {len(facturas_seleccionadas)} facturas por un total de **${facturas_seleccionadas['VALOR'].sum():,.0f}**")
                
                output = BytesIO()
                with pd.ExcelWriter(output, engine="openpyxl") as writer:
                    df_export = df_all[df_all["FACTURA"].isin(facturas_seleccionadas["FACTURA"])].drop(columns=['MES_GENERACION', 'DIAS_CREDITO', 'VENCIDA', 'DIAS_VENCIDA', 'DIAS_PARA_VENCER', 'PROXIMO_A_VENCER', 'RIESGO'], errors='ignore')
                    df_export.to_excel(writer, index=False)
                
                st.download_button(
                    label="⬇️ Descargar Archivo de Pagos (Excel)",
                    data=output.getvalue(),
                    file_name="pagos_realizados.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

    except Exception as e:
        st.error(f"❌ Error procesando los archivos: {e}")


# =========================
# APP 3: FLUJO DE CAJA (FINAL)
# =========================
def obtener_semanas_fc():
    hoy = pd.Timestamp.today().normalize()
    lunes = hoy - pd.Timedelta(days=hoy.weekday())
    semanas = []
    for i in range(4):
        start = lunes + pd.Timedelta(days=7*i)
        end = start + pd.Timedelta(days=6)
        label = f"Sem {i+1} ({start.strftime('%d/%m')} - {end.strftime('%d/%m')})"
        semanas.append({"idx": i, "start": start, "end": end, "label": label})
    return semanas

def cargar_datos_manuales():
    if os.path.exists("flujo_caja_manual.json"):
        with open("flujo_caja_manual.json", "r") as f:
            return json.load(f)
    return {
        "saldo_inicial": 0, 
        "S1": {"ordinarios": 0, "extra": 0, "fijos": 0, "nomina": 0}, 
        "S2": {"ordinarios": 0, "extra": 0, "fijos": 0, "nomina": 0}, 
        "S3": {"ordinarios": 0, "extra": 0, "fijos": 0, "nomina": 0}, 
        "S4": {"ordinarios": 0, "extra": 0, "fijos": 0, "nomina": 0}
    }

def guardar_datos_manuales(data):
    with open("flujo_caja_manual.json", "w") as f:
        json.dump(data, f)

def app_flujo_caja():
    st.title("💸 Flujo de Caja a 4 Semanas")
    st.markdown("Proyección de ingresos en USD (convertidos a COP) y egresos por pagos a proveedores.")

    hoy_dt = pd.Timestamp.today()
    es_dia_edicion = hoy_dt.weekday() in [0, 1]

    col1, col2 = st.columns(2)
    with col1: st.info(f"📅 Hoy es: **{hoy_dt.strftime('%A, %d de %B')}**")
    with col2: 
        forzar_edicion = st.checkbox("⚙️ Forzar Edición (Modo Admin)")

    puede_editar = es_dia_edicion or forzar_edicion

    if not puede_editar:
        st.warning("🔒 **Modo Lectura Activo:** El Flujo de Caja solo se puede actualizar los días Lunes y Martes.")

    st.markdown("### 1. Carga de Archivos Base")
    colA, colB, colC, colD = st.columns(4)
    with colA: f_usd = st.file_uploader("CxC (USD)", type=["xlsx"])
    with colB: m_usd = st.file_uploader("Monetizaciones", type=["xlsx"])
    with colC: f_compras = st.file_uploader("CxP (COP)", type=["xlsx", "xlsm"], accept_multiple_files=True)
    with colD: f_pagos = st.file_uploader("Pagos Realizados (Conciliación)", type=["xlsx", "xlsm"], accept_multiple_files=True)

    if not (f_usd and f_compras):
        st.info("Sube al menos Cuentas por Cobrar en USD y un archivo de Compras en COP para ver la proyección.")
        return

    # Procesar Ingresos USD
    df_usd = cargar_facturas(f_usd)
    df_mon = cargar_monetizaciones(m_usd)
    
    fechas_validas = df_usd["fecha_factura"].dropna()
    fecha_inicial = fechas_validas.min().normalize() if not fechas_validas.empty else hoy_dt.normalize()
    df_trm = descargar_trm_historica(fecha_inicial, hoy_dt.normalize())
    
    df_usd = asignar_trm_factura(df_usd, df_trm)
    trm_hoy = float(df_trm["trm"].iloc[-1])
    resumen_usd = calcular_resumen_actual(df_usd, df_mon, trm_hoy, trm_hoy, 0.0)
    
    # Solo toma el saldo vivo de las facturadas (con fecha) para el 20%
    df_facturadas_usd = resumen_usd[resumen_usd["fecha_factura"].notna()]
    saldo_vivo_usd_total = df_facturadas_usd['saldo_vivo_actual_usd'].sum()
    saldo_vivo_cop_total = saldo_vivo_usd_total * trm_hoy
    ingreso_semanal_usd_cop = saldo_vivo_cop_total * 0.20 

    # Procesar Compras (COP) y descontar pagos
    df_compras = procesar_compras_dataframe(f_compras)
    
    if f_pagos and not df_compras.empty:
        df_pagos_hechos = procesar_compras_dataframe(f_pagos)
        if not df_pagos_hechos.empty:
            facturas_pagadas_lista = df_pagos_hechos["FACTURA"].astype(str).tolist()
            df_compras = df_compras[~df_compras["FACTURA"].astype(str).isin(facturas_pagadas_lista)]
            st.success(f"✅ Se han descontado {len(facturas_pagadas_lista)} facturas ya pagadas de la proyección.")

    semanas = obtener_semanas_fc()
    
    ap_semanas = [0, 0, 0, 0]
    if not df_compras.empty:
        ap_semanas[0] = df_compras[df_compras['VENCIMIENTO'] <= semanas[0]['end']]['VALOR'].sum()
        ap_semanas[1] = df_compras[(df_compras['VENCIMIENTO'] >= semanas[1]['start']) & (df_compras['VENCIMIENTO'] <= semanas[1]['end'])]['VALOR'].sum()
        ap_semanas[2] = df_compras[(df_compras['VENCIMIENTO'] >= semanas[2]['start']) & (df_compras['VENCIMIENTO'] <= semanas[2]['end'])]['VALOR'].sum()
        ap_semanas[3] = df_compras[(df_compras['VENCIMIENTO'] >= semanas[3]['start']) & (df_compras['VENCIMIENTO'] <= semanas[3]['end'])]['VALOR'].sum()

    data_manual = cargar_datos_manuales()

    st.markdown("### 2. Variables del Flujo (COP)")
    
    with st.form("form_flujo"):
        saldo_bancos = st.number_input("Saldo Inicial en Bancos (Ahorro COP)", value=float(data_manual.get("saldo_inicial", 0)), disabled=not puede_editar)
        
        st.markdown("**Proyección Manual a 4 Semanas**")
        cols = st.columns(4)
        nuevos_datos = {}
        
        for i, col in enumerate(cols):
            clave_s = f"S{i+1}"
            with col:
                st.write(f"**{semanas[i]['label']}**")
                st.caption(f"Ingreso Cartera (20%): ${ingreso_semanal_usd_cop:,.0f}")
                st.caption(f"CxP Proveedores: ${ap_semanas[i]:,.0f}")
                
                ing_ord = st.number_input(f"Ing. Ordinarios", value=float(data_manual[clave_s].get("ordinarios", 0)), key=f"ord_{i}", disabled=not puede_editar)
                ing_ex = st.number_input(f"Ingresos Extra", value=float(data_manual[clave_s].get("extra", 0)), key=f"ex_{i}", disabled=not puede_editar)
                g_fijos = st.number_input(f"Gastos Fijos", value=float(data_manual[clave_s].get("fijos", 0)), key=f"gf_{i}", disabled=not puede_editar)
                nom = st.number_input(f"Nómina/Seg", value=float(data_manual[clave_s].get("nomina", 0)), key=f"nom_{i}", disabled=not puede_editar)
                
                nuevos_datos[clave_s] = {"ordinarios": ing_ord, "extra": ing_ex, "fijos": g_fijos, "nomina": nom}

        submit = st.form_submit_button("💾 Guardar Proyección", disabled=not puede_editar)
        if submit:
            data_manual["saldo_inicial"] = saldo_bancos
            data_manual.update(nuevos_datos)
            guardar_datos_manuales(data_manual)
            st.success("¡Datos guardados correctamente!")

    # =========================
    # TABLA FINAL DE FLUJO DE CAJA
    # =========================
    st.markdown("### 3. Resultado Flujo de Caja")
    
    tabla_fc = []
    saldo_actual = data_manual.get("saldo_inicial", 0)

    for i in range(4):
        clave_s = f"S{i+1}"
        
        ing_ord_val = data_manual[clave_s].get("ordinarios", 0)
        ing_ex_val = data_manual[clave_s].get("extra", 0)
        gf_val = data_manual[clave_s].get("fijos", 0)
        nom_val = data_manual[clave_s].get("nomina", 0)
        
        ingresos_totales = ingreso_semanal_usd_cop + ing_ord_val + ing_ex_val
        egresos_totales = ap_semanas[i] + gf_val + nom_val
        flujo_neto = ingresos_totales - egresos_totales
        saldo_final = saldo_actual + flujo_neto
        
        tabla_fc.append({
            "Semana": semanas[i]['label'],
            "Saldo Inicial": saldo_actual,
            "+ Cartera USD (20%)": ingreso_semanal_usd_cop,
            "+ Ing. Ordinarios": ing_ord_val,
            "+ Ingresos Extra": ing_ex_val,
            "- CxP Proveedores": ap_semanas[i],
            "- Gastos Fijos": gf_val,
            "- Nómina": nom_val,
            "FLUJO NETO": flujo_neto,
            "SALDO FINAL": saldo_final
        })
        saldo_actual = saldo_final

    df_fc = pd.DataFrame(tabla_fc)
    
    st.dataframe(
        df_fc.style.format({col: "${:,.0f}" for col in df_fc.columns if col != "Semana"})
             .map(lambda x: 'color: red' if x < 0 else 'color: green', subset=['FLUJO NETO', 'SALDO FINAL']),
        use_container_width=True
    )

    # =========================
    # GRÁFICA COMBINADA 
    # =========================
    st.markdown("### 4. Gráfica de Flujo de Caja")
    
    fig_fc, ax_fc = plt.subplots(figsize=(12, 5))
    
    x_labels = [row["Semana"] for row in tabla_fc]
    netos = [row["FLUJO NETO"] for row in tabla_fc]
    saldos = [row["SALDO FINAL"] for row in tabla_fc]
    
    colores_barras = ["#50E3C2" if val >= 0 else "#E15554" for val in netos]
    
    bars = ax_fc.bar(x_labels, netos, color=colores_barras, alpha=0.8, label="Flujo Neto")
    ax_fc.axhline(0, color='black', linewidth=1.2)
    ax_fc.set_ylabel("Flujo Neto (COP)", fontweight='bold')
    ax_fc.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
    ax_fc.spines['top'].set_visible(False)
    
    ax_saldo = ax_fc.twinx()
    ax_saldo.plot(x_labels, saldos, color="#4A90E2", marker="o", linewidth=3, markersize=8, label="Saldo Final (Liquidez)")
    ax_saldo.set_ylabel("Saldo Final Acumulado (COP)", color="#4A90E2", fontweight='bold')
    ax_saldo.tick_params(axis="y", labelcolor="#4A90E2")
    ax_saldo.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
    ax_saldo.spines['top'].set_visible(False)
    
    lines_1, labels_1 = ax_fc.get_legend_handles_labels()
    lines_2, labels_2 = ax_saldo.get_legend_handles_labels()
    ax_fc.legend(lines_1 + lines_2, labels_1 + labels_2, loc="upper left")
    
    plt.title("Evolución del Flujo Neto Semanal vs Saldo en Bancos", fontsize=14, pad=15)
    fig_fc.tight_layout()
    st.pyplot(fig_fc, clear_figure=True)


# =========================
# MENÚ PRINCIPAL
# =========================
def main():
    st.sidebar.title("Navegación")
    app_seleccionada = st.sidebar.radio(
        "¿Qué aplicación quieres utilizar?",
        ("Diferencia en cambio", "Revisar facturas de compras", "Flujo de Caja a 4 Semanas")
    )
    
    st.sidebar.markdown("---")
    
    if app_seleccionada == "Diferencia en cambio":
        app_diferencia_cambio()
    elif app_seleccionada == "Revisar facturas de compras":
        app_facturas_compras()
    elif app_seleccionada == "Flujo de Caja a 4 Semanas":
        app_flujo_caja()
        
if __name__ == "__main__":
    main()