from __future__ import annotations

import json
import os
import tempfile
from io import BytesIO

import matplotlib.pyplot as plt
import pandas as pd
import requests
import streamlit as st
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter

# Intentar importar FPDF para los reportes en PDF
try:
    from fpdf import FPDF
except ImportError:
    FPDF = None

# =========================
# CONFIGURACIÓN DE PÁGINA
# =========================
st.set_page_config(page_title="Dashboard Financiero Pro", layout="wide")

# =========================
# ESTADO DE SESIÓN (Conexión y Seguridad)
# =========================
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "datos_credito" not in st.session_state:
    st.session_state.datos_credito = {"desembolso": 0, "cuota": 0, "activo": False}
if "datos_leaseback" not in st.session_state:
    st.session_state.datos_leaseback = {"desembolso": 0, "cuota": 0, "activo": False}

# =========================
# CONFIGURACIÓN GENERAL
# =========================
TRM_API_DATASETS = ["32sa-8pi3", "dit9-nnvp"]
DATOS_GOV_BASE = "https://www.datos.gov.co/resource"
SPREAD_POR_DEFECTO = 0.02


# =========================
# FORMATOS Y MATEMÁTICA
# =========================
def formato_pesos(x, pos=None):
    return f"${x:,.0f}"

def formato_pesos_decimales(x, pos=None):
    return f"${x:,.2f}"

def calcular_cuota_mensual(valor_credito, tasa_ea, meses):
    if valor_credito <= 0 or meses <= 0:
        return 0
    tasa_mensual = (1 + tasa_ea)**(1/12) - 1
    if tasa_mensual == 0:
        return valor_credito / meses
    cuota = valor_credito * (tasa_mensual * (1 + tasa_mensual)**meses) / ((1 + tasa_mensual)**meses - 1)
    return cuota


# =========================
# VALIDACIONES Y CARGA (Para Diferencia en Cambio)
# =========================
def validar_columnas_facturas(df: pd.DataFrame) -> None:
    columnas_requeridas = {"factura", "cliente", "fecha_factura", "moneda", "valor_usd"}
    faltantes = columnas_requeridas - set(df.columns)
    if faltantes:
        raise ValueError(f"Faltan columnas obligatorias en facturas_abiertas: {sorted(faltantes)}")

def validar_columnas_monetizaciones(df: pd.DataFrame) -> None:
    columnas_requeridas = {"fecha", "factura", "monto_usd", "tasa_monetizacion"}
    faltantes = columnas_requeridas - set(df.columns)
    if faltantes:
        raise ValueError(f"Faltan columnas obligatorias en monetizaciones: {sorted(faltantes)}")

def cargar_facturas(archivo) -> pd.DataFrame:
    df = pd.read_excel(archivo)
    validar_columnas_facturas(df)
    df["fecha_factura"] = pd.to_datetime(df["fecha_factura"], errors="coerce")
    df["valor_usd"] = pd.to_numeric(df["valor_usd"], errors="coerce")
    df = df.copy()
    df["moneda"] = df["moneda"].astype(str).str.upper().str.strip()
    df["factura"] = df["factura"].astype(str).str.strip()
    df["cliente"] = df["cliente"].astype(str).str.strip()
    df = df[(df["moneda"] == "USD") & (df["valor_usd"] > 0)].copy()
    return df.reset_index(drop=True)

def cargar_monetizaciones(archivo) -> pd.DataFrame:
    if archivo is None:
        return pd.DataFrame(columns=["fecha", "factura", "monto_usd", "tasa_monetizacion"])
    df = pd.read_excel(archivo)
    validar_columnas_monetizaciones(df)
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    df["factura"] = df["factura"].astype(str).str.strip()
    df["monto_usd"] = pd.to_numeric(df["monto_usd"], errors="coerce")
    df["tasa_monetizacion"] = pd.to_numeric(df["tasa_monetizacion"], errors="coerce")
    return df.sort_values(["factura", "fecha"]).reset_index(drop=True)


# =========================
# TRM AUTOMÁTICA
# =========================
def convertir_trm_a_float(valor):
    if pd.isna(valor): return None
    s = str(valor).strip().replace("$", "").replace(" ", "")
    if "," in s and "." in s: s = s.replace(",", "")
    elif s.count(",") == 1 and s.count(".") >= 1: s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s: s = s.replace(",", ".")
    return float(s)

@st.cache_data(show_spinner=False)
def descargar_trm_historica(start_date: pd.Timestamp, end_date: pd.Timestamp) -> pd.DataFrame:
    for dataset_id in TRM_API_DATASETS:
        try:
            url = f"{DATOS_GOV_BASE}/{dataset_id}.json"
            start_iso = start_date.strftime("%Y-%m-%dT00:00:00")
            end_iso = end_date.strftime("%Y-%m-%dT23:59:59")
            params = {
                "$select": "valor,vigenciadesde,vigenciahasta",
                "$where": f"vigenciadesde <= '{end_iso}' AND vigenciahasta >= '{start_iso}'",
                "$order": "vigenciadesde ASC", "$limit": 50000,
            }
            response = requests.get(url, params=params, timeout=30)
            data = response.json()
            if not data: continue
            df = pd.DataFrame(data)
            df["valor"] = df["valor"].apply(convertir_trm_a_float)
            df["vigenciadesde"] = pd.to_datetime(df["vigenciadesde"], errors="coerce")
            df["vigenciahasta"] = pd.to_datetime(df["vigenciahasta"], errors="coerce")
            registros = []
            for _, row in df.iterrows():
                rango = pd.date_range(start=row["vigenciadesde"].normalize(), end=row["vigenciahasta"].normalize(), freq="D")
                for fecha in rango:
                    if start_date.normalize() <= fecha <= end_date.normalize():
                        registros.append({"fecha": fecha, "trm": float(row["valor"])})
            df_ex = pd.DataFrame(registros)
            if not df_ex.empty:
                return df_ex.drop_duplicates(subset=["fecha"], keep="last").sort_values("fecha").reset_index(drop=True)
        except Exception:
            continue
    raise ValueError("No fue posible descargar TRM actualizada.")

def asignar_trm_factura(df_facturas: pd.DataFrame, df_trm: pd.DataFrame) -> pd.DataFrame:
    facturas = df_facturas.copy()
    facturas["fecha_factura_norm"] = pd.to_datetime(facturas["fecha_factura"]).dt.normalize()
    df_con = facturas.dropna(subset=['fecha_factura_norm']).copy()
    df_sin = facturas[facturas['fecha_factura_norm'].isna()].copy()
    trm = df_trm.copy()
    trm["fecha"] = pd.to_datetime(trm["fecha"]).dt.normalize()
    trm = trm[["fecha", "trm"]].drop_duplicates(subset=["fecha"], keep="last").sort_values("fecha").reset_index(drop=True)
    if not df_con.empty:
        df_con = df_con.sort_values("fecha_factura_norm").reset_index(drop=True)
        df_con = pd.merge_asof(df_con, trm, left_on="fecha_factura_norm", right_on="fecha", direction="backward")
        df_con = df_con.rename(columns={"trm": "trm_factura"}).drop(columns=["fecha"])
    df_sin["trm_factura"] = None
    return pd.concat([df_con, df_sin], ignore_index=True).drop(columns=["fecha_factura_norm"])


# =========================
# CÁLCULOS (Diferencia Cambio)
# =========================
def validar_monetizaciones_vs_facturas(df_facturas: pd.DataFrame, df_monetizaciones: pd.DataFrame) -> pd.DataFrame:
    if df_monetizaciones.empty: return pd.DataFrame()
    monet_por_factura = df_monetizaciones.groupby("factura", as_index=False)["monto_usd"].sum()
    control = df_facturas[["factura", "valor_usd"]].merge(monet_por_factura, on="factura", how="left").fillna(0)
    control["saldo_vivo_calculado"] = control["valor_usd"] - control["monto_usd"]
    return control

def calcular_resumen_actual(df_facturas: pd.DataFrame, df_monetizaciones: pd.DataFrame, trm_actual: float, trm_ayer: float, spread: float) -> pd.DataFrame:
    registros = []
    for _, fila in df_facturas.iterrows():
        factura_str = str(fila["factura"]).strip()
        es_sin_factura = pd.isna(fila["fecha_factura"])
        monet = df_monetizaciones[df_monetizaciones["factura"] == factura_str].copy()
        
        if es_sin_factura:
            anticipo_usd = float(monet["monto_usd"].sum()) if not monet.empty else 0.0
            saldo = max(float(fila["valor_usd"]) - anticipo_usd, 0.0)
            registros.append({
                "factura": fila["factura"], "cliente": fila["cliente"], "fecha_factura": None,
                "valor_usd": float(fila["valor_usd"]), "trm_factura": None,
                "anticipo_previo_total_usd": anticipo_usd, "abonos_post_factura_total_usd": 0.0,
                "saldo_vivo_actual_usd": saldo, "dif_anticipos": 0.0, "dif_realizada_post": 0.0,
                "dif_no_realizada": 0.0, "dif_total": 0.0, "dif_dia_base": 0.0,
                "dif_dia_plus_2pct": 0.0, "dif_dia_minus_2pct": 0.0,
            })
        else:
            fecha_factura = pd.to_datetime(fila["fecha_factura"]).normalize()
            if not monet.empty: monet["fecha"] = pd.to_datetime(monet["fecha"]).dt.normalize()
            anticipos = monet[monet["fecha"] < fecha_factura].copy() if not monet.empty else monet.copy()
            post = monet[monet["fecha"] >= fecha_factura].copy() if not monet.empty else monet.copy()
            anticipo_usd = float(anticipos["monto_usd"].sum()) if not anticipos.empty else 0.0
            abono_usd = float(post["monto_usd"].sum()) if not post.empty else 0.0
            saldo = max(float(fila["valor_usd"]) - anticipo_usd - abono_usd, 0.0)
            trm_fac = float(fila["trm_factura"]) if pd.notnull(fila["trm_factura"]) else trm_actual
            dif_anticipos = float(((trm_fac - anticipos["tasa_monetizacion"]) * anticipos["monto_usd"]).sum()) if not anticipos.empty else 0.0
            dif_realizada_post = float(((post["tasa_monetizacion"] - trm_fac) * post["monto_usd"]).sum()) if not post.empty else 0.0
            dif_no_realizada = saldo * (trm_actual - trm_fac)
            registros.append({
                "factura": fila["factura"], "cliente": fila["cliente"], "fecha_factura": fecha_factura,
                "valor_usd": float(fila["valor_usd"]), "trm_factura": trm_fac,
                "anticipo_previo_total_usd": anticipo_usd, "abonos_post_factura_total_usd": abono_usd,
                "saldo_vivo_actual_usd": saldo, "dif_anticipos": dif_anticipos, "dif_realizada_post": dif_realizada_post,
                "dif_no_realizada": dif_no_realizada, "dif_total": dif_anticipos + dif_realizada_post + dif_no_realizada,
                "dif_dia_base": saldo * (trm_actual - trm_ayer),
                "dif_dia_plus_2pct": saldo * (trm_actual * (1 + spread) - trm_ayer),
                "dif_dia_minus_2pct": saldo * (trm_actual * (1 - spread) - trm_ayer),
            })
    return pd.DataFrame(registros)

def construir_saldos_diarios(df_facturas: pd.DataFrame, df_monetizaciones: pd.DataFrame, fechas: pd.Series, fechas_trm_map: pd.Series) -> pd.DataFrame:
    registros = []
    monetizaciones_por_factura = {f: g.sort_values("fecha") for f, g in df_monetizaciones.groupby("factura")} if not df_monetizaciones.empty else {}
    for _, fila in df_facturas.iterrows():
        if pd.isna(fila["fecha_factura"]): continue
        factura, fecha_factura = fila["factura"], pd.to_datetime(fila["fecha_factura"]).normalize()
        valor_usd, trm_factura = float(fila["valor_usd"]), float(fila["trm_factura"])
        monet_fact = monetizaciones_por_factura.get(factura, pd.DataFrame(columns=["fecha", "factura", "monto_usd", "tasa_monetizacion"])).copy()
        if not monet_fact.empty: monet_fact["fecha"] = pd.to_datetime(monet_fact["fecha"]).dt.normalize()
        anticipos = monet_fact[monet_fact["fecha"] < fecha_factura].copy() if not monet_fact.empty else monet_fact.copy()
        post = monet_fact[monet_fact["fecha"] >= fecha_factura].copy() if not monet_fact.empty else monet_fact.copy()
        anticipo_previo_usd = float(anticipos["monto_usd"].sum()) if not anticipos.empty else 0.0
        dif_anticipos = float(((trm_factura - anticipos["tasa_monetizacion"]) * anticipos["monto_usd"]).sum()) if not anticipos.empty else 0.0
        for fecha in fechas:
            if fecha < fecha_factura:
                saldo_vivo = abonos_post_factura_usd = dif_realizada_post = dif_no_realizada = dif_total = 0.0
            else:
                post_hasta_fecha = post[post["fecha"] <= fecha].copy() if not post.empty else post.copy()
                abonos_post_factura_usd = float(post_hasta_fecha["monto_usd"].sum()) if not post_hasta_fecha.empty else 0.0
                dif_realizada_post = float(((post_hasta_fecha["tasa_monetizacion"] - trm_factura) * post_hasta_fecha["monto_usd"]).sum()) if not post_hasta_fecha.empty else 0.0
                saldo_vivo = max(valor_usd - anticipo_previo_usd - abonos_post_factura_usd, 0.0)
                trm_del_dia = float(fechas_trm_map.loc[fecha]) if fecha in fechas_trm_map.index else None
                dif_no_realizada = saldo_vivo * (trm_del_dia - trm_factura) if trm_del_dia is not None else 0.0
                dif_total = dif_anticipos + dif_realizada_post + dif_no_realizada
            registros.append({
                "fecha": fecha, "factura": factura, "cliente": fila["cliente"], "valor_usd": valor_usd, "trm_factura": trm_factura,
                "anticipo_previo_usd": anticipo_previo_usd, "abonos_post_factura_usd": abonos_post_factura_usd,
                "saldo_vivo_usd": saldo_vivo, "dif_anticipos": dif_anticipos if fecha >= fecha_factura else 0.0,
                "dif_realizada_post": dif_realizada_post, "dif_no_realizada": dif_no_realizada, "dif_total": dif_total,
            })
    return pd.DataFrame(registros)

def construir_serie_total(df_facturas: pd.DataFrame, df_monetizaciones: pd.DataFrame, df_trm: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    fechas_trm_map = df_trm[["fecha", "trm"]].drop_duplicates(subset=["fecha"], keep="last").set_index("fecha")["trm"]
    fechas = df_trm["fecha"].sort_values().reset_index(drop=True)
    detalle_diario = construir_saldos_diarios(df_facturas, df_monetizaciones, fechas, fechas_trm_map)
    if detalle_diario.empty: return pd.DataFrame(), pd.DataFrame()
    detalle_diario = detalle_diario.merge(df_trm, on="fecha", how="left")
    serie_total = (
        detalle_diario.groupby("fecha", as_index=False)
        .agg(trm=("trm", "first"), saldo_total_usd=("saldo_vivo_usd", "sum"), dif_anticipos_total=("dif_anticipos", "sum"),
             dif_realizada_post_total=("dif_realizada_post", "sum"), dif_no_realizada_total=("dif_no_realizada", "sum"), dif_total=("dif_total", "sum"))
        .sort_values("fecha").reset_index(drop=True)
    )
    serie_total["dif_dia_base"] = serie_total["dif_no_realizada_total"].diff().fillna(0)
    return serie_total, detalle_diario

def construir_serie_factura(fila_factura: pd.Series, df_monetizaciones: pd.DataFrame, df_trm: pd.DataFrame) -> pd.DataFrame:
    fechas_trm_map = df_trm[["fecha", "trm"]].drop_duplicates(subset=["fecha"], keep="last").set_index("fecha")["trm"]
    fechas = df_trm["fecha"].sort_values().reset_index(drop=True)
    detalle_diario = construir_saldos_diarios(pd.DataFrame([fila_factura]), df_monetizaciones, fechas, fechas_trm_map)
    detalle_diario = detalle_diario.merge(df_trm, on="fecha", how="left")
    detalle_diario["dif_dia_base"] = detalle_diario["dif_no_realizada"].diff().fillna(0)
    return detalle_diario


# =========================
# GRÁFICOS Y EXPORTACIÓN
# =========================
def exportar_resultados_excel(detalle_actual, serie_total, detalle_diario) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        detalle_actual.to_excel(writer, sheet_name="detalle_actual", index=False)
        if not serie_total.empty: serie_total.to_excel(writer, sheet_name="serie_total", index=False)
        if not detalle_diario.empty: detalle_diario.to_excel(writer, sheet_name="detalle_diario", index=False)
    output.seek(0)
    return output.getvalue()

def fig_trm(serie_total):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(serie_total["fecha"], serie_total["trm"], linewidth=2, color="#4A90E2")
    ax.set_title("Evolución diaria de la TRM", fontsize=14)
    ax.set_ylabel("TRM (COP por USD)")
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos_decimales))
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    fig.tight_layout()
    return fig

def fig_saldo_vivo(serie_total):
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(serie_total["fecha"], serie_total["saldo_total_usd"], linewidth=2, color="#50E3C2")
    ax.set_title("Saldo vivo total en USD", fontsize=14)
    ax.set_ylabel("USD")
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
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
    
    plt.title("TRM y diferencia en cambio total", fontsize=14)
    lineas = linea1 + linea2
    etiquetas = [l.get_label() for l in lineas]
    ax1.legend(lineas, etiquetas, loc="upper left")
    
    texto_resumen = f"Diferencia total al día: ${diferencia_total_actual:,.2f}"
    fig.text(0.5, 0.02, texto_resumen, ha="center", va="center", fontsize=11, bbox=dict(boxstyle="round,pad=0.5", facecolor="white", edgecolor="black"))
    fig.tight_layout(rect=[0, 0.06, 1, 1])
    return fig

def fig_pnl_dia(serie_total):
    fig, ax = plt.subplots(figsize=(15, 6))
    ax.bar(serie_total["fecha"], serie_total["dif_dia_base"].fillna(0), width=1.0, color="#E15554")
    ax.axhline(0, color="black", linewidth=1)
    ax.set_title("Diferencia del día sobre saldo vivo", fontsize=14)
    ax.set_ylabel("COP")
    ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
    ax.grid(True, axis="y", alpha=0.3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
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
    ax2.bar(serie_factura["fecha"], serie_factura["dif_dia_base"].fillna(0), alpha=0.20, label="Dif. del día", color="gray")
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

    plt.title(f"Detalle de diferencia en cambio - Factura {factura}", fontsize=14)
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
# APP 1: DIFERENCIA EN CAMBIO
# =========================
def app_diferencia_cambio(facturas_file, monetizaciones_file):
    st.title("Diferencia en cambio - cartera en USD")
    spread = st.sidebar.number_input("Spread bancario (escenarios)", min_value=0.0, max_value=0.20, value=SPREAD_POR_DEFECTO, step=0.005, format="%.3f")

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
            trm_actual = float(df_trm["trm"].iloc[-1])
            trm_ayer = float(df_trm["trm"].iloc[-2]) if len(df_trm) > 1 else trm_actual

            detalle_actual = calcular_resumen_actual(df_facturas, df_monetizaciones, trm_actual, trm_ayer, spread)
            serie_total, detalle_diario = construir_serie_total(df_facturas, df_monetizaciones, df_trm)

            df_sin_factura = detalle_actual[detalle_actual["fecha_factura"].isna()]
            df_facturadas = detalle_actual[detalle_actual["fecha_factura"].notna()]

            st.markdown("---")
            if not df_sin_factura.empty:
                st.markdown("### 🚧 Proyectos / Anticipos sin Facturar")
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
                
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("TRM actual", f"${trm_actual:,.2f}")
                c2.metric("Saldo vivo total USD", f"${saldo_total_actual_usd:,.2f}")
                c3.metric("Dif. realizada total", f"${dif_realizada_total:,.2f}")
                c4.metric("Dif. no realizada total", f"${dif_no_realizada_total:,.2f}")

                tab1, tab2, tab3, tab4 = st.tabs(["Resumen", "Gráficas Generales", "Factura individual", "Descarga"])
                with tab1:
                    st.dataframe(df_facturadas, use_container_width=True)
                with tab2:
                    if not serie_total.empty:
                        st.pyplot(fig_trm(serie_total), clear_figure=True)
                        st.pyplot(fig_trm_y_diferencia_total(serie_total, dif_total_actual), clear_figure=True)
                with tab3:
                    factura_sel = st.selectbox("Selecciona una factura", options=sorted(df_facturadas["factura"].astype(str).unique().tolist()))
                    if factura_sel:
                        fila_factura = df_facturadas[df_facturadas["factura"] == factura_sel].iloc[0]
                        serie_factura = construir_serie_factura(fila_factura, df_monetizaciones, df_trm)
                        if not serie_factura.empty:
                            ultimo = serie_factura.iloc[-1]
                            st.pyplot(fig_factura_individual(serie_factura, factura_sel, float(ultimo["dif_total"]), float(ultimo["dif_dia_base"]), df_monetizaciones, fila_factura), clear_figure=True)
                with tab4:
                    excel_bytes = exportar_resultados_excel(detalle_actual, serie_total, detalle_diario)
                    st.download_button("Descargar Excel", data=excel_bytes, file_name="diferencia_cambio.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.info("No hay facturas con fecha registrada.")
        except Exception as e:
            st.error(f"Error procesando: {e}")
    else:
        st.info("👈 Sube Cuentas por Cobrar (CxC USD) en la barra lateral.")


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
        df_temp = df_temp.rename(columns={"GENERACION": "GENERACIO", "FECHA_GENERACION": "GENERACIO"})
        if not {"PROVEEDOR", "VALOR", "FACTURA", "GENERACIO", "VENCIMIENTO"}.issubset(df_temp.columns): continue
        df_temp = df_temp.dropna(subset=["GENERACIO"])
        df_temp = df_temp[df_temp["GENERACIO"].astype(str).str.strip() != ""]
        df_temp = df_temp.dropna(subset=["PROVEEDOR"])
        df_temp["PROVEEDOR"] = df_temp["PROVEEDOR"].astype(str).str.strip()
        df_temp["GENERACIO"] = pd.to_datetime(df_temp["GENERACIO"], errors="coerce", dayfirst=True)
        df_temp["VENCIMIENTO"] = pd.to_datetime(df_temp["VENCIMIENTO"], errors="coerce", dayfirst=True)
        df_temp["VALOR"] = pd.to_numeric(df_temp["VALOR"], errors="coerce")
        df_temp = df_temp.dropna(subset=["GENERACIO", "VENCIMIENTO", "VALOR"])
        dfs.append(df_temp)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def app_facturas_compras(compras_files):
    st.title("📊 Dashboard de Facturas de Compras")
    if not compras_files:
        st.info("👈 Sube Cuentas por Pagar (CxP COP) en la barra lateral.")
        return

    df_all = procesar_compras_dataframe(compras_files)
    if df_all.empty:
        st.error("❌ Ninguno de los archivos subidos contenía datos válidos.")
        return

    hoy = pd.Timestamp.today().normalize()
    df_all["MES_GENERACION"] = df_all["GENERACIO"].dt.strftime('%Y-%m')
    df_all["VENCIDA"] = df_all["VENCIMIENTO"] < hoy
    df_all["DIAS_VENCIDA"] = (hoy - df_all["VENCIMIENTO"]).dt.days.clip(lower=0)
    df_all["DIAS_PARA_VENCER"] = (df_all["VENCIMIENTO"] - hoy).dt.days
    df_all["PROXIMO_A_VENCER"] = (df_all["DIAS_PARA_VENCER"] >= 1) & (df_all["DIAS_PARA_VENCER"] <= 7)

    def clasificar_riesgo(row):
        if row["VENCIDA"]: return "🔴 Vencida Crítica (>30d)" if row["DIAS_VENCIDA"] > 30 else "🟠 Vencida Reciente (1-30d)"
        else: return "🟡 Próximo a Vencer (1-7d)" if row["PROXIMO_A_VENCER"] else "🟢 Al día (>7d)"
    df_all["RIESGO"] = df_all.apply(clasificar_riesgo, axis=1)

    tab1, tab2 = st.tabs(["📊 Análisis y Dashboard", "💸 Planeador de Pagos"])
    with tab1:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("💰 Saldo Total", f"${df_all['VALOR'].sum():,.0f}")
        c2.metric("🟢 Al Día", f"${df_all[df_all['RIESGO'] == '🟢 Al día (>7d)']['VALOR'].sum():,.0f}")
        c3.metric("🟡 Próx. Vencer", f"${df_all[df_all['RIESGO'] == '🟡 Próximo a Vencer (1-7d)']['VALOR'].sum():,.0f}")
        c4.metric("🔴 Vencido", f"${df_all[df_all['VENCIDA']]['VALOR'].sum():,.0f}")
        st.dataframe(df_all[["PROVEEDOR", "FACTURA", "VALOR", "VENCIMIENTO", "RIESGO"]], use_container_width=True)

    with tab2:
        st.subheader("Generador de Archivo de Pagos")
        df_pagos = df_all.copy()
        df_pagos.insert(0, "PAGAR", False)
        edited_df = st.data_editor(
            df_pagos[["PAGAR", "PROVEEDOR", "FACTURA", "VALOR", "VENCIMIENTO", "RIESGO"]],
            column_config={"PAGAR": st.column_config.CheckboxColumn("Seleccionar", default=False), "VALOR": st.column_config.NumberColumn(format="$%d")},
            disabled=["PROVEEDOR", "FACTURA", "VALOR", "VENCIMIENTO", "RIESGO"],
            hide_index=True, use_container_width=True
        )
        facturas_sel = edited_df[edited_df["PAGAR"]]
        if not facturas_sel.empty:
            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                df_all[df_all["FACTURA"].isin(facturas_sel["FACTURA"])].drop(columns=['MES_GENERACION', 'VENCIDA', 'DIAS_VENCIDA', 'DIAS_PARA_VENCER', 'PROXIMO_A_VENCER', 'RIESGO'], errors='ignore').to_excel(writer, index=False)
            st.download_button("⬇️ Descargar Archivo de Pagos", data=output.getvalue(), file_name="pagos_realizados.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


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
        with open("flujo_caja_manual.json", "r") as f: return json.load(f)
    return {"saldo_inicial": 0, "S1": {}, "S2": {}, "S3": {}, "S4": {}}

def guardar_datos_manuales(data):
    with open("flujo_caja_manual.json", "w") as f: json.dump(data, f)

def app_flujo_caja(f_usd, m_usd, f_compras, f_pagos):
    st.title("💸 Flujo de Caja a 4 Semanas")
    hoy_dt = pd.Timestamp.today()
    es_dia_edicion = hoy_dt.weekday() in [0, 1]

    col1, col2 = st.columns(2)
    with col1: st.info(f"📅 Hoy es: **{hoy_dt.strftime('%A, %d de %B')}**")
    with col2: forzar_edicion = st.checkbox("⚙️ Forzar Edición (Modo Admin)")

    puede_editar = es_dia_edicion or forzar_edicion
    if not puede_editar: st.warning("🔒 **Modo Lectura Activo:** Edición habilitada Lunes y Martes.")

    if not (f_usd and f_compras):
        st.info("👈 Sube los archivos de CxC (USD) y CxP (COP) en la barra lateral.")
        return

    # Ingresos USD
    df_usd = cargar_facturas(f_usd)
    df_mon = cargar_monetizaciones(m_usd)
    fechas_validas = df_usd["fecha_factura"].dropna()
    df_trm = descargar_trm_historica(fechas_validas.min().normalize() if not fechas_validas.empty else hoy_dt.normalize(), hoy_dt.normalize())
    df_usd = asignar_trm_factura(df_usd, df_trm)
    trm_hoy = float(df_trm["trm"].iloc[-1])
    resumen_usd = calcular_resumen_actual(df_usd, df_mon, trm_hoy, trm_hoy, 0.0)
    ingreso_semanal_usd_cop = (resumen_usd[resumen_usd["fecha_factura"].notna()]['saldo_vivo_actual_usd'].sum() * trm_hoy) * 0.20 

    # Compras COP y Pagos
    df_compras = procesar_compras_dataframe(f_compras)
    if f_pagos and not df_compras.empty:
        df_pagos_hechos = procesar_compras_dataframe(f_pagos)
        if not df_pagos_hechos.empty:
            df_compras = df_compras[~df_compras["FACTURA"].astype(str).isin(df_pagos_hechos["FACTURA"].astype(str).tolist())]

    semanas = obtener_semanas_fc()
    ap_semanas = [0, 0, 0, 0]
    if not df_compras.empty:
        for i in range(4):
            if i == 0: ap_semanas[i] = df_compras[df_compras['VENCIMIENTO'] <= semanas[i]['end']]['VALOR'].sum()
            else: ap_semanas[i] = df_compras[(df_compras['VENCIMIENTO'] >= semanas[i]['start']) & (df_compras['VENCIMIENTO'] <= semanas[i]['end'])]['VALOR'].sum()

    data_manual = cargar_datos_manuales()
    with st.form("form_flujo"):
        saldo_bancos = st.number_input("Saldo Inicial en Bancos (COP)", value=float(data_manual.get("saldo_inicial", 0)), disabled=not puede_editar)
        cols = st.columns(4)
        nuevos_datos = {}
        for i, col in enumerate(cols):
            clave_s = f"S{i+1}"
            with col:
                st.write(f"**{semanas[i]['label']}**")
                ing_ord = st.number_input("Ing. Ordinarios", value=float(data_manual[clave_s].get("ordinarios", 0)), key=f"ord_{i}", disabled=not puede_editar)
                ing_ex = st.number_input("Ingresos Extra", value=float(data_manual[clave_s].get("extra", 0)), key=f"ex_{i}", disabled=not puede_editar)
                f_serv = st.number_input("Servicios Públicos", value=float(data_manual[clave_s].get("f_serv", 0)), key=f"fs_{i}", disabled=not puede_editar)
                f_arr = st.number_input("Arriendo", value=float(data_manual[clave_s].get("f_arr", 0)), key=f"fa_{i}", disabled=not puede_editar)
                f_otr = st.number_input("Otros Fijos", value=float(data_manual[clave_s].get("f_otr", 0)), key=f"fo_{i}", disabled=not puede_editar)
                n_sue = st.number_input("Sueldos", value=float(data_manual[clave_s].get("n_sue", 0)), key=f"ns_{i}", disabled=not puede_editar)
                n_ss = st.number_input("Seg. Social", value=float(data_manual[clave_s].get("n_ss", 0)), key=f"nss_{i}", disabled=not puede_editar)
                nuevos_datos[clave_s] = {"ordinarios": ing_ord, "extra": ing_ex, "f_serv": f_serv, "f_arr": f_arr, "f_otr": f_otr, "n_sue": n_sue, "n_ss": n_ss, "n_otr": 0}

        if st.form_submit_button("💾 Guardar Proyección", disabled=not puede_editar):
            data_manual["saldo_inicial"] = saldo_bancos
            data_manual.update(nuevos_datos)
            guardar_datos_manuales(data_manual)
            st.success("¡Datos guardados!")

    st.subheader("📊 Resultado Flujo de Caja")
    tabla_fc = []
    saldo_actual = data_manual.get("saldo_inicial", 0)
    d_cre, d_lea = st.session_state.datos_credito, st.session_state.datos_leaseback

    for i in range(4):
        clave_s = f"S{i+1}"
        ing_ord_val, ing_ex_val = data_manual[clave_s].get("ordinarios", 0), data_manual[clave_s].get("extra", 0)
        desembolsos_fin = (d_cre["desembolso"] if d_cre["activo"] else 0) + (d_lea["desembolso"] if d_lea["activo"] else 0) if i == 0 else 0
        gf_val = data_manual[clave_s].get("f_serv", 0) + data_manual[clave_s].get("f_arr", 0) + data_manual[clave_s].get("f_otr", 0)
        nom_val = data_manual[clave_s].get("n_sue", 0) + data_manual[clave_s].get("n_ss", 0)
        cuotas_fin = (d_cre["cuota"] if d_cre["activo"] else 0) + (d_lea["cuota"] if d_lea["activo"] else 0) if i == 3 else 0

        ingresos_totales = ingreso_semanal_usd_cop + ing_ord_val + ing_ex_val + desembolsos_fin
        egresos_totales = ap_semanas[i] + gf_val + nom_val + cuotas_fin
        flujo_neto = ingresos_totales - egresos_totales
        saldo_final = saldo_actual + flujo_neto
        
        tabla_fc.append({
            "Semana": semanas[i]['label'], "Saldo Inicial": saldo_actual,
            "+ Cartera USD": ingreso_semanal_usd_cop, "+ Ingresos Op.": ing_ord_val + ing_ex_val + desembolsos_fin,
            "- Proveedores": ap_semanas[i], "- Gastos Op.": gf_val + nom_val + cuotas_fin,
            "FLUJO NETO": flujo_neto, "SALDO FINAL": saldo_final
        })
        saldo_actual = saldo_final

    df_fc = pd.DataFrame(tabla_fc)
    st.dataframe(df_fc.style.format({col: "${:,.0f}" for col in df_fc.columns if col != "Semana"}).map(lambda x: 'color: red' if x < 0 else 'color: green', subset=['FLUJO NETO', 'SALDO FINAL']), use_container_width=True)

    fig_fc, ax_fc = plt.subplots(figsize=(12, 5))
    x_labels, netos, saldos = [row["Semana"] for row in tabla_fc], [row["FLUJO NETO"] for row in tabla_fc], [row["SALDO FINAL"] for row in tabla_fc]
    ax_fc.bar(x_labels, netos, color=["#50E3C2" if val >= 0 else "#E15554" for val in netos], alpha=0.8, label="Flujo Neto")
    ax_fc.axhline(0, color='black', linewidth=1.2)
    ax_saldo = ax_fc.twinx()
    ax_saldo.plot(x_labels, saldos, color="#4A90E2", marker="o", linewidth=3, label="Saldo Final")
    l1, lab1 = ax_fc.get_legend_handles_labels()
    l2, lab2 = ax_saldo.get_legend_handles_labels()
    ax_fc.legend(l1 + l2, lab1 + lab2, loc="upper left")
    st.pyplot(fig_fc, clear_figure=True)


# =========================
# APP 4: ANÁLISIS DE ENDEUDAMIENTO Y CAPEX
# =========================
def app_endeudamiento_capex():
    st.title("🏦 Análisis de Endeudamiento y CAPEX")
    ibr_ea = st.sidebar.number_input("IBR Efectiva Anual (%)", value=12.5) / 100

    t1, t2 = st.tabs(["💰 Crédito Capital de Trabajo", "🏗️ Leaseback de Maquinaria"])
    with t1:
        st.subheader("Simulación Crédito Corto Plazo")
        colA, colB = st.columns(2)
        with colA:
            monto = st.number_input("Monto del Crédito (COP)", value=0, step=10000000)
            plazo = st.slider("Plazo (Meses)", 1, 6, 3)
        with colB:
            tipo_tasa = st.radio("Tipo de Tasa", ["Fija", "Variable (IBR + Spread)"])
            if tipo_tasa == "Fija": tasa_final_ea = st.number_input("Tasa Fija E.A. (%)", value=18.0) / 100
            else: tasa_final_ea = ibr_ea + (st.number_input("Spread E.A. (%)", value=5.0) / 100)

        cuota = calcular_cuota_mensual(monto, tasa_final_ea, plazo)
        st.info(f"**Resultado:** Desembolso de **${monto:,.0f}** con cuota mensual de **${cuota:,.0f}**")
        
        if st.button("🚀 Aplicar al Flujo de Caja", key="btn_cre"):
            st.session_state.datos_credito = {"desembolso": monto, "cuota": cuota, "activo": True}
            st.success("Impacto cargado al Flujo de Caja (S1 y S4).")

    with t2:
        st.subheader("Estrategia Sale & Leaseback")
        df_maquinas = pd.DataFrame([{"Máquina": "Inyectora 1", "Valor": 50000000}, {"Máquina": "Molde A", "Valor": 15000000}])
        edited_m = st.data_editor(df_maquinas, num_rows="dynamic", use_container_width=True)
        valor_total_capex = edited_m["Valor"].sum()
        
        colC, colD = st.columns(2)
        with colC:
            plazo_l = st.slider("Plazo Leaseback (Años)", 5, 10, 7)
            tasa_l_ea = ibr_ea + (st.number_input("Spread Leaseback E.A. (%)", value=4.0) / 100)
        with colD:
            st.metric("Total Liquidez Liberada", f"${valor_total_capex:,.0f}")
        cuota_l = calcular_cuota_mensual(valor_total_capex, tasa_l_ea, plazo_l * 12)
        
        if st.button("🏗️ Aplicar Leaseback al Flujo", key="btn_lea"):
            st.session_state.datos_leaseback = {"desembolso": valor_total_capex, "cuota": cuota_l, "activo": True}
            st.success("Leaseback integrado.")

# =========================
# APP 5: SIMULADOR ESTRATÉGICO CCC
# =========================
def app_simulador_ccc():
    st.title("🔄 Simulador de Ciclo de Conversión de Efectivo (CCC)")
    col1, col2, col3, col4 = st.columns(4)
    with col1: ventas_anuales = st.number_input("Ventas Anuales Estimadas (COP)", value=1000000000, step=100000000)
    with col2: dso_actual = st.number_input("Días de Cobro Actuales (DSO)", value=60)
    with col3: dio_actual = st.number_input("Días de Inventario (DIO)", value=45)
    with col4: dpo_actual = st.number_input("Días de Pago Actuales (DPO)", value=30)

    ccc_actual = dio_actual + dso_actual - dpo_actual
    ventas_diarias = ventas_anuales / 365
    st.info(f"**Ciclo de Caja Actual:** {ccc_actual} días.")

    st.markdown("### Simulación de Estrategias")
    scol1, scol2 = st.columns(2)
    with scol1: dso_simulado = st.slider("Nuevos Días de Cobro (Factoring)", 0, int(dso_actual), int(dso_actual))
    with scol2: dpo_simulado = st.slider("Nuevos Días de Pago (Confirming)", int(dpo_actual), 180, int(dpo_actual))

    ccc_simulado = dio_actual + dso_simulado - dpo_simulado
    dias_ganados = ccc_actual - ccc_simulado
    caja_liberada = dias_ganados * ventas_diarias

    r1, r2, r3 = st.columns(3)
    r1.metric("Nuevo Ciclo de Caja", f"{ccc_simulado} días", f"{-dias_ganados} días de mejora", delta_color="inverse")
    r2.metric("Venta Promedio Diaria", f"${ventas_diarias:,.0f}")
    r3.metric("💰 Capital de Trabajo Liberado", f"${caja_liberada:,.0f}")

# =========================
# APP 6: RESUMEN EJECUTIVO (PDF)
# =========================
def generar_pdf_gerencial(kpis, df_deuda, fig_liquidez) -> bytes:
    if FPDF is None: return None
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", 'B', 16)
    pdf.cell(200, 10, txt="Reporte Gerencial - Tesoreria y Liquidez", ln=1, align='C')
    pdf.ln(10)
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(200, 10, txt="1. Indicadores de Salud Financiera Inmediata", ln=1)
    pdf.set_font("Arial", '', 11)
    for k, v in kpis.items():
        pdf.cell(200, 8, txt=f"- {k}: {v}", ln=1)
    pdf.ln(10)
    if not df_deuda.empty:
        pdf.set_font("Arial", 'B', 12)
        pdf.cell(200, 10, txt="2. Resumen de Deuda y Financiacion (Corto Plazo)", ln=1)
        pdf.set_font("Arial", '', 11)
        for _, row in df_deuda.iterrows():
            pdf.cell(200, 8, txt=f"- {row['Tipo']}: Desembolso ${row['Desembolso']:,.0f} | Cuota: ${row['Cuota Mensual']:,.0f}", ln=1)
        pdf.ln(10)
    pdf.set_font("Arial", 'B', 12)
    pdf.cell(200, 10, txt="3. Composicion de Liquidez vs Deuda", ln=1)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmpfile:
        fig_liquidez.savefig(tmpfile.name, format="png", bbox_inches="tight")
        pdf.image(tmpfile.name, x=30, y=pdf.get_y(), w=150)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as pdf_file:
        pdf.output(pdf_file.name)
        with open(pdf_file.name, "rb") as f:
            pdf_bytes = f.read()
    return pdf_bytes

def app_resumen_ejecutivo(f_usd, m_usd, f_compras):
    st.title("📈 Resumen Ejecutivo y Reporte Gerencial")
    if not (f_usd and f_compras):
        st.warning("⚠️ Sube los archivos de CxC (USD) y CxP (COP) en la barra lateral para ver tus KPIs.")
        return

    df_usd = cargar_facturas(f_usd)
    df_mon = cargar_monetizaciones(m_usd)
    df_trm = descargar_trm_historica(df_usd["fecha_factura"].dropna().min() if not df_usd["fecha_factura"].dropna().empty else pd.Timestamp.today(), pd.Timestamp.today())
    df_usd = asignar_trm_factura(df_usd, df_trm)
    trm_hoy = float(df_trm["trm"].iloc[-1])
    resumen_usd = calcular_resumen_actual(df_usd, df_mon, trm_hoy, trm_hoy, 0.0)
    activo_corriente_cxc = resumen_usd['saldo_vivo_actual_usd'].sum() * trm_hoy

    df_compras = procesar_compras_dataframe(f_compras)
    pasivo_corriente_cxp = df_compras['VALOR'].sum() if not df_compras.empty else 0.0
    pasivo_vencido = df_compras[df_compras['VENCIMIENTO'] < pd.Timestamp.today()]['VALOR'].sum() if not df_compras.empty else 0.0

    data_manual = cargar_datos_manuales()
    saldo_bancos = float(data_manual.get("saldo_inicial", 0))

    activo_total_liquido = activo_corriente_cxc + saldo_bancos
    capital_trabajo_neto = activo_total_liquido - pasivo_corriente_cxp
    razon_liquidez = activo_total_liquido / pasivo_corriente_cxp if pasivo_corriente_cxp > 0 else 0
    porcentaje_vencido = (pasivo_vencido / pasivo_corriente_cxp) if pasivo_corriente_cxp > 0 else 0

    st.subheader("📊 Salud Financiera Inmediata")
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Capital de Trabajo Neto", f"${capital_trabajo_neto:,.0f}")
    k2.metric("Prueba Ácida", f"{razon_liquidez:,.2f}x")
    k3.metric("Total Activo Líquido", f"${activo_total_liquido:,.0f}")
    k4.metric("Cartera Vencida (CxP)", f"{porcentaje_vencido:.1%}")

    fig, ax = plt.subplots(figsize=(6, 4))
    ax.bar(["Activo Líquido", "Pasivo Corriente"], [activo_total_liquido, pasivo_corriente_cxp], color=["#50E3C2", "#E15554"])
    ax.set_title("Cobertura de Corto Plazo")
    ax.yaxis.set_major_formatter(FuncFormatter(formato_pesos))
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)

    st.markdown("---")
    st.subheader("📥 Exportar Reporte en PDF")
    
    if FPDF is None:
        st.error("⚠️ Instala la librería ejecutando: `pip install fpdf` en tu terminal, y reinicia streamlit (`Ctrl+C` y luego `streamlit run app.py`).")
    else:
        kpis_dict = {
            "Capital de Trabajo Neto": formato_pesos(capital_trabajo_neto),
            "Razón de Liquidez (Prueba Ácida)": f"{razon_liquidez:.2f}x",
            "Total Activo Líquido (Bancos + CxC)": formato_pesos(activo_total_liquido),
            "Total Pasivo Corto Plazo (CxP)": formato_pesos(pasivo_corriente_cxp),
            "Porcentaje de Deuda Vencida": f"{porcentaje_vencido:.1%}"
        }
        deuda_activa = []
        if st.session_state.datos_credito["activo"]:
            deuda_activa.append({"Tipo": "Crédito Cap. Trabajo", "Desembolso": st.session_state.datos_credito["desembolso"], "Cuota Mensual": st.session_state.datos_credito["cuota"]})
        if st.session_state.datos_leaseback["activo"]:
            deuda_activa.append({"Tipo": "Leaseback Operativo", "Desembolso": st.session_state.datos_leaseback["desembolso"], "Cuota Mensual": st.session_state.datos_leaseback["cuota"]})
        
        pdf_bytes = generar_pdf_gerencial(kpis_dict, pd.DataFrame(deuda_activa), fig)
        st.download_button("📄 Descargar Reporte Gerencial (PDF)", data=pdf_bytes, file_name="Reporte_Gerencial_Tesoreria.pdf", mime="application/pdf")

# =========================
# APP 7: LECTOR DE ESTADOS FINANCIEROS (NUEVO)
# =========================
def limpiar_y_extraer_ultimo_numero(val):
    if pd.isna(val): return None
    s = str(val).strip()
    if not any(c.isdigit() for c in s): return None
    
    # Separar por espacios o símbolos de peso por si el Excel unió varias columnas en una
    partes = s.replace("$", " ").replace("\n", " ").split()
    for parte in reversed(partes):
        if any(c.isdigit() for c in parte):
            # Limpiar formato colombiano: 8.029.923.801,00 -> 8029923801.00
            p = parte.replace(".", "")
            if "," in p: p = p.replace(",", ".")
            p = ''.join(c for c in p if c.isdigit() or c == '.')
            try: return float(p)
            except ValueError: continue
    return None

def app_analisis_financiero():
    st.title("📑 Análisis de Estados Financieros (Lector Automático)")
    st.markdown("Sube tu archivo de **Estado de Situación Financiera Completo** (Excel o CSVs) para extraer KPIs contables bajo norma NIIF/IFRS.")
    
    eeff_files = st.file_uploader("Subir Archivos Contables (Excel/CSV)", type=["xlsx", "xls", "csv"], accept_multiple_files=True)
    
    if eeff_files:
        try:
            keywords = {
                "Activo Corriente": ["ACTIVO CORRIENTE", "ACTIVOS CORRIENTES"],
                "Pasivo Corriente": ["PASIVO CORRIENTE", "TOTAL PASIVO CORRIENTE"],
                "Activo Total": ["TOTAL ACTIVO", "TOTAL ACTIVOS"],
                "Pasivo Total": ["TOTAL PASIVO", "TOTAL PASIVOS"],
                "Patrimonio": ["TOTAL PATRIMONIO", "PATRIMONIO TOTAL"],
                "Ingresos": ["INGRESOS DE ACTIVIDADES ORDINARIAS", "INGRESOS OPERACIONALES", "VENTAS"],
                "Utilidad Bruta": ["GANANCIA BRUTA", "UTILIDAD BRUTA"],
                "Utilidad Operativa": ["GANANCIA OPERATIVA", "UTILIDAD OPERATIVA"],
                "Utilidad Neta": ["UTILIDAD DEL PERIODO", "GANANCIA NETA", "UTILIDAD NETA", "GANANCIA  (PÉRDIDA)"]
            }
            
            valores = {k: 0.0 for k in keywords}
            
            for eeff_file in eeff_files:
                if eeff_file.name.endswith('.csv'):
                    df = pd.read_csv(eeff_file, on_bad_lines='skip')
                    dfs = {"Hoja1": df}
                else:
                    dfs = pd.read_excel(eeff_file, sheet_name=None)
                
                for sheet_name, df in dfs.items():
                    for idx, row in df.iterrows():
                        row_str = " ".join([str(x).upper() for x in row.values if pd.notnull(x)])
                        for key, kw_list in keywords.items():
                            if valores[key] == 0.0:
                                for kw in kw_list:
                                    if kw in row_str:
                                        # Buscar el número de derecha a izquierda (para sacar el año más reciente)
                                        for val in reversed(row.values):
                                            numero = limpiar_y_extraer_ultimo_numero(val)
                                            if numero is not None:
                                                valores[key] = abs(numero)
                                                break
                                        break
            
            st.success("✅ Estados Financieros leídos exitosamente. Generando KPIs...")
            
            liq_corriente = valores["Activo Corriente"] / valores["Pasivo Corriente"] if valores["Pasivo Corriente"] > 0 else 0
            nivel_endeudamiento = valores["Pasivo Total"] / valores["Activo Total"] if valores["Activo Total"] > 0 else 0
            margen_neto = valores["Utilidad Neta"] / valores["Ingresos"] if valores["Ingresos"] > 0 else 0
            roe = valores["Utilidad Neta"] / valores["Patrimonio"] if valores["Patrimonio"] > 0 else 0
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown("### 💧 Liquidez")
                st.metric("Razón Corriente", f"{liq_corriente:.2f}x")
                st.metric("Activo Corriente", formato_pesos(valores["Activo Corriente"]))
                st.metric("Pasivo Corriente", formato_pesos(valores["Pasivo Corriente"]))
            
            with col2:
                st.markdown("### ⚖️ Endeudamiento")
                st.metric("Nivel de Endeudamiento", f"{nivel_endeudamiento:.1%}")
                st.metric("Pasivo Total", formato_pesos(valores["Pasivo Total"]))
                st.metric("Patrimonio Total", formato_pesos(valores["Patrimonio"]))
                
            with col3:
                st.markdown("### 📈 Rentabilidad")
                st.metric("Margen Neto", f"{margen_neto:.1%}")
                st.metric("ROE (Retorno Patrimonio)", f"{roe:.1%}")
                st.metric("Ingresos Anuales", formato_pesos(valores["Ingresos"]))
                
            st.markdown("---")
            with st.expander("Ver Datos Base Extraídos del Archivo"):
                st.json({k: formato_pesos(v) for k, v in valores.items()})

        except Exception as e:
            st.error(f"Error procesando los estados financieros: {e}")

# =========================
# MENÚ PRINCIPAL Y LOGIN
# =========================
def main():
    if not st.session_state.logged_in:
        st.title("🔒 Acceso Seguro - Tesorería")
        st.markdown("Ingresa tus credenciales corporativas para acceder al ecosistema financiero.")
        with st.form("login_form"):
            usuario = st.text_input("Usuario")
            password = st.text_input("Contraseña", type="password")
            submit = st.form_submit_button("Ingresar")
            if submit:
                if usuario == "admin" and password == "admin":
                    st.session_state.logged_in = True
                    st.rerun()
                else:
                    st.error("❌ Credenciales incorrectas.")
        return

    st.sidebar.title("Navegación Pro")
    st.sidebar.write("👤 Conectado como: **Administrador**")
    if st.sidebar.button("Cerrar Sesión"):
        st.session_state.logged_in = False
        st.rerun()

    st.sidebar.markdown("---")
    app_sel = st.sidebar.radio("Menú Principal:", (
        "Resumen Ejecutivo (PDF)", 
        "Diferencia en cambio", 
        "Revisar facturas de compras", 
        "Flujo de Caja a 4 Semanas", 
        "Endeudamiento y CAPEX", 
        "Simulador Estratégico CCC",
        "Lector de Estados Financieros (NUEVO)"
    ))
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("📂 Base de Datos Global")
    f_usd = st.sidebar.file_uploader("1. CxC (USD)", type=["xlsx"], key="g_cxc")
    m_usd = st.sidebar.file_uploader("2. Monetizaciones", type=["xlsx"], key="g_mon")
    f_compras = st.sidebar.file_uploader("3. CxP (COP) [Múltiples]", type=["xlsx", "xlsm"], accept_multiple_files=True, key="g_cxp")
    f_pagos = st.sidebar.file_uploader("4. Pagos Realizados", type=["xlsx", "xlsm"], accept_multiple_files=True, key="g_pag")
    
    if app_sel == "Resumen Ejecutivo (PDF)": app_resumen_ejecutivo(f_usd, m_usd, f_compras)
    elif app_sel == "Diferencia en cambio": app_diferencia_cambio(f_usd, m_usd)
    elif app_sel == "Revisar facturas de compras": app_facturas_compras(f_compras)
    elif app_sel == "Flujo de Caja a 4 Semanas": app_flujo_caja(f_usd, m_usd, f_compras, f_pagos)
    elif app_sel == "Endeudamiento y CAPEX": app_endeudamiento_capex()
    elif app_sel == "Simulador Estratégico CCC": app_simulador_ccc()
    elif app_sel == "Lector de Estados Financieros (NUEVO)": app_analisis_financiero()

if __name__ == "__main__":
    main()