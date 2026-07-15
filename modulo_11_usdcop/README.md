# Modulo 11 - Inteligencia cambiaria USD/COP

Scaffold listo para integrar en una aplicacion Streamlit existente. El modulo produce soporte diario para decisiones de caja y coberturas en horizontes de **15, 30, 45 y 60 dias calendario**.

## Principios

- No es una herramienta de especulacion ni una recomendacion de inversion.
- No presenta una cifra del modelo hasta que exista entrenamiento y validacion walk-forward.
- Mantiene separados: spot/TRM, ancla teorica de carry, curva forward ejecutable y pronostico probabilistico.
- Registra fecha de observacion, fecha de publicacion, fecha de recuperacion, fuente y version del modelo.
- La pagina de Streamlit visualiza; la actualizacion y el entrenamiento se ejecutan fuera del proceso web.

## Inicio rapido

```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install -e .
copy .env.example .env

python scripts/update_daily.py
python scripts/train_models.py
python scripts/run_forecast.py
streamlit run pages/11_USD_COP.py
```

En Linux/macOS use `source .venv/bin/activate` y `cp .env.example .env`.

Antes de ejecutar la actualización, configure `FRED_API_KEY` en `.env`. En
GitHub Actions, cree un secret de repositorio con el mismo nombre. Nunca incluya
la clave directamente en el código.

Para el dashboard principal, genere un hash de contraseña con
`python scripts/hash_password.py` desde la raíz del repositorio. Configure
`APP_USERNAME` y `APP_PASSWORD_HASH` como variables de entorno, o copie
`.streamlit/secrets.toml.example` a `.streamlit/secrets.toml` y complete `[auth]`.

## Integracion en una plataforma existente

```python
from usdcop.ui.module import render_module

render_module(project_root="/ruta/al/modulo_11_usdcop")
```

Tambien puede copiar `pages/11_USD_COP.py` al directorio `pages/` de su proyecto y ajustar `MODULE_ROOT`.

## Estado inicial incluido

El repositorio contiene un snapshot oficial al 15 de julio de 2026 y una tabla de **benchmarks**, no un pronostico validado. El campo `status` se mantiene como `BENCHMARK_ONLY_NOT_TRAINED` hasta que el pipeline encuentre datos historicos suficientes, entrene y apruebe el modelo.

## Fuentes

- Banco de la Republica / SUAMECA: TRM, IBR, tasa de politica, TES, reservas, cuenta corriente y otras series.
- DANE: balanza comercial y estadisticas de comercio exterior.
- FRED: SOFR, VIX, indice amplio del dolar, tasas del Tesoro y petroleo.
- Fuente de mercado opcional: NDF ejecutable, volatilidad implicita, risk reversals, CDS y order flow.

La API REST de SUAMECA usada por el conector es publica pero no esta documentada como contrato estable. En produccion se debe habilitar tambien un adaptador de respaldo para SDMX o descarga oficial por archivo.

## Automatizacion

El flujo recomendado es:

1. `update_daily.py`: descarga y valida datos.
2. `train_models.py`: reentrena segun calendario, no en cada carga de pagina.
3. `run_forecast.py`: calcula benchmarks, pronosticos e intervalos.
4. Streamlit lee artefactos versionados de una base persistente.

El workflow incluido publica `outputs/latest_forecasts.csv` y
`outputs/forecast_status.json` en la rama `main` después de una ejecución
correcta. Streamlit Community Cloud detecta ese commit y actualiza la aplicación.
Los Parquet y SQLite completos se conservan como artifacts de GitHub Actions para
evitar aumentar innecesariamente el historial Git.

Para un despliegue multiusuario use PostgreSQL u object storage. El almacenamiento local incluido (Parquet + SQLite) es apropiado para desarrollo y un MVP controlado.

## Pruebas

```bash
python -m unittest discover -s tests -v
```
