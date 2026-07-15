# Integracion con la plataforma Streamlit existente

## Opcion A: importar una funcion

Copie el directorio `src/usdcop` al repositorio principal o instale este paquete en modo editable. En el router de su aplicacion:

```python
from usdcop.ui.module import render_module

render_module(project_root=ROOT / "modules" / "modulo_11_usdcop")
```

## Opcion B: pagina multipage

Copie `pages/11_USD_COP.py` a la carpeta `pages/` de la aplicacion y cambie `MODULE_ROOT` por la ruta persistente donde vivan `config/`, `data/` y `outputs/`.

## Separacion de responsabilidades

- Streamlit: lectura, visualizacion, filtros y escenarios.
- ETL: proceso programado separado.
- Entrenamiento: semanal o ante cambio material; nunca en cada sesion web.
- Scoring: diario y, con feed de mercado, recalculo intradia sin reentrenamiento.
- Persistencia: PostgreSQL u object storage en produccion.

## Programacion sugerida para America/Bogota

Ejemplo de cron en servidor persistente:

```cron
15 6 * * 1-5  /opt/usdcop/.venv/bin/python /opt/usdcop/scripts/update_daily.py >> /var/log/usdcop.log 2>&1
30 6 * * 1-5  /opt/usdcop/.venv/bin/python /opt/usdcop/scripts/run_forecast.py >> /var/log/usdcop.log 2>&1
15 17 * * 1-5 /opt/usdcop/.venv/bin/python /opt/usdcop/scripts/update_daily.py >> /var/log/usdcop.log 2>&1
30 17 * * 1-5 /opt/usdcop/.venv/bin/python /opt/usdcop/scripts/run_forecast.py >> /var/log/usdcop.log 2>&1
0 7 * * 6    /opt/usdcop/.venv/bin/python /opt/usdcop/scripts/train_models.py >> /var/log/usdcop.log 2>&1
```

Los datos mensuales y trimestrales se incorporan por fecha efectiva de publicacion. La pagina debe alertar cuando una serie exceda su tolerancia de rezago.

## Tiempo real

TRM, remesas, deuda y cuenta corriente no son series de tiempo real. Para una capa intradia se requiere el mercado USD/COP en vivo, NDF y, de ser posible, opciones/CDS. El modulo acepta un CSV o adaptador contratado mediante `MarketDataAdapter`; no se incluye ninguna credencial.
