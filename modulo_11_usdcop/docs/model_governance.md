# Gobierno del modelo

## Objetivo

Apoyar decisiones de caja y cobertura, no maximizar utilidad especulativa. La funcion de perdida debe reflejar el costo de una exposicion no cubierta, el costo del hedge y el riesgo de cola.

## Benchmarks obligatorios

1. Spot sin cambio.
2. Ancla teorica de carry.
3. Curva NDF mid ejecutable cuando exista fuente contratada.
4. Modelo directo regularizado.
5. Ensamble dinamico aprobado.

## Validacion

- Backtest walk-forward con vintages y fechas de publicacion.
- Evaluacion separada para 15, 30, 45 y 60 dias calendario.
- MAE, RMSE/MASE, acierto direccional, pinball loss, CRPS y cobertura de intervalos.
- PnL o costo de hedge bajo una politica predefinida; no optimizar la politica con el mismo periodo de prueba.
- Evaluacion por regimen y por ventanas de estres.

## Champion/challenger

El champion solo cambia cuando el challenger supera los benchmarks en varias ventanas, mantiene cobertura probabilistica y pasa revision humana. Cada artefacto debe guardar version de codigo, datos, hiperparametros, metricas y aprobacion.

## Politica de salida

- `BENCHMARK_ONLY_NOT_TRAINED`: solo spot y carry.
- `MODEL_ACTIVE_AUTOMATED_DAILY`: modelo activo para generar el pronostico diario.
- `APPROVED_FOR_DECISION_SUPPORT`: aprobado para soporte, siempre con supervision humana.
- `SUSPENDED_DATA_OR_DRIFT`: ocultar mediana del modelo y volver a benchmarks.

## Revisión operativa diaria

La selección y frescura de las fuentes se validan automáticamente en cada ejecución.
La interfaz permite que el usuario autenticado revise el snapshot publicado y apruebe
el pronóstico exacto dentro de su sesión. La aprobación queda invalidada cuando cambia
la fecha de generación, la versión del modelo o la fecha de corte. Esta revisión diaria
no reentrena ni cambia el modelo champion.
