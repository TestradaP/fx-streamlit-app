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

- Backtest walk-forward con fechas de disponibilidad; los vintages inmutables se
  incorporan progresivamente desde la primera captura registrada.
- Evaluacion separada para 15, 30, 45 y 60 dias calendario.
- MAE, RMSE/MASE, acierto direccional, pinball loss, CRPS y cobertura de intervalos.
- PnL o costo de hedge bajo una politica predefinida; no optimizar la politica con el mismo periodo de prueba.
- Evaluacion por regimen y por ventanas de estres.

## Champion/challenger

El champion solo cambia cuando el challenger supera los benchmarks en varias ventanas, mantiene cobertura probabilistica y pasa revision humana. Cada artefacto debe guardar version de codigo, datos, hiperparametros, metricas y aprobacion.

La implementación evalúa ElasticNet, Ridge, Huber, gradient boosting, PCA-Ridge,
Extra Trees, boosting cuantílico, Ridge por régimen VIX y ensambles igualitario y
ponderado por error OOS histórico, stacking positivo regularizado, Ridge de historia
larga y Extra Trees de ventana reciente. Cada horizonte se decide por separado. La promoción automática exige
skill mínimo, dirección, mayoría de ventanas positivas y límite superior negativo
del intervalo block-bootstrap de la diferencia de error. Además debe mejorar en
una muestra final sellada que no interviene en la selección inicial. Si no se
cumplen todas las reglas, el registro selecciona `random_walk` como fallback. Los
p-valores se ajustan con Holm para controlar la búsqueda entre múltiples challengers;
los modelos ajustados usan pipelines completos dentro de CV temporal purgada.

## Politica de salida

- `BENCHMARK_ONLY_NOT_TRAINED`: solo spot y carry.
- `MODEL_ACTIVE_AUTOMATED_DAILY`: modelo activo para generar el pronostico diario.
- `APPROVED_FOR_DECISION_SUPPORT`: aprobado para soporte, siempre con supervision humana.
- `SUSPENDED_DATA_OR_DRIFT`: ocultar mediana del modelo y volver a benchmarks.

## Revisión operativa diaria

La selección y frescura de las fuentes se validan automáticamente en cada ejecución.
La interfaz permite que el usuario autenticado revise el snapshot publicado y confirme
la revisión de los datos dentro de su sesión. La confirmación queda invalidada cuando cambia
la fecha de generación, la versión del modelo o la fecha de corte. Esta revisión diaria
no reentrena, no cambia el modelo champion y no constituye aprobación estadística.

## Criterio académico

La etiqueta de uso académico confirmatorio permanece deshabilitada mientras falte
cualquiera de estos elementos: cobertura histórica point-in-time suficiente, superioridad consistente
frente a spot sin cambio y carry, estabilidad por régimen y cobertura fuera de muestra de
intervalos probabilísticos. Los valores que no cumplan estos criterios se publican únicamente
como resultados experimentales reproducibles.
