# Diccionario de datos recomendado

Cada observacion debe conservar:

- `series_id`: identificador interno estable.
- `observation_date`: periodo al que corresponde el dato.
- `value`: valor numerico.
- `release_timestamp`: momento en que el dato era conocible por el modelo.
- `release_timestamp_is_authoritative`: indica si la fuente entregó una fecha de
  publicación verificable o si se aplicó un rezago conservador.
- `release_timestamp_source`: ALFRED, calendario oficial o regla configurada.
- `initial_release_value`: valor de la primera publicación cuando la fuente
  conserva vintages históricos, usado en lugar del dato revisado en el backtest.
- `retrieved_at`: momento de descarga.
- `source`: BanRep, DANE, FRED o proveedor.
- `vintage_id`: version o revision, cuando exista.
- `quality_status`: vigente, rezagado, revisado, fallido.

Variables de alta frecuencia: spot, NDF, IBR/OIS, TES, SOFR, VIX, DXY/broad USD, Brent, curvas de EE. UU., CDS y opciones.

Variables de menor frecuencia: remesas, deuda externa y amortizaciones, balanza comercial, cuenta corriente, IED, reservas, intervencion, tenencia extranjera de TES, fiscal y eventos politicos. Estas variables nunca deben interpolarse como si hubieran sido observadas diariamente antes de su publicacion.
