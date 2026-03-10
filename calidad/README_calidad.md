# Calidad de Datos — Licitaciones Publicas de Espana

Pipeline de calidad que aplica **20 indicadores** de validez, consistencia y fiabilidad sobre el dataset nacional de [PLACSP](https://contrataciondelsectorpublico.gob.es/), cruzando con TED (Diario Oficial de la UE) y BORME (Registro Mercantil).

## Resultados

**8,693,891 contratos** evaluados | Score medio: **88.3** | Mediana: **89.5**

| Indicador | % Fallo | Evaluados | Dimension | Fuente |
|---|---|---|---|---|
| INT-VAL-01 | 22.6% | 8.7M | Validez | PPDS |
| INT-VAL-02 | 31.8% | 8.7M | Validez | PPDS |
| INT-VAL-03 | 0.4% | 8.7M | Validez | PPDS |
| INT-VAL-04 | 0.0% | 8.7M | Validez | PPDS |
| INT-VAL-05 | 0.0% | 8.7M | Validez | PPDS |
| INT-VAL-06 | 0.2% | 8.7M | Validez | PPDS |
| INT-VAL-07 | 39.2% | 8.7M | Validez | PPDS |
| INT-VAL-09 | 23.3% | 8.7M | Validez | PPDS |
| INT-VAL-10 | 0.0% | 8.7M | Validez | PPDS |
| INT-VAL-12 | 33.4% | 8.7M | Validez | Yo con asistencia de la IA |
| INT-VAL-14 | 1.3% | 8.7M | Validez | Jaime Gomez-Obregon |
| INT-CONS-01 | 1.1% | 8.6M | Consistencia | PPDS |
| INT-CONS-08 | 0.3% | 8.7M | Consistencia | PPDS |
| INT-CONS-18 | 51.3% | 5.1M | Consistencia | Yo con asistencia de la IA |
| INT-CONS-20 | 77.5% | 1.3M | Consistencia | PPDS |
| INT-FIA-01 | 0.6% | 8.7M | Fiabilidad | PPDS |
| INT-FIA-04 | 21.3% | 8.7M | Fiabilidad | PPDS |
| INT-FIA-08 | 0.4% | 8.7M | Fiabilidad | PPDS |
| INT-FIA-09 | 0.8% | 8.7M | Fiabilidad | PPDS |
| INT-FIA-11 | 0.0% | 8.7M | Fiabilidad | PPDS |

### Menores vs Regulares

| Indicador | Menores (3.3M) | Regulares (5.4M) | Diferencia |
|---|---|---|---|
| INT-VAL-01 (importe licitacion) | 53.1% | 4.1% | +49.0pp |
| INT-VAL-02 (importe adjudicacion) | 0.3% | 51.0% | -50.8pp |
| INT-VAL-07 (fecha adjudicacion) | 0.8% | 62.6% | -61.9pp |
| INT-VAL-09 (CPV) | 60.3% | 0.7% | +59.6pp |
| INT-VAL-12 (NIF adjudicatario) | 2.7% | 52.1% | -49.5pp |
| Score medio | 90.8 | 86.7 | +4.1 |

Los menores tienen mejor NIF y fecha de adjudicacion (campos obligatorios para publicar), pero peor CPV e importe de licitacion (campos opcionales que no se rellenan).

## Uso

```bash
pip install pandas pyarrow

# Solo indicadores base (17)
python calidad_licitaciones.py -i nacional/licitaciones_espana.parquet

# Completo con TED + BORME (20 indicadores)
python calidad_licitaciones.py -i nacional/licitaciones_espana.parquet \
  --ted ted/crossval_sara_v2.parquet \
  --borme borme_empresas.parquet

# Muestra rapida
python calidad_licitaciones.py -i nacional/licitaciones_espana.parquet \
  --ted ted/crossval_sara_v2.parquet \
  --borme borme_empresas.parquet \
  -s 200000
```

## Archivos

| Archivo | Descripcion | Tamano |
|---|---|---|
| `calidad_licitaciones.py` | Script del pipeline (429 lineas) | 15 KB |
| `calidad_licitaciones_resultado.parquet` | 8.7M contratos x 70 columnas (48 originales + 20 indicadores + score + es_menor) | 977 MB |

## Indicadores

### Validez (11)

| ID | Indicador | Regla |
|---|---|---|
| INT-VAL-01 | Importe de licitacion en formato valido | `importe_sin_iva` no nulo y numerico |
| INT-VAL-02 | Importe de adjudicacion en formato valido | `importe_adjudicacion` no nulo y numerico |
| INT-VAL-03 | Importe minimo plausible | Al menos un importe >= 1 EUR |
| INT-VAL-04 | Numero de licitadores es entero | `num_ofertas` sin decimales |
| INT-VAL-05 | Numero de licitadores no negativo | `num_ofertas` >= 0 |
| INT-VAL-06 | Fecha de publicacion valida | Fecha parseable en rango 1990-2030 |
| INT-VAL-07 | Fecha de adjudicacion valida | Fecha parseable en rango 1990-2030 |
| INT-VAL-09 | Codigo CPV valido | 8 digitos (con/sin guion + digito de control) |
| INT-VAL-10 | Codigo territorial valido | Patron NUTS Espana (ES + 0-3 caracteres) |
| INT-VAL-12 | NIF/CIF adjudicatario valido | Checksum completo: DNI, NIE y CIF. Detecta emails y texto libre |
| INT-VAL-14 | Procedimiento coherente con cuantia | Contrato menor: obras <= 40K EUR, servicios/suministros <= 15K EUR (LCSP art. 118) |

### Consistencia (4)

| ID | Indicador | Regla |
|---|---|---|
| INT-CONS-01 | Adjudicado con >=1 oferta | Si estado contiene "adjud/formaliz/resuel", exige `num_ofertas` >= 1 |
| INT-CONS-08 | Importe adjudicacion <= licitacion | Misma base IVA, tolerancia 5% |
| INT-CONS-18 | Adjudicatario existe en BORME | Nombre normalizado buscado en 3.3M empresas del Registro Mercantil. Solo evalua personas juridicas (CIF) |
| INT-CONS-20 | Contrato SARA publicado en TED | Usa `_ted_validated` del pipeline de cross-validation (5 estrategias de matching) |

### Fiabilidad (5)

| ID | Indicador | Regla |
|---|---|---|
| INT-FIA-01 | Num ofertas en rango razonable | Max. global 500 + percentil 99 por division CPV |
| INT-FIA-04 | Plazo presentacion ofertas razonable | Dias entre publicacion y fecha limite: 0-365 |
| INT-FIA-08 | PBL no outlier | Importe licitacion <= 50M EUR |
| INT-FIA-09 | PA plausible por segmento CPV | Dentro de P1-P99 por division CPV |
| INT-FIA-11 | Trazabilidad minima del expediente | Tiene `expediente` + (`url` o `id`) |

## Configuracion

Todos los umbrales son configurables en el diccionario `CONFIG` del script:

| Parametro | Valor | Indicador |
|---|---|---|
| `importe_minimo` | 1.0 EUR | INT-VAL-03 |
| `umbral_menor_obras` | 40,000 EUR | INT-VAL-14 |
| `umbral_menor_servicios` | 15,000 EUR | INT-VAL-14 |
| `tolerancia_adj_lic` | 5% | INT-CONS-08 |
| `max_ofertas_global` | 500 | INT-FIA-01 |
| `max_plazo_dias` | 365 | INT-FIA-04 |
| `pbl_outlier` | 50,000,000 EUR | INT-FIA-08 |

## Ejemplos

```python
import pandas as pd

df = pd.read_parquet('calidad/calidad_licitaciones_resultado.parquet')

# Score medio por organo contratante
df.groupby('organo_contratante')['score_calidad'].mean().nlargest(20)

# Contratos menores que superan umbral LCSP
df[df['INT-VAL-14'] == False][['expediente', 'organo_contratante', 'importe_adjudicacion', 'tipo_contrato']]

# Contratos SARA no publicados en TED
df[df['INT-CONS-20'] == False].groupby('organo_contratante').size().nlargest(10)

# Adjudicatarios con NIF invalido
df[df['INT-VAL-12'] == False]['nif_adjudicatario'].value_counts().head(20)

# Adjudicatarios no encontrados en BORME
df[df['INT-CONS-18'] == False][['adjudicatario', 'nif_adjudicatario', 'importe_adjudicacion']].head(20)

# Comparativa menores vs regulares
df.groupby('es_menor')['score_calidad'].describe()
```

## Metodologia

Los indicadores se basan en el marco de calidad de datos de contratacion publica desarrollado por PPDS, con contribuciones de:

- **PPDS** -- marco base de indicadores de validez, consistencia y fiabilidad
- **Jaime Gomez-Obregon** -- umbrales LCSP para contratos menores (INT-VAL-14)
- **Gerard Sanchez** -- financiacion PTRT (pendiente de implementacion)
- **OIRESCON** -- benchmarks de PBL outlier y trazabilidad CPM

El `score_calidad` (0-100) es el porcentaje de indicadores evaluables que pasa cada contrato.

## Notas

- **INT-CONS-18 (BORME)**: El 51.3% de "no encontrados" incluye autonomos, empresas extranjeras, UTEs y variantes de nombre que no matchean exacto. No todo "no encontrado" es sospechoso.
- **INT-CONS-20 (TED)**: El 77.5% de missing es una cota superior conservadora. El pipeline de cross-validation valida el 28.2% con 5 estrategias, pero el join por `expediente|nif_adjudicatario` pierde los matches por NIF organo (E3) y lotes agrupados (E4).
- **INT-VAL-01/02/07/09/12**: Los altos porcentajes de fallo reflejan principalmente campos nulos (no publicados), no errores de formato. Es una medida de completitud mas que de validez estricta.
