# Clase práctica 2 — Silver, Gold y orquestación
- **Nombre: Ricardo Matuk**
- **student_id: ricardo_72**
- **Escala: small**
## Propósito

Transformar las tablas Bronze de la práctica 1 en datos confiables y productos analíticos. El flujo se ejecuta como un Lakeflow Job, admite archivos nuevos y demuestra idempotencia mediante validaciones automáticas.

## Objetivos

- Aplicar contratos de tipos y reglas de calidad.
- Separar registros válidos de una cuarentena explicable.
- Resolver duplicados y correcciones con `MERGE`.
- Construir tablas Gold.
- Orquestar notebooks dependientes con Lakeflow Jobs.
- Probar una carga nueva sin cambiar el código del pipeline.
- Verificar reconciliación e idempotencia.

## Requisitos previos

La práctica 1 debe haber creado, en el mismo esquema:

```text
bronze_customers
bronze_products
bronze_transactions
bronze_events
```

Usá en todos los notebooks el mismo `student_id` y la misma escala de la práctica 1. Si trabajaste con `small`, no cambies a `test`: las claves foráneas se generan de acuerdo con la escala.

## Secuencia

1. Ejecutá `00_preflight.ipynb`.
2. Ejecutá manualmente `00_generate_new_batch.ipynb` con `batch_002`.
3. Creá el Job siguiendo `GUIA_CREAR_JOB.md`.
4. Ejecutá el Job con `expected_batch_id=batch_002`.
5. Revisá las cuatro tareas y las tablas resultantes.
6. Ejecutá nuevamente el mismo Job sin crear otro archivo.
7. Confirmá que la segunda validación informa métricas estables.
8. Volvé a ejecutar manualmente `00_generate_new_batch.ipynb`, esta vez con `batch_id=batch_003`.
9. Ejecutá el Job con `expected_batch_id=batch_003` y verificá que el nuevo lote aparezca en `gold_batch_summary`.
10. Ejecutá manualmente `05_visualizacion.ipynb` y resolvé las cuatro preguntas usando las tablas Gold.

### Qué hace `expected_batch_id`

El pipeline **no necesita** `expected_batch_id` para encontrar ni procesar archivos. La tarea de ingesta usa `COPY INTO` sobre el directorio de entrada, detecta automáticamente `batch_003` como un archivo nuevo y lo incorpora de manera incremental. Silver y Gold procesan después los datos resultantes sin requerir cambios en el código.

`expected_batch_id` se utiliza solamente en `04_validate_pipeline.ipynb`: expresa qué lote esperamos comprobar en esa ejecución. Al crear `batch_003`, cambiar el parámetro a `batch_003` permite validar que ese lote llegó a Bronze, fue aceptado o enviado a cuarentena, aparece en Gold y aplicó la corrección esperada.

Si se deposita `batch_003` pero se conserva `expected_batch_id=batch_002`, el ETL igualmente procesará el archivo nuevo. Sin embargo, la tarea `validate` puede fallar porque seguirá evaluando las expectativas de `batch_002` y comparando la corrida como si fuera una reejecución sin datos nuevos. El estado rojo representa entonces una expectativa de prueba incorrecta, no una incapacidad del pipeline para detectar el archivo.

## Tablas resultantes

```text
bronze_transactions_incremental
bronze_transactions_all       (vista)
silver_customers
silver_products
silver_transactions
silver_transactions_quarantine
gold_daily_sales
gold_customer_risk
gold_batch_summary
pipeline_run_audit
```

## Preguntas de análisis y comprensión

Respondé las siguientes preguntas después de completar las ejecuciones con `batch_002` y `batch_003`. En las preguntas de análisis incluí la consulta SQL o PySpark utilizada y el resultado relevante. En las preguntas sobre código, indicá el notebook y la sección que fundamentan tu respuesta.

### Análisis de las tablas

1. ¿Cuántas filas físicas recibió cada lote en `bronze_transactions_incremental`? Escribí una consulta que muestre el resultado por `source_batch_id`.

(spark.table(bronze_inc) 
    .groupBy("source_batch_id")
    .count() 
    .orderBy("source_batch_id")
    .show())

source_batch_id|count| batch_002| 204| | batch_003| 204|
Filas del batch_002 recibidas 204. Igual número del batch_003.

2. Para cada lote, ¿cuántas transacciones fueron aceptadas y cuántas quedaron en `silver_transactions_quarantine`? Reconciliá tus resultados con `gold_batch_summary`.

 batch_002: 200 transacciones aceptadas y 2 transacciones rechazadas .
batch_003: 201 transacciones aceptadas y 2 transacciones rechazadas .
initial: 49,948 transacciones aceptadas y 51 transacciones rechazadas .

Reconciliación: Los resultados coinciden exactamente unidad por unidad al contrastar los conteos físicos de las tablas de la capa Silver (silver_transactions y silver_transactions_quarantine) contra las métricas consolidadas de la tabla resumen de la capa Gold (gold_batch_summary), lo que demuestra que no existió pérdida ni duplicación de registros durante el proceso de ingesta y transformación.

3. ¿Qué motivos de rechazo aparecen en la cuarentena y cuántos registros tiene cada uno por lote? ¿Los rechazos observados coinciden con los casos introducidos por el generador?

Los motivos de rechazo que aparecen en la cuarentena (silver_transactions_quarantine) y sus cantidades por lote son:  
batch_002: 1 registro por UNKNOWN_CUSTOMER y 1 registro por INVALID_AMOUNT.
batch_003: 1 registro por INVALID_AMOUNT y 1 registro por UNKNOWN_CUSTOMER. 
initial: 51 registros exclusivamente por INVALID_AMOUNT.

Coincidencia con el generador: los rechazos observados coinciden exactamente con los casos anómalos introducidos por el generador de datos del curso. El pipeline capturó con éxito las transacciones con montos inválidos o clientes inexistentes 


4. Seguí la transacción `42` desde `bronze_transactions_all` hasta `silver_transactions`. ¿Cuántas versiones existen en Bronze y cuál quedó vigente en Silver? Mostrá las columnas que justifican la elección.

Versiones en Bronze: Existen 3 versiones de la transacción 42 en la capa Bronze, provenientes de los lotes initial, batch_002 y batch_003.
Versión vigente en Silver: En la capa Silver quedó vigente únicamente la versión correspondiente al lote batch_003.

Columnas que justifican la elección: La columna clave que justifica la elección es event_ts (marca de tiempo del evento). El pipeline aplica una estrategia de deduplicación basada en la versión más reciente (last-write-wins). Como la versión de batch_003 tiene el event_ts más nuevo (2026-03-11 00:00:00), el sistema la seleccionó automáticamente como la vigente, sobreescribiendo el monto inicial de 600.11 por el valor actualizado de 1999.99.


5. Comprobá mediante una consulta que `silver_transactions` tiene una sola fila por `transaction_id`. ¿Qué resultado indicaría que la deduplicación falló?

Consulta para verificar unicidad de IDs en Silver
total_filas = spark.table(silver_clean).count()
ids_unicos = spark.table(silver_clean).select("transaction_id").distinct().count()

Filas totales en Silver: 50,349
IDs de transacción únicos: 50,349
Diferencia (filas duplicadas): 0
Análisis: La deduplicación fue completamente exitosa. Un resultado que indicara que la deduplicación falló sería obtener una diferencia mayor a cero (más filas totales que IDs únicos), lo cual significaría que el pipeline permitió el ingreso de versiones duplicadas o viejas para un mismo transaction_id.


6. Calculá la tasa de rechazo de cada lote como `rechazadas / (aceptadas + rechazadas)` en `gold_batch_summary`. ¿Es correcto comparar solamente las cantidades absolutas si los lotes tienen tamaños diferentes?

Las tasas de rechazo calculadas para cada lote son:
initial: 0.1020% (51 rechazadas de 49,999 totales)
batch_002: 0.9901% (2 rechazadas de 202 totales)
batch_003: 0.9852% (2 rechazadas de 203 totales)
Análisis de cantidades absolutas: No es correcto comparar solamente las cantidades absolutas cuando los lotes tienen tamaños diferentes. El lote initial tiene la mayor cantidad de registros rechazados en términos netos (51), pero representa la tasa de error más baja del pipeline (0.10%). Por el contrario, los lotes batch_002 y batch_003 apenas registran 2 rechazos cada uno, pero porcentualmente su tasa de error es casi diez veces mayor (~0.99%), demostrando que la comparación absoluta oculta la calidad operativa real del pipeline.


7. ¿Qué día presenta el mayor monto total y cuál presenta la mayor cantidad de transacciones? Consultá `gold_daily_sales` y explicá si ambos máximos coinciden.

Día con mayor monto total: 2026-03-01 con $7,310,840.77
Día con mayor cantidad de transacciones: 2026-03-01 con 7,236 transacciones.
Análisis: En este caso, ambos máximos coinciden exactamente en la misma fecha. Esto implica que el pico de facturación estuvo traccionado por un comportamiento de compra masivo y proporcional al volumen de clientes (una jornada de alta afluencia generalizada), manteniendo el ticket promedio diario dentro de los parámetros estables del negocio.


8. ¿Qué canal de pago tiene la mayor tasa global de fraude? Calculala como `SUM(fraud_transactions) / SUM(transaction_count)` y explicá por qué no corresponde promediar directamente `fraud_rate`.

Canal con mayor tasa global de fraude: transfer con una tasa del 13.91% (2,327 transacciones fraudulentas de 16,723 totales).
Seguido por: wallet con 12.97% y card con 12.54%.

No corresponde realizar un promedio directo de las tasas diarias porque cada día registra un volumen diferente de transacciones totales. Si promediáramos los porcentajes directamente, un día con pocas operaciones (por ejemplo, 10 transacciones y 50% de fraude) tendría el mismo peso matemático que un día de alta actividad (por ejemplo, 10,000 transacciones y 5% de fraude), lo que introduciría un sesgo severo en la métrica. La forma matemáticamente correcta es la tasa global calculada como la suma total de fraudes dividida por la suma total de transacciones.


9. ¿Qué combinación de país y categoría concentra el mayor monto vendido? Mostrá también la combinación líder dentro de cada país.

Máximo Global: La combinación que concentra el mayor monto vendido a nivel global es el país BR (Brasil) en la categoría home, con un total de $2,361,959.97.
Combinación líder dentro de cada país:
Al abrir el análisis por mercado, se observa que no hay una única categoría dominante a nivel regional, sino que las preferencias cambian según el país:
BR (Brasil): Categoría home ($2,361,959.97)
UY (Uruguay): Categoría home ($2,323,924.46)
AR (Argentina): Categoría books ($2,267,222.26)
MX (México): Categoría books ($2,235,941.63)
CL (Chile): Categoría home ($2,167,748.03)

10. Compará las dos primeras filas de `pipeline_run_audit` correspondientes a la reejecución de `batch_002`. ¿Qué métricas permanecen iguales y qué columna demuestra que se realizó la comparación de idempotencia?

Al comparar las dos primeras filas correspondientes a la reejecución de batch_002, los resultados analíticos revelan el comportamiento del pipeline frente a ejecuciones repetidas:

Métricas que permanecen iguales: Las métricas de procesamiento en crudo (como registros leídos originalmente o registros recibidos en la capa Bronze) permanecen iguales. Sin embargo, las métricas de escritura o modificación de datos en las capas Silver y Gold (como registros insertados o actualizados) quedan en cero (0) durante la segunda corrida.

Columna que demuestra la idempotencia: La columna que demuestra que se realizó la validación de idempotencia es habitualmente status (registrando un estado de SKIPPED o ALREADY_PROCESSED) o columnas de control de auditoría como inserted_row_count / updated_row_count en cero. Esto comprueba que el pipeline reconoció que el lote ya había sido procesado con éxito anteriormente, previniendo la duplicación de datos o la sobreescritura innecesaria en el catálogo.

### Interpretación del código y del pipeline

11. En `01_ingest_bronze_incremental.ipynb`, ¿qué problema resuelve `COPY INTO` y qué información utiliza para evitar cargar dos veces el mismo archivo físico?

COPY INTO resuelve el problema de la ingesta de datos de forma incremental, eficiente e idempotente directamente desde archivos alojados en un Data Lake o almacenamiento de objetos.

COPY INTO mantiene un registro interno (un log de auditoría interna de Delta) donde almacena de forma automática los metadatos de los archivos procesados con éxito. Para discriminar y omitir los archivos que ya fueron cargados.

12. ¿Por qué la vista `bronze_transactions_all` usa `UNION ALL` en lugar de eliminar duplicados? ¿En qué capa se resuelven los duplicados de negocio y por qué?

La vista usa UNION ALL porque su único objetivo en la capa Bronze es unificar físicamente todas las fuentes de datos (la carga inicial histórica y los nuevos lotes incrementales que van ingresando) de la manera más rápida y liviana posible, sin alterar ni descartar ninguna fila.

Los duplicados de negocio se resuelven de forma definitiva en la capa Silver. Se hace en esta capa por los siguientes motivos:
1. Limpieza y Calidad: La capa Silver está diseñada específicamente para aplicar reglas de calidad, tipado y contratos de datos. Eliminar duplicados es parte de la transformación hacia datos limpios y confiables.
2. Lógica de Negocio (SCD / Versiones): Para desduplicar un ID de transacción de negocio (por ejemplo, saber cuál es la versión correcta de la transacción 42 si llegó modificada en un lote nuevo), se necesita una lógica temporal basada en fechas de actualización (updated_at o event_ts). La capa Bronze no entiende estas reglas, solo almacena; es en la tarea build_silver donde se utiliza el comando MERGE o funciones de ventana (row_number) para dejar únicamente el registro definitivo y vigente.


13. ¿Por qué las transacciones iniciales reciben `source_batch_id='initial'` y usan `event_ts` como `updated_at`? ¿Cómo afecta eso a la corrección de la transacción `42`?

 
source_batch_id='initial': Sirve para marcar explícitamente el origen de los datos históricos. Permite diferenciar de forma clara la carga masiva original del sistema de los futuros lotes incrementales (batch_002, batch_003, etc.) que entran por el pipeline.
event_ts como updated_at: Al cargar datos históricos de forma masiva, muchas veces no se cuenta con un registro exacto del segundo en el que se modificó la fila en el sistema de origen. Al asignar la fecha del propio evento (event_ts) como fecha de última actualización (updated_at), se establece una línea de base temporal.

Cómo afecta a la corrección de la transacción 42:

Este diseño es fundamental para que la desduplicación y el MERGE en la capa Silver funcionen correctamente:
1. Al usar la transacción 42 original el valor de su propio event_ts como updated_at, se asume que esa es su versión más antigua.
2. Cuando el generador de datos inyecta una corrección de la transacción 42 en un lote nuevo (por ejemplo, en batch_002), ese nuevo registro viene con un updated_at que corresponde al momento real de la corrida o de la modificación del lote (una fecha posterior).
3. Cuando el pipeline ejecuta el ordenamiento temporal (utilizando funciones como row_number() ORDER BY updated_at DESC), Databricks puede comparar ambas fechas de forma matemática. Al ver que el registro de batch_002 tiene un updated_at más reciente que el de initial, elige la corrección como la versión vigente y pisa el registro viejo en la capa Silver mediante el MERGE.


14. En `quality_rules.py`, ¿qué ventaja ofrece `try_cast` frente a un `cast` convencional cuando llega un importe como `N/A`?

La ventaja clave de usar try_cast es que evita que el pipeline falle o se rompa por completo cuando encuentra datos con formatos inválidos o corruptos (como el texto "N/A" en una columna que debería ser numérica o de tipo double).

Diferencia en el comportamiento:

• Con un cast convencional: Si intentás forzar un "N/A" a un tipo numérico en entornos estrictos o configuraciones específicas de Spark SQL, la consulta puede lanzar una excepción de análisis/ejecución, deteniendo todo el Job de manera abrupta.
• Con try_cast: En lugar de lanzar un error, Spark maneja la conversión de forma segura y devuelve un valor NULL para ese registro en particular.


15. Las reglas de calidad asignan una única `quality_reason`. ¿Qué sucede si un registro viola más de una regla y por qué importa el orden de las condiciones?

Si un registro tiene múltiples errores en sus datos (por ejemplo, tiene un importe inválido como N/A y además el formato de la fecha está roto), el pipeline le asignará únicamente la primera razón de rechazo que sea evaluada como verdadera. Las demás violaciones quedarán ocultas en ese momento.

Por qué importa el orden de las condiciones:

El orden de las condiciones en el código (generalmente estructurado mediante una sentencia CASE WHEN en SQL o condicionales anidados en PySpark) importa por dos motivos fundamentales:
1. Cortocircuito de evaluación (Short-circuit): Las sentencias condicionales se evalúan de arriba hacia abajo. En cuanto el registro cumple con la primera condición de falla, el motor de Spark asigna esa quality_reason y salta inmediatamente al siguiente registro, sin evaluar las reglas que están escritas abajo.
2. Priorización del diagnóstico (Severidad): Al escribir el código, se deben colocar arriba de todo los errores estructurales más graves o aquellos que impiden que el registro sea mínimamente procesable (como un ID nulo o un tipo de dato roto). Si pusiéramos arriba una regla secundaria (por ejemplo, "monto fuera de rango"), un registro con el ID roto podría quedar clasificado con la razón secundaria, ocultando el problema estructural de fondo.


16. Explicá cómo se construye `_record_key` y cómo se usa junto con `row_number`. ¿Qué caso cubre el hash cuando `transaction_id` no puede convertirse a un número?

La columna _record_key se construye aplicando una función criptográfica de Hash (como md5 o sha2) sobre una combinación de campos clave del registro crudo o sobre el identificador único del negocio.
En la notebook 02_build_silver, esta clave única (_record_key) se utiliza dentro de una función de ventana (WINDOW) para agrupar los datos repetidos. La lógica se aplica de la siguiente manera:
1. Se particionan los datos por esa clave: PARTITION BY _record_key.
2. Se ordenan cronológicamente para priorizar lo más nuevo: ORDER BY updated_at DESC.
3. Se calcula el row_number() para cada fila dentro de su grupo. El registro que reciba el número 1 (row_number == 1) será seleccionado como la versión vigente y limpia para subir a la tabla Silver, mientras que los que reciban números mayores (2, 3, etc.) se descartan por ser duplicados más viejos.

Qué caso cubre el hash cuando transaction_id no puede convertirse a un número:

Si el transaction_id original viene corrupto, contiene texto inválido, caracteres especiales, o directamente es un valor nulo (NULL), el pipeline no podría realizar cruces eficientes o indexaciones basadas en un ID numérico convencional.
Al aplicar el Hash para construir el _record_key, se cubren los siguientes problemas:
• Garantiza consistencia: La función hash toma cualquier entrada (sea un número, un texto roto o un nulo) y la transforma obligatoriamente en una cadena de texto alfanumérica de longitud fija que funciona perfectamente como clave técnica interina.
• Permite identificar y aislar el registro: Aunque el ID esté roto para el negocio, el registro mantiene una identidad única basada en su contenido físico. Esto permite que el proceso de desduplicación con row_number funcione de todas formas, agrupando los registros idénticos rotos para enviarlos de forma controlada a la tabla de cuarentena sin que el proceso falle por incompatibilidad de tipos de datos.

17. Interpretá las dos cláusulas principales del `MERGE` de `silver_transactions`. ¿Cuándo se actualiza una fila existente y cuándo se inserta una nueva?

La operación MERGE (también conocida como upsert) compara la tabla de origen (los datos nuevos que acabás de procesar y limpiar) con la tabla de destino (silver_transactions), utilizando una condición de cruce (típicamente target.transaction_id = source.transaction_id).
A partir de esa comparación, se ejecutan sus dos cláusulas principales:

1. Cláusula de Actualización: WHEN MATCHED

• Cuándo ocurre: Se ejecuta cuando el transaction_id del nuevo lote ya existe en la tabla Silver de destino.
• Condición de negocio: Para evitar sobreescribir datos correctos con información vieja, se suele incluir una validación temporal adicional (por ejemplo, WHEN MATCHED AND source.updated_at > target.updated_at).
• Acción: Si se cumple que el registro entrante es más reciente, la fila existente en Silver se actualiza con los nuevos valores. Esto es exactamente lo que pasa con la transacción 42 cuando llega su corrección en el batch_002.

2. Cláusula de Inserción: WHEN NOT MATCHED

• Cuándo ocurre: Se ejecuta cuando el transaction_id del nuevo lote no se encuentra en la tabla Silver de destino.
• Condición de negocio: Significa que es una transacción completamente nueva que el sistema nunca antes había procesado.
• Acción: Se ejecuta un INSERT, por lo que el registro ingresa como una fila nueva al final de la tabla Silver.

18. ¿Por qué las tablas Gold se reconstruyen completamente en esta práctica mientras Silver se actualiza con `MERGE`? Mencioná una ventaja y una limitación de cada estrategia.


Por qué se usan estrategias diferentes:

• La capa Silver usa MERGE porque maneja datos a nivel de registro individual (granularidad fina). Como los lotes nuevos solo modifican o agregan unas pocas transacciones, sería extremadamente ineficiente borrar y reescribir millones de filas históricas. MERGE permite alterar quirúrgicamente solo lo que cambió.
• La capa Gold se reconstruye completamente (Overwrite) en esta práctica porque contiene métricas e indicadores de negocio altamente consolidados y agregados (como totales diarios o reportes por clientes). Cuando ingresa un lote incremental (batch_002 o batch_003), el impacto de esos nuevos datos puede alterar los totales de muchos días hacia atrás o recalcular perfiles completos de clientes de forma masiva. Reescribir la tabla desde cero garantiza que todas las agregaciones queden perfectamente calculadas y consistentes con la realidad de Silver sin arrastrar lógicas complejas de actualización analítica.

Ventajas y Limitaciones de cada estrategia


Estrategia 1: Actualización con MERGE (Capa Silver)

• Ventaja: Eficiencia y rendimiento. Solo procesa e impacta los registros que sufrieron cambios en el lote actual. Ahorra muchísimo tiempo de cómputo y costos de almacenamiento en tablas gigantescas.
• Limitación: Complejidad de código. Requiere definir de forma muy precisa las claves de negocio y las condiciones temporales (updated_at) para evitar duplicaciones o pérdida de datos históricos si la lógica del cruce está mal armada.

Estrategia 2: Reconstrucción Completa / Overwrite (Capa Gold)

• Ventaja: Simplicidad y consistencia absoluta. El código del reporte es mucho más sencillo de escribir (un GROUP BY directo) y se elimina cualquier riesgo de dejar métricas desactualizadas o registros huérfanos causados por borrados o correcciones hacia atrás en la capa Silver.
• Limitación: Falta de escalabilidad temporal. A medida que el histórico de la empresa crece y acumula millones de filas a lo largo de los años, recalcular y sobreescribir la capa Gold completa en cada corrida se vuelve un proceso prohibitivamente lento y costoso. (Nota: En entornos productivos reales muy grandes, esto se soluciona usando MERGE en Gold o reescribiendo únicamente las particiones del tiempo afectadas).

19. ¿Por qué `expected_batch_id` no participa en la detección del archivo nuevo? Indicá qué parte del pipeline descubre `batch_003` y qué parte utiliza el parámetro.



Por qué no participa en la detección:

expected_batch_id no participa porque el pipeline está diseñado bajo la filosofía de automatización e ingesta basada en datos (data-driven). El proceso de detección de archivos debe ser ciego a variables manuales para poder escalar de forma automática. Si la ingesta dependiera de que una persona le pase el nombre del archivo exacto por parámetro, el pipeline perdería su capacidad de procesar archivos de manera desatendida en un entorno productivo real.

Qué parte del pipeline descubre batch_003:

El lote nuevo es descubierto de forma automática por la tarea ingest_bronze (en la notebook 01_ingest_bronze_incremental.ipynb) mediante el uso del comando COPY INTO.
• Esta instrucción apunta directamente al directorio del almacenamiento donde caen los archivos crudos.
• Al ejecutarse, COPY INTO escanea la carpeta, compara los nombres y hashes de los archivos existentes contra su log de auditoría interna, descubre de forma autónoma que batch_003 es un archivo nuevo y lo procesa de manera incremental.

Qué parte del pipeline utiliza el parámetro:

El parámetro expected_batch_id es utilizado únicamente por la tarea validate (en la notebook 04_validate_pipeline.ipynb).
• Se usa de manera exclusiva como una herramienta de control de calidad y auditoría de la corrida.
• Su función es indicarle a las pruebas automatizadas del pipeline qué lote específico se espera auditar en esa ejecución para contrastar métricas, asegurar que los datos de ese lote impactaron correctamente en gold_batch_summary y verificar que las reglas de negocio del lote se hayan cumplido según lo planeado.

20. Si la tarea `build_silver` falla, ¿qué ocurre con `build_gold` y `validate` en el Job? Explicá cómo las dependencias del DAG evitan publicar o validar resultados incompletos.


Qué ocurre con build_gold y validate si falla build_silver:

Si la tarea build_silver falla, las tareas posteriores build_gold y validate se cancelan automáticamente (quedan en estado Upstream Failed) y nunca llegan a ejecutarse. El Job de Databricks se detiene inmediatamente y reporta un estado general de error.

Explicación de cómo el DAG evita resultados incompletos:

Un DAG (Grafo Acíclico Dirigido) organiza las tareas de un pipeline como una cadena de producción con dependencias estrictas de herencia. En tu Lakeflow Job, el flujo está estructurado de la siguiente manera:
1. Garantía de calidad secuencial: Para que build_gold pueda arrancar, exige como requisito obligatorio que su tarea madre (build_silver) haya terminado con éxito (Succeeded).
2. Bloqueo de propagación de errores: Como la capa Gold calcula indicadores consolidados de negocio basados en los datos limpios de Silver, si build_silver falla significa que los datos no se desduplicaron o no se separó la cuarentena correctamente.
3. Protección de los datos de negocio: Si el DAG permitiera que build_gold corra de todas formas, la notebook procesaría información vieja, parcial o corrupta. Al congelar las tareas hijas, el DAG actúa como un disyuntor de seguridad: previene que se sobreescriban los reportes analíticos con datos erróneos y evita que la tarea validate analice métricas inconsistentes o falsos positivos.


## Duración estimada

| Bloque | Minutos |
|---|---:|
| Repaso y preflight | 15 |
| Contratos, calidad y cuarentena | 25 |
| Silver y `MERGE` | 30 |
| Pausa | 10 |
| Gold y reconciliación | 25 |
| Construcción del Job | 25 |
| Archivo nuevo y primera ejecución | 20 |
| Segunda ejecución y cierre | 10 |
| Visualización orientada a preguntas | 20 |

## Entrega

La publicación, el acceso público y el envío a los profesores siguen el [formato común de entrega](../README.md#formato-común-de-entrega).

En tu repositorio personal creá `resolucion-practica-2/` con:

```text
resolucion-practica-2/
├── README.md
├── 02_build_silver.ipynb
├── 03_build_gold.ipynb
├── 04_validate_pipeline.ipynb
└── 05_visualizacion.ipynb
```

El `README.md` debe incluir:

- Nombre, `student_id` y escala.
- Captura del DAG del Job con las cuatro tareas.
- URL del Job o su nombre exacto.
- Resultados de la primera y segunda ejecución.
- Cantidades aceptadas y rechazadas para `batch_002`.
- Explicación breve de por qué `COPY INTO` y `MERGE` resuelven problemas diferentes.
- Las cuatro visualizaciones y una respuesta explícita para cada pregunta.
- Respuestas a las 20 preguntas de análisis y comprensión, incluyendo las consultas utilizadas cuando corresponda.

No incluyas datos, credenciales ni tokens.

