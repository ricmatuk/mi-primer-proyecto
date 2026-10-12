# Entrega práctica 3 — Bases de datos NoSQL

- **Nombre: Ricardo Matuk**
- **student_id: ricardo_72**
- **Escala: small**

Respondé cada pregunta en una a tres oraciones. Cuando se pide un dato, copiá el valor que te dio el notebook.

## CAP y PACELC

**1. "Error: no disponible" en modo CP y pierde disponiblidad. "devuelve 100" respondió en modo AP y pierde consistencia. **

**2. En AP, C responde 100 aunque ya se había escrito 80 (pierde consistencia), y al reparar la red una de las dos escrituras concurrentes desaparece.**

**3.¿cuál es la p99 con W=1 y con W=3? 9.6 y 55.6 respectivamente
¿Qué se gana a cambio de esperar más réplicas? se consigue más consistencia, pero viene con mayor latencia**

**4. sólo el lado con mayoría puede seguir escribiendo porque estoy valorando la consistencia**

## Clave-valor

**5.El dict es 2,04 millones de veces más rápido.
Los pares en un dict de Python, que es una tabla hash en memoria, igual que el corazón de Redis. **

**6. En Redis no existe WHERE country = 'AR'. Hay que leer y parsear cada valor.
Ventaja: súper rápido.
Desventaja: si se requieren otro tipo de consulta, habría que mantener una/s estructura/s extra/s.**

**7. porcentaje de claves se movió con hash mod N: cerca de 80%. Con hashing consistente se movió 20% aproximadamente.
Con el anillo, o hashing constante, se mueve aproximadamente 1/(N+1), lo que lo hace más conveniente.**

## Documental

**8. La consulta relasional devolvió 11 filas del cliente 42 (Relacional: 3 tablas, 2 joins, 11 filas devueltas), mientras que 1 solo documento la consulta documental (Documental: 1 colección, 0 joins, 1 documento devuelto).
Qué datos quedaron embebidos dentro del documento? Datos del profile, de stats y de transactions.**

**9.Al leer documentos con campos distintos, ¿qué hizo Spark con los campos que faltaban en algunos? No precisa esquemas rígidos. Algunos campos pueden estar presentes en ciertos elementos y listo.
Por qué se dice que el modelo documental tiene esquema flexible? por su escala horizontal logra crecer facilmente según demanda. Las bases documentales permiten desarrollar más rápido, iterar sin fricción y evolucionar el modelo de datos al ritmo del negocio.**

**10. ¿Cuánto pesa el documento más grande? 16 MB.
¿Qué problema aparece si un documento crece sin límite? Fragmentación del disco (Movements): Si un documento crece constantemente, la base de datos se ve obligada a reubicarlo físicamente en otra parte del disco para hacerle espacio, lo que ralentiza muchísimo las operaciones de escritura.
 **

**11. Va bien con: • Lecturas por clave o ID: Traer una entidad completa (como un cliente con todas sus compras embebidas) en una sola operación de lectura.
• Escrituras de datos relacionados: Insertar todo el bloque de información junto, sin fragmentar la base de datos.
• Consultas de baja latencia: Ideal para aplicaciones web y móviles donde se necesita recuperar un objeto estructurado rápido sin hacer uniones complejas.
Va mal con: • Agregaciones complejas con arrays: Operaciones que obligan a usar explode (en Spark) o $unwind (en MongoDB) para aplanar subdocumentos, ya que duplican las filas en memoria y saturan el procesamiento.
• Cruces de datos (Joins / Lookups): Relacionar colecciones distintas. El modelo documental no está optimizado para integridad referencial ni para realizar "JOINs" masivos en tiempo real.
• Reportes analíticos globales: Computar métricas cruzadas sobre millones de registros anidados (como calcular totales por categorías secundarias).
**

## Grafos

**12. Cuántos nodos Customer (tiene 5.000), nodos Device (tiene 2.361) y aristas USES (5.400) tiene el grafo?**

**13.¿Cuántos joins necesitó SQL para el patrón de 2 saltos y para el de 4? 3 y 5 respectivamente
¿Por qué Neo4j recorre relaciones más eficientemente que una base relacional? La gran ventaja de Neo4j (y los grafos en general) se debe a una característica técnica llamada Index-Free Adjacency (Adyacencia Libre de Índices):
• Punteros directos en memoria: En Neo4j, cada nodo guardado en el disco y en la memoria RAM tiene punteros físicos directos hacia sus nodos vecinos (sus aristas).
• No calcula intersecciones: Para dar un "salto", Neo4j no tiene que buscar en un índice global ni escanear una tabla intermedia como hace SQL. Simplemente sigue la dirección del puntero de memoria, como si fuera un link.
• Costo constante O(1): El tiempo que tarda en recorrer un salto depende únicamente del tamaño del subgrafo local que estás visitando, y no del tamaño total de la base de datos. En SQL, si tu tabla tiene mil millones de filas, cada JOIN se vuelve extremadamente lento; en Neo4j, da igual el tamaño total de la red.
**

**14. ¿Cuántas iteraciones tardó en converger la búsqueda de componentes conexos y cuántos nodos tiene el componente más grande? 23 nodos y 22 aristas**

**15. ¿Qué porcentaje de aristas quedó entre máquinas distintas al repartir los nodos con hash(id) mod 4? 74,4%
¿Por qué es difícil distribuir una base de grafos? porque obliga a un salto de red.**

## Vectorial

**16. ¿Qué representa el vector de cada cliente y qué mide la similitud coseno? El vector busca resumir su comportamiento. La similitud se busca por los que tengan la misma dirección**

**17. ¿Qué porcentaje de clientes marcados hay entre los vecinos de clientes marcados y cuál es la tasa base? 31,6% y la tasa base es 14,8% 
¿Para qué sirve buscar "vecinos parecidos"? Al buscar, sólo revisa los nprobe clusters más cercanos a la consulta. Escanea menos, pero puede perder algún vecino verdadero: el trade-off recall vs. costo.**

**18. En la curva del índice IVF, ¿qué recall y qué porcentaje de vectores escaneados se obtienen con nprobe=4? 0.872 y 5.986 respectivamente
¿Qué se gana y qué se pierde con la búsqueda aproximada (ANN)?

Qué se gana

• Velocidad extrema (Baja Latencia): Las consultas pasan de tardar segundos o minutos (hacer un barrido lineal por toda la base de datos) a resolverse en milisegundos.
• Escalabilidad masiva: Permite buscar eficientemente entre millones o miles de millones de vectores de alta dimensión sin saturar el sistema.
• Ahorro de recursos (Cómputo/I/O): Al usar índices como IVF (Inverted File) o HNSW, el algoritmo solo escanea un subconjunto muy chico de los vectores (los grupos o clusters más probables), evitando calcular la distancia contra toda la base de datos (fuerza bruta).

Qué se pierde

• Precisión / Exactitud (Recall): No se garantiza encontrar los vecinos exactos ideales. Existe la posibilidad de que omitas el vector más cercano real si este quedó fuera de las regiones o clusters escaneados (por ejemplo, si configuras un nprobe muy bajo).
• Costo de memoria RAM: Los índices de búsqueda aproximada ocupan mucho espacio. Para mantener la velocidad, estos índices complejos generalmente deben cargarse por completo en la memoria RAM.
• Costo de construcción (Indexación): Crear el índice al principio (o actualizarlo cuando ingresan nuevos vectores) requiere un proceso de cómputo pesado (como entrenar clusters de K-Means en IVF).**

## Columnar

**19. ¿Cuánto ocupan los datos en CSV, JSON y Parquet? Parquet 5.3 GB, JSON 12.1 MB y CSV 50.0 MB
¿Qué columnas leyó Spark para la consulta de amount sobre Parquet (ReadSchema)? Consulta sobre amount:       ['ReadSchema: struct<amount:decimal(12,2)>'], solo lee una única columna. 
¿Cuántos archivos se pueden saltear con los datos ordenados? 7 de 8 archivos**

**20. ¿Por qué el formato columnar es bueno para analítica y poco conveniente para modificar una fila por vez?  permite:
Leer sólo las columnas necesarias (column pruning).
Comprimir mejor: valores del mismo tipo, uno al lado del otro.
Saltear bloques enteros usando estadísticas min/max (data skipping)
Poco conveniente para modificar una fila porque debe rastrillar toda la base.**
