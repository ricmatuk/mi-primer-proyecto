# Entrega práctica 4 — Streaming

- **Nombre: Ricardo Matuk**
- **student_id: ricardo_72**
- **Escala: small**

Respondé cada pregunta en una a tres oraciones. Cuando se pide un dato, copiá el valor que te dio el notebook.

## Log y offsets

**1. Cantidad de mensajes quedaron en la cola y cuántos en el log después de leer tres: 2 en Cola y los 5 en Log. ¿Qué diferencia hay entre una cola tradicional y un log como Kafka? En Log se mantiene toda la info, detallando lo que se mandó al consumidor **

**2. Después de la llegada 2, mensajes leídos por el consumidor antifraude: 2; y cuántos reportes: 0 ¿El atraso de reportes afectó a antifraude? No ¿Por qué? Tienen distinto ritmo de procesamiento. Antifraude procesa en cada llegada.**

**3. ¿Qué es un offset y dónde lo guarda cada consumidor en esta práctica? Representa el checkpoint de cada consulta, donde se guarda hasta qué versión del topic se leyó. En este caso se detalla (próxima versión a leer): 6 **

**4. ¿Cuántos mensajes leyó el replay? 280
¿Qué propone la arquitectura Kappa para reprocesar datos y en qué se diferencia de Lambda? En Kappa hay un solo código de streaming: para reprocesar se lanza una consulta nueva (otro checkpoint) que lee el log desde el principio. En una arquitectura Lambda habría dos sistemas: un batch preciso y una capa de streaming rápida, con la misma lógica escrita dos veces. **

## Ingesta y enriquecimiento

**5. ¿Cuántas filas nuevas procesó Bronze en cada llegada y cuántas al reejecutar sin archivos nuevos? Cada consulta procesa lo nuevo. El stream_001 tiene 60 filas y leyó 60. En strea_002 tiene 120 filas y lee 120 Bronze, etc.
¿Qué componente evita leer dos veces el mismo archivo? Es gracias al Auto Loader (format("cloudFiles")) por lo cual recuerda en su checkpoint qué archivos ya leyó. Es lo similar al offset pero para archivos.**

**6. ¿Qué motivos de rechazo aparecieron en la cuarentena y en qué llegada? Motivos: INVALID_AMOUNT y UNKNOWN_CUSTOMER, todo en el stream_002.
¿Por qué Bronze guarda el texto original en lugar de descartar los registros con errores? Porque Bronze guarda los datos crudos, en la siguiente capa se procesan y se van filtrando.**

**7. ¿Qué columnas agregó el join con silver_customers y silver_products?  Se agregó el país del cliente (silver_customers) y la categoría del producto (silver_products)
¿Qué tipo de join es (stream-stream, stream-tabla o tabla-tabla) según la teoría? El join stream-tabla enriquece cada evento con datos de referencia. Si la tabla de referencia cambia, los eventos siguientes ven la versión nueva; los ya procesados no se recalculan **

## Tiempo y ventanas

**8. ¿Qué diferencia hay entre event time y processing time? El primero es cuando sucedió y el segundo es cuando nos enteramos o cuando llega a nuestro sistema.  late_ok ocurrió a las 12:03 pero llega en el stream de compras entre 12:06 a 12:08.**

**9. ¿Cuánto valía el watermark al terminar cada llegada?  En la llegada 1 el watermark valía 11:54; en la 2 11:58, tec.
¿Cómo se calcula a partir del máximo event time visto? Son 10 minutos antes del "máximo event time visto".**

**10.¿En qué llegada aparecieron las primeras ventanas tumbling en modo append y por qué no antes? No aparecen antes porque el watermark todavía no superó el fin de ninguna ventana. En la llegada 3 aparecen compras de las 12:25 a 12:26. El watermark salta a 12:16 y se cierran las ventanas de 12:00 a 12:10.**

**11. ¿Qué pasó con late_bad? late_bad (12:02), más viejo que el watermark (12:16): se descarta y no suma en ninguna ventana. 
¿Por qué se aceptó late_ok y se descartó late_bad? Se aceptó late_ok porque ingresó dentro del watermark**

**12. Mirando el gráfico de ventanas, ¿en qué se diferencian tumbling, hopping y session? Tumbling es un conjunto cerrado, sin solapamiento. Hopping es un promedio de los últimos 10 minutos (hay un momento en que ese valor sube a 120, juntando dos paquetes disjuntos en Tumbling). Session no mezcla los consumidores.
Da un ejemplo de negocio para cada una. Tumbling: muestra la cantidad de energía empleada o consumida, hopping: Uber si el pedido promedio de autos se eleva se actualiza el precio, y session: el carrito de compras de un consumidor en un supermercado **

**13. Para la ventana [12:00, 12:05) del canal card, ¿cuántas veces y con qué valores se emitió en modo update y en modo append? En update la ventana se emite apenas tiene datos (llegada 1) y se vuelve a emitir corregida cuando llega late_ok.
En append se emite una sola vez, con el valor final, recién cuando el watermark la cierra.
¿Qué ventaja y qué desventaja tiene cada modo? En Update el consumidor debe tolerar recibir la misma ventana dos veces. Append es más simple, pero con más demora.**

**14. ¿Qué se gana y qué se pierde si el watermark fuera de 1 minuto en lugar de 10? Elegir el watermark es un trade-off entre latencia (cerrar antes) y completitud (esperar tardíos). **

## Garantías y fallas

**15. ¿Cuántas filas quedaron en el destino sin deduplicar y cuántas deduplicando por event_id? Compras válidas en Silver: 220 filas · 200 event_id distintos → 20 duplicados
¿Por qué el productor puede enviar el mismo evento dos veces? El productor: ante la duda, reintenta**

**16. ¿Qué pasó al reiniciar con el mismo checkpoint y qué pasó al perderlo? 
Sin checkpoint la consulta no sabe qué procesó y empieza de nuevo. Como el destino es un append común, todo se escribe otra vez.
¿Qué garantía de entrega se observa en cada caso? duplicamos resultados **

**17. ¿Por qué con MERGE el resultado no cambió aunque se perdiera el checkpoint? Con un MERGE por event_id: si el evento ya está, no se inserta.   
¿Qué significa que una escritura sea idempotente? Una escritura idempotente (MERGE por clave, SET x = 5 en lugar de INCREMENT x) hace que reprocesar no cambie el resultado.**

## CDC

**18. ¿Cuántos eventos de cambio generó el log y de qué tipos? Cinco operaciones. cliente 1 pasa a premium
cliente 2 se muda a CL
se elimina el cliente 3
alta del cliente 100
cliente 2 se muda a BR
¿Por qué un UPDATE genera dos eventos? Un UPDATE genera dos eventos: la fila antes (update_preimage) y después (update_postimage)**

**19. ¿Se pudo reconstruir la tabla a partir del log? Sí. La idea de log compaction en Kafka: guardar sólo el último valor por clave alcanza para reconstruir la base.
¿Qué información tiene el log que la tabla actual no tiene? El log conserva información que el estado pierde (la mudanza intermedia a CL, el cliente borrado).**

## Cierre

**20. Se descarta**
