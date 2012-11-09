### TP POD - 2012 - Pereyra - Legajo 51190 - ITBA

- El sistema implementado distribuye mediante jGroups 'señales' para ser procesadas a cada nodo de la red, optimizando así el tiempo de procesamiento en un orden linear a la cantidad de computadoras agregadas al cluster.
- A su vez, maximiza el aprovechamiento de los núcleo del procesador o procesadores mediante la distribución del cálculo entre los mismos.
- Se prioriza la reducción de la cantidad de mensajes enviados al mínimo posible.
- La implementación es 1-tolerante a fallas, pudiendo responder a los cálculos incluso en la caida de un nodo, y rebalanceando los nodos en la caida del mismo.
- La asignación de las señales se hace basada en el hashCode cada señal individual.

### Cosas a mejorar (A criterio del alumno)
- La cantidad de mensajes al redistribuir los nodos puede ser excesiva, ya que se requiere rehashear cada nodo y enviar todo desde cero.
- La función de hash no fue una buena elección para distribuir los nodos, ya que con determinadas cantidades de nodos la distribución no es 100% "fair" a diferencia de lo que sería una aleatoria.
- Los tests adjuntados de autoría propia (los que usan la clase NodeController), NO se garantizan que pasen todas las pruebas, algunas se exceden del fin de este trabajo. Sí funcionan los tests SideBySide.

### Guía de uso

Se requiere maven 3 y java 7.

```
# Entre () los parámetros opcionales
# Entre <> los parámetros obligatorios

# Compilar
mvn clean package assembly:single -DskipTests=true

# Correr nodo
java (-Xmx2g) (-Djgroups.bind_addr=<ip_a_bindear>) -jar target/tp-pod-1.0-SNAPSHOT-jar-with-dependencies.jar <port> <threads> (clustername)

```


