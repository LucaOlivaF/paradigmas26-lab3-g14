# INFORME


## Ejercicio 1

### a) Diagrama de flujo
<img src="public/diagrama_de_flujo.png" />

### b) Abstracciones de Spark
- Inicialización: al ser solo la lectura del archivo JSON que contiene las suscripciones a través del driver, no encaja con ninguna abstracción de Spark.
- Conexión y descarga: puede expresarse como un map, ya que por cada conexión crea una tupla con el URL y la lista de posts.
- Extracción y clasificación de entidades: puede expresarse como un flatMap, ya que por cada post puede crear una lista de cero o más NamedEntity.
- Conteo de entidades: puede expresarse con un reduceByKey, ya que agrupa las NamedEntitys por tipo y nombre.
- Impresión de resultados: al ser impresión de datos por pantalla, paralelizarlo podría producir que la información que sale por pantalla no sea legible o no tenga el formato deseado.

### c) Barreras de sincronización
Los pasos del pipeline que son barreras de sincronización serían "Conteo de entidades" e "Impresión de resultados". Como "Conteo de entidades" está implementado con un reduceByKey, todos los resultados no pueden ser procesados por "Impresión de resultados" hasta que estén completos, porque usamos collect para los resultados obtenidos anteriormente.
En cambio, Conexión y descarga, y Extracción y clasificación de entidades, pueden ejecutarse de forma completamente independiente. Esto es posible porque usan flatMap y map, donde cada post se procesa y crea su propia lista de entidades.

### d) Restricciones del mecanismo de extensión de Spark
Dada la paralelización de los workers con Spark, las restricciones del mecanismo de extensión están definidas por las siguientes características:
- Serialización: Cuando le pasás una función en el Driver, Spark tiene que empaquetar ese código y enviarlo a través de la red hacia todos los Workers. Por lo tanto, tanto la función como cualquier variable externa que la función utilice adentro deben ser serializables. Si intentás usar objetos no serializables, el programa va a crashear con una excepción de serialización.
- Estado compartido: Dado que los workers son procesos independientes, si la función intenta modificar una variable externa normal, cada worker modificará una copia local de esa variable. El valor original en el Driver nunca cambiará.
- Efectos secundarios: Las funciones que se pasan a Spark deberían ser "puras" (como lo define el paradigma funcional). Si un worker falla por un problema de red o falta de memoria, Spark es tolerante a fallos y puede volver a ejecutar esa misma tarea en otro worker. Si la función tenía un efecto secundario, al reejecutarse la tarea podría volver a producir ese efecto secundario.