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

## Ejercicio 2
### ¿Qué pasaría si dejaramos propagar la excepción?
Si en lugar de capturar el error adentro del flatMap dejaramos que la excepción se propague, pasarían estas cosas en cadena:
1. Spark reintenta la tarea. Cuando un worker lanza una excepción, Spark no falla inmediatamente. Primero reintenta la tarea en otro worker (por defecto 3 veces). Esto significa que esa descarga HTTP fallida se va a intentar 3 veces más, gastando tiempo y recursos innecesariamente.
2. Si todos los reintentos fallan, falla la partición entera
Una partición puede contener varias suscripciones. Si una falla y no se captura, todas las suscripciones de esa partición se pierden, no solo la que falló.
3. Si la partición era crítica, falla el job completo Spark termina el programa con una excepción como SparkException: Job aborted due to stage failure. No obtenemos ningún resultado parcial, todo el trabajo que hicieron los otros workers se descarta.

## Ejercicio 3
### reduceByKey es una barrera de sincronización. 
### ¿Qué ocurre en el cluster en ese punto? ¿Por qué es inevitable para este problema?

Cuando se invoca una reducción como reduceByKey, ocurre un proceso interno en Spark conocido como Shuffle.

Durante el shuffle, Spark necesita reagrupar los datos a través de la red para que todos los valores asociados a una misma clave (en este caso, el par (tipo, nombre) de la entidad) terminen en la misma partición y, por ende, en el mismo worker.

Es una "barrera de sincronización" porque ningún worker puede completar esta fase de reducción hasta que la fase anterior (el map que genera los pares) haya finalizado en todos los workers. Spark debe esperar a tener todos los datos mapeados antes de poder mezclarlos y enviarlos a sus destinos finales por la red.

### ¿Por qué es inevitable para este problema?
Es inevitable porque necesitas "sumar los valores de cada clave para obtener el conteo total por entidad" a nivel global. Como los posts originales se descargan y procesan en distintos workers de forma completamente independiente , la entidad "Scala", por ejemplo, puede haber sido detectada en el worker A y en el worker B. Para obtener el conteo total (la suma), el framework obligatoriamente debe juntar esas apariciones parciales cruzando la barrera de la red. El resultado depende de todos los elementos, no de uno solo.

### ¿Qué restricciones debe cumplir la función que se le pasa a reduceByKey?
La función que le pasas a reduceByKey (que en tu código es _ + _) debe cumplir estrictamente con dos propiedades matemáticas para que el resultado sea determinista y correcto en un entorno distribuido:  Conmutatividad (a + b = b + a): El orden de los operandos no debe alterar el resultado. En un entorno distribuido, los datos llegan a través de la red de diferentes workers a distintas velocidades. Spark no garantiza el orden en el que los valores de una misma clave serán procesados.Asociatividad ((a + b) + c = a + (b + c)): La agrupación de las operaciones no debe alterar el resultado final. Spark realiza agregaciones locales parciales en cada worker (combiner) antes de enviar los datos por la red para hacer la agregación final. Si la función no es asociativa, estas sumas parciales generarían un resultado incorrecto al combinarse globalmente.

### ¿Dónde se hace la lectura del diccionario de entidades? ¿En el driver o los workers?
Si observamos tu archivo Main.scala, la lectura del diccionario se realiza mediante la línea:

val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

Esta lectura se ejecuta en el driver. Ocurre en el cuerpo principal del programa (la función main), antes de invocar transformaciones sobre el RDD de posts como flatMap o map.

### ¿Cómo llega a los workers entonces? 
Cuando utilizas la variable dictionary dentro de la función de flatMap (específicamente en Analyzer.detectEntities(combinedText, dictionary)), Spark identifica que esa variable externa es necesaria para ejecutar la tarea distribuida. El framework se encarga de serializar ese diccionario y enviar una copia a través de la red a cada worker (en un concepto conocido como closure o clausura). De esta forma, aunque el driver lee el archivo del disco, los workers utilizan una copia en memoria que el framework les envió para hacer la detección.

## Ejercicio 4
### a) ¿Por qué los Accumulators solo deben usarse para métricas y no para tomar decisiones lógicas dentro de las etapas distribuidas del pipeline? ¿En qué situación un Accumulator puede dar un valor incorrecto?
Los Accumulators están diseñados exclusivamente para operaciones de solo escritura desde el punto de vista de los workers. El clúster no garantiza que un worker pueda leer el valor acumulado por otro en tiempo real durante la ejecución de una etapa distribuida. Si se intentara utilizar el valor de un acumulador para alterar el flujo lógico o aplicar un condicional (if/else) dentro de un map o filter, el comportamiento del programa sería indeterminado, rompiendo el paradigma funcional y de inmutabilidad sobre el que se construye Apache Spark.
Un acumulador puede arrojar un valor duplicado o incorrecto cuando se encuentra dentro de una transformacion si se produce un error dentro de esta. Esto entraria dentro del caso de efectos secundarios que mencionamos antes, donde Spark vuelve a ejecutar el worker despues del error, modificando el accumulator nuevamente. 

### b) ¿En qué momento del pipeline está disponible el valor de un Accumulator para ser leído por el driver?
El valor de un Accumulator solo se encuentra disponible y consolidado para ser leído por el driver únicamente después de que una acción terminal se haya completado con éxito (por ejemplo, luego de un .count() o un .collect()). Esto se debe a que el accumulator no es procesado hasta que todos los workers envien sus contadores al driver. Si se intenta leer antes de la acción terminal, el acumulador devolverá siempre su valor inicial (0).

### c) Comparen el tiempo que tarda cada etapa del pipeline que midieron en la versión no paralelizada y la versión con Spark. ¿Qué conclusiones pueden sacar? Para la cantidad de datos que estamos trabajando, ¿se aprecia la diferencia? Justifique por qué. Nota: La comparación debe realizarse en ejecuciones sobre la misma computadora y la misma conexión a internet.
| Versión del código      | Fase 1 | Fase 2 |
|-------------------------|--------|--------|
| Paralelizado con cache  | 5.374s | 0.206s |
| Paralelizado sin cache  | 5.332s | 5.409s |
| Sin paralelización | 15.876s | 0.051s |

Hay una diferencia apreciable, pero difiere en dos partes del código.
En la fase 1, que corresponde a la descarga de feeds y el procesado y filtrado de los posts, claramente hay una mejora a la hora de usar paralelización. Poder dividir entre workers la descarga de los feeds permite poder esperar las respuestas de los request en simultaneo, disminuyendo el tiempo de ejecución.
En la fase 1, que corresponde al procesado de entidades nombradas, los resultados difieren bastante. Primero está la diferencia entre el paralelizado con y sin cache. Esta diferencia viene de que al no guardar los datos de la fase 1 con cache() la pipeline entera se vuelve a ejecutar, dandote un tiempo en sin cache similiar a la suma de la fase 1 y 2 de con cache. Por el otro lado, la versión sin paralelización es más rápida que las otras dos. Esto se debe a la poca cantidad de post y entidades nombradas que hay para procesar, haciendo que el tiempo que se toma spark en crear los workers y recibir los resultados sea mayor a lo q tomas simplemente procesarlos sin paralelización.

## Ejercicio 5
### a) ¿Qué ocurriría si no llamaran a cache()? ¿Cuántas veces se ejecutaría la descarga de feeds?
Cuando no se llama a cache() cada vez que se usa un RDD desde el driver vuelve a ejecutar los pasos del pipeline requeridos para llegar a ese RDD.
En el caso de la descarga de feeds, como era el primer paso del pipeline era el que mas se repetia. En nuestro caso esto sucedia 5 veces por cada llamada de filteredPostsRDD que se usaba en main.

### b) ¿Por qué es incorrecto llamar a collect() entre los pasos a) y b) del ejercicio 3 y luego continuar el pipeline? ¿Qué consecuencia tiene sobre la distribución deltrabajo?
Es incorrecto llamar el collect() entre el flatmap del paso a) (entitiesRDD) y el map del paso b) (paresRDD) fuerza al driver a juntar los resultados de todos los workers ejecutando el paso a) antes de pasar al b). Esto rompe la paralelizacion de ambos pasos en el pipeline.

### c) cache() es también lazy. ¿En qué momento se almacena realmente el RDD en memoria?
Al usar cache() el RDD se almacena en memoria cuando se produce su primer accion terminal. Esto le da una ventaja sobre collect si el driver llama mas de una vez al RDD.
