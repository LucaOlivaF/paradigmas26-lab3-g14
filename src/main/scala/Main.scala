import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
 
    // ─────────────────────────────────────────────────────────────
    // Parseamos los argumentos de línea de comando (igual que antes)
    // ─────────────────────────────────────────────────────────────
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return
    }
    
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()
 
    // Silenciamos los logs verbosos de Spark para que la salida sea legible
    spark.sparkContext.setLogLevel("ERROR")
 
    val sc = spark.sparkContext

    val feedsSuccess = sc.longAccumulator("Feeds descargados con exito")
    val feedsFailed = sc.longAccumulator("Feeds que fallaron")
    val postsSuccess = sc.longAccumulator("Posts descargados en total")
    val postsFailed = sc.longAccumulator("Posts fallidos al parsear")
    val postsFiltered = sc.longAccumulator("Posts filtrados (vacíos/nulos)")

    val subscriptions: List[Subscription] = FileIO.readSubscriptions(cmdArgs.subscriptionFile) match {
      case Left(errorMsg) =>
        // Error fatal: archivo no encontrado o JSON inválido
        println(errorMsg)
        spark.stop()
        return
 
      case Right(subOpts) =>
        // Imprimimos warning por cada suscripción malformada (None)
        subOpts.foreach {
          case None => println("Warning: Skipping malformed subscription (missing 'name' or 'url' field)")
          case _    => ()
        }
        // Nos quedamos solo con las válidas (Some)
        subOpts.flatten
    }
 
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      spark.stop()
      return
    }
 
    val subscriptionsRDD = sc.parallelize(subscriptions)
 
  
    val allPostsRDD = subscriptionsRDD.flatMap { subscription =>
 
      val feedResult = FileIO.downloadFeed(subscription)
 
      feedResult match {
        case Left(warningMsg) =>
          println(warningMsg)
          feedsFailed.add(1)
          List.empty[Post]

        case Right(jsonContent) =>
          JsonParser.parsePosts(jsonContent, subscription) match {
            case Left(warningMsg) =>
              postsFailed.add(1)
              println(warningMsg)
              List.empty[Post]
 
            case Right(posts) =>
              feedsSuccess.add(1)
              postsSuccess.add(posts.length)
              posts
          }
      }
    }

    val filteredPostsRDD = allPostsRDD.filter { post =>
      val nonEmpty =
        post.title.nonEmpty &&
        post.selftext.nonEmpty &&
        post.selftext.trim.nonEmpty
      if(!nonEmpty) {
        postsFiltered.add(1)
      }
      nonEmpty
    }.cache()
 
    val t1_inicio = System.currentTimeMillis()

    val totalDownloaded = filteredPostsRDD.count()
    val t1_fin = System.currentTimeMillis()

    println(s"Accion terminal 1: ${(t1_fin - t1_inicio) / 1000.0} segundos")
 
    val avgChars: Long = if (postsSuccess.value > 0) {
      val totalChars = filteredPostsRDD.map(p => p.title.length + p.selftext.length).sum().toLong
      totalChars / postsSuccess.value
    } else 0L
 
    val stats = Map(
      "feedsSuccess"   -> feedsSuccess.value.toInt,
      "feedsFailed"    -> feedsFailed.value.toInt,
      "postsSuccess"   -> postsSuccess.value.toInt,
      "postsFailed"    -> postsFailed.value.toInt,
      "postsFiltered"  -> postsFiltered.value.toInt,
      "avgChars"       -> avgChars.toInt
    )
 
    println(Formatters.formatProcessingStats(stats))
    println()
 
   
    if (filteredPostsRDD.isEmpty()) {
      println("Error: No valid posts downloaded after filtering")
      spark.stop()
      return
    }
 
    val entitiesDir = new java.io.File(cmdArgs.entitiesDir)
    if (!entitiesDir.exists() || !entitiesDir.isDirectory) {
      println(s"Error: entities directory '${cmdArgs.entitiesDir}' not found")
      spark.stop()
      return
    }
 
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // a) flatMap: extraer entidades de cada post → RDD[NamedEntity]
    val entitiesRDD = filteredPostsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }.cache()

    // b) map: convertir cada entidad en par ((tipo, nombre), 1)
    val paresRDD = entitiesRDD.map { entity =>
      ((entity.entityType, entity.text), 1)
    }

    // c) reduceByKey: sumar por clave → RDD[((String, String), Int)]
    val entityCountsRDD = paresRDD.reduceByKey(_ + _)

    val t2_inicio = System.currentTimeMillis()
    val finalEntities = entityCountsRDD.collect()
    val t2_fin = System.currentTimeMillis()

    filteredPostsRDD.unpersist()

    println(s"Accion terminal 2: ${(t2_fin - t2_inicio) / 1000.0} segundos")

    // d) Ordenar y mostrar
    // Reuse the already-collected finalEntities instead of collecting entityCountsRDD again
    val entityCounts: Map[(String, String), Int] = finalEntities.toMap

    // Collect entitiesRDD only once and reuse the result for local analysis
    val entitiesList: List[NamedEntity] = entitiesRDD.collect().toList

    val typeStats = Analyzer.countByType(entitiesList)

    println(Formatters.formatTypeStats(typeStats))
    println()
    println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    }
}
