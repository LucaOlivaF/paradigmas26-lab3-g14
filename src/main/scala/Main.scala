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
          List.empty[Post]
 
        case Right(jsonContent) =>
          JsonParser.parsePosts(jsonContent, subscription) match {
            case Left(warningMsg) =>
              println(warningMsg)
              List.empty[Post]
 
            case Right(posts) =>
              posts
          }
      }
    }

    val filteredPostsRDD = allPostsRDD.filter { post =>
      post.title.nonEmpty &&
      post.selftext.nonEmpty &&
      post.selftext.trim.nonEmpty
    }
 
    val totalDownloaded = allPostsRDD.count()
    val totalFiltered   = filteredPostsRDD.count()
    val postsFiltered   = totalDownloaded - totalFiltered
 
    val feedResultsRDD = subscriptionsRDD.map { subscription =>
      FileIO.downloadFeed(subscription).isRight
    }
    val feedsSuccess = feedResultsRDD.filter(identity).count()
    val feedsFailed  = feedResultsRDD.filter(!_).count()
 
    val avgChars: Long = if (totalFiltered > 0) {
      val totalChars = filteredPostsRDD.map(p => p.title.length + p.selftext.length).sum().toLong
      totalChars / totalFiltered
    } else 0L
 
    val stats = Map(
      "feedsSuccess"   -> feedsSuccess.toInt,
      "feedsFailed"    -> feedsFailed.toInt,
      "postsSuccess"   -> totalDownloaded.toInt,
      "postsFailed"    -> 0,
      "postsFiltered"  -> postsFiltered.toInt,
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
 
    val allEntities = filteredPostsRDD.flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictionary)
    }.collect().toList
 
    // a) flatMap: extraer entidades de cada post → RDD[NamedEntity]
val entitiesRDD = filteredPostsRDD.flatMap { post =>
  val combinedText = post.title + " " + post.selftext
  Analyzer.detectEntities(combinedText, dictionary)
}

// b) map: convertir cada entidad en par ((tipo, nombre), 1)
val paresRDD = entitiesRDD.map { entity =>
  ((entity.entityType, entity.text), 1)
}

// c) reduceByKey: sumar por clave → RDD[((String, String), Int)]
val entityCountsRDD = paresRDD.reduceByKey(_ + _)

// d) Ordenar y mostrar
val entityCounts: Map[(String, String), Int] = entityCountsRDD.collect().toMap

val typeStats = Analyzer.countByType(entitiesRDD.collect().toList)

println(Formatters.formatTypeStats(typeStats))
println()
println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))

    }
}
