import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._

object FileIO {

  /**
   * Read subscriptions from JSON file.
   * @param filePath path to subscriptions file
   * @return Right(list) on success, Left(errorMessage) on failure.
   *         Each subscription is Some(Subscription) if valid, None if malformed.
   */
   def readSubscriptions(filePath: String): Either[String, List[Option[Subscription]]] = {
    // Intentamos abrir el archivo. Si no existe, devolvemos Left con el mensaje de error.
    val content = try {
      val source = Source.fromFile(filePath)
      val c = source.mkString
      source.close()
      c
    } catch {
      case _: java.io.FileNotFoundException =>
        return Left(s"Error: Could not load $filePath - file not found")
    }
 
    // Intentamos parsear el JSON. Si está malformado, devolvemos Left.
    val json = try {
      parse(content)
    } catch {
      case _: Exception =>
        return Left(s"Error: Could not load $filePath - invalid JSON format")
    }
 
    implicit val formats: Formats = DefaultFormats
 
    // Extraemos la lista de mapas. Si el JSON no es una lista, también es inválido.
    val rawList: List[Map[String, Any]] = try {
      json.extract[List[Map[String, Any]]]
    } catch {
      case _: Exception =>
        return Left(s"Error: Could not load $filePath - invalid JSON format")
    }
 
    // Para cada entrada, intentamos construir una Subscription.
    // Si le falta "name" o "url", devolvemos None y el caller imprimirá el warning.
    val subscriptions = rawList.map { sub =>
      (sub.get("name"), sub.get("url")) match {
        case (Some(name: String), Some(url: String)) => Some(Subscription(name, url))
        case _ => None
      }
    }
 
    Right(subscriptions)
  }
 

  /**
   * Download feed JSON from URL.
   * @param url Reddit feed URL
   * @return Right(content) on success, Left(errorMessage) on failure.
   */
  def downloadFeed(subscription: Subscription): Either[String, String] = {
    try {
      val source = Source.fromURL(subscription.url)
      val content = source.mkString
      source.close()
      Right(content)
    } catch {
      case _: Exception =>
        Left(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
    }
  }

  /**
   * Read dictionary file line by line.
   * @param filePath path to dictionary file
   * @return None if file is missing or unreadable (caller prints the warning).
   */
  def readDictionaryFile(filePath: String): Option[List[String]] = {
    try {
      val source = Source.fromFile(filePath)
      val lines = source.getLines()
        .map(_.trim)
        .filter(_.nonEmpty)
        .filterNot(_.startsWith("#"))
        .toList
      source.close()
      Some(lines)
    } catch {
      case _: Exception => None
    }
  }
}
