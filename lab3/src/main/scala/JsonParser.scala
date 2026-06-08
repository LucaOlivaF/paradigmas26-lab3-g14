import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonParser {

  /**
   * Parse Reddit JSON feed and extract posts.
   * @param jsonContent JSON string from Reddit API
   * @param subscriptionName name of subscription (for logging)
   * @return Right(posts) on success, Left(warningMessage) on failure.
   */
  def parsePosts(jsonContent: String, subscription: Subscription): Either[String, List[Post]] = {
    try {
      implicit val formats: Formats = DefaultFormats
 
      val json = parse(jsonContent)
      val children = (json \ "data" \ "children").extract[List[JValue]]
 
      val posts = children.flatMap { child =>
        val data = child \ "data"
        val title    = (data \ "title").extractOpt[String].getOrElse("")
        val selftext = (data \ "selftext").extractOpt[String].getOrElse("")
        List(Post(title, selftext))
      }
 
      Right(posts)
    } catch {
      case _: Exception =>
        Left(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
    }
  }
}
