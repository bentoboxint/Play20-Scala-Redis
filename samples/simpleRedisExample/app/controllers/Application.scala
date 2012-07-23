package controllers

import play.api._
import play.api.mvc._

import com.redis._

object Application extends Controller {
  
  def index = Action {

    val r = new RedisClient("localhost", 6379)
    r.set("applicationCopy", "This message came from redis")

    Ok(r.get("applicationCopy").getOrElse("there was a problem getting the key from redis..."))
  }
  
}
