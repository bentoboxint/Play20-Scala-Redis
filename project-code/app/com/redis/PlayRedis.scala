package com.redis

import play.api.Play
import com.redis._

object PlayRedis {
    def RedisCache:RedisClient = new RedisClient(
        Play.current.configuration.getString("redis.host") match {
            case Some(x) => x
            case None => "localhost"
        },
        Play.current.configuration.getInt("redis.port") match {
            case Some(x) => x
            case None => 6379
        }
    )
}
