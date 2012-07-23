package com.redis

import serialization._

trait StringOperations { self: Redis =>

  // SET KEY (key, value)
  // sets the key with the specified value.
  def set(key: Any, value: Any)(implicit format: Format): Boolean =
    send("SET", List(key, value))(asBoolean)

  // GET (key)
  // gets the value for the specified key.
  def get[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("GET", List(key))(asBulk)
  
  // GETSET (key, value)
  // is an atomic set this value and return the old value command.
  def getset[A](key: Any, value: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("GETSET", List(key, value))(asBulk)
  
  // SETNX (key, value)
  // sets the value for the specified key, only if the key is not there.
  def setnx(key: Any, value: Any)(implicit format: Format): Boolean =
    send("SETNX", List(key, value))(asBoolean)

  def setex(key: Any, expiry: Int, value: Any)(implicit format: Format): Boolean =
    send("SETEX", List(key, expiry, value))(asBoolean) 

  // INCR (key)
  // increments the specified key by 1
  def incr(key: Any)(implicit format: Format): Option[Int] =
    send("INCR", List(key))(asInt)

  // INCR (key, increment)
  // increments the specified key by increment
  def incrby(key: Any, increment: Int)(implicit format: Format): Option[Int] =
    send("INCRBY", List(key, increment))(asInt)

  // DECR (key)
  // decrements the specified key by 1
  def decr(key: Any)(implicit format: Format): Option[Int] =
    send("DECR", List(key))(asInt)

  // DECR (key, increment)
  // decrements the specified key by increment
  def decrby(key: Any, increment: Int)(implicit format: Format): Option[Int] =
    send("DECRBY", List(key, increment))(asInt)

  // MGET (key, key, key, ...)
  // get the values of all the specified keys.
  def mget[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[List[Option[A]]] =
    send("MGET", key :: keys.toList)(asList)

  // MSET (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Overwrite value if key exists
  def mset(kvs: (Any, Any)*)(implicit format: Format) =
    send("MSET", kvs.foldRight(List[Any]()){ case ((k,v),l) => k :: v :: l })(asBoolean)

  // MSETNX (key1 value1 key2 value2 ..)
  // set the respective key value pairs. Noop if any key exists
  def msetnx(kvs: (Any, Any)*)(implicit format: Format) =
    send("MSETNX", kvs.foldRight(List[Any]()){ case ((k,v),l) => k :: v :: l })(asBoolean)

  // SETRANGE key offset value
  // Overwrites part of the string stored at key, starting at the specified offset, 
  // for the entire length of value.
  def setrange(key: Any, offset: Int, value: Any)(implicit format: Format): Option[Int] =
    send("SETRANGE", List(key, offset, value))(asInt)

  // GETRANGE key start end
  // Returns the substring of the string value stored at key, determined by the offsets 
  // start and end (both are inclusive).
  def getrange[A](key: Any, start: Int, end: Int)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("GETRANGE", List(key, start, end))(asBulk)

  // STRLEN key
  // gets the length of the value associated with the key
  def strlen(key: Any)(implicit format: Format): Option[Int] =
    send("STRLEN", List(key))(asInt)

  // APPEND KEY (key, value)
  // appends the key value with the specified value.
  def append(key: Any, value: Any)(implicit format: Format): Option[Int] =
    send("APPEND", List(key, value))(asInt)

  // GETBIT key offset
  // Returns the bit value at offset in the string value stored at key
  def getbit(key: Any, offset: Int)(implicit format: Format): Option[Int] =
    send("GETBIT", List(key, offset))(asInt)

  // SETBIT key offset value
  // Sets or clears the bit at offset in the string value stored at key
  def setbit(key: Any, offset: Int, value: Any)(implicit format: Format): Option[Int] =
    send("SETBIT", List(key, offset, value))(asInt)

  // SET EXPIRE (key, ttl)
  // sets the ttl for the key
  def expire(key: Any, ttl: Any)(implicit format: Format): Boolean =
    send("EXPIRE", List(key,ttl))(asBoolean)
}
