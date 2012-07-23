package com.redis

import serialization._

trait SetOperations { self: Redis =>

  // SADD (VARIADIC: >= 2.4)
  // Add the specified members to the set value stored at key.
  def sadd(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Int] =
    send("SADD", List(key, value) ::: values.toList)(asInt)

  // SREM (VARIADIC: >= 2.4)
  // Remove the specified members from the set value stored at key.
  def srem(key: Any, value: Any, values: Any*)(implicit format: Format): Option[Int] =
    send("SREM", List(key, value) ::: values.toList)(asInt)

  // SPOP
  // Remove and return (pop) a random element from the Set value at key.
  def spop[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("SPOP", List(key))(asBulk)

  // SMOVE
  // Move the specified member from one Set to another atomically.
  def smove(sourceKey: Any, destKey: Any, value: Any)(implicit format: Format): Option[Int] =
    send("SMOVE", List(sourceKey, destKey, value))(asInt)

  // SCARD
  // Return the number of elements (the cardinality) of the Set at key.
  def scard(key: Any)(implicit format: Format): Option[Int] =
    send("SCARD", List(key))(asInt)

  // SISMEMBER
  // Test if the specified value is a member of the Set at key.
  def sismember(key: Any, value: Any)(implicit format: Format): Boolean =
    send("SISMEMBER", List(key, value))(asBoolean)

  // SINTER
  // Return the intersection between the Sets stored at key1, key2, ..., keyN.
  def sinter[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] =
    send("SINTER", key :: keys.toList)(asSet)

  // SINTERSTORE
  // Compute the intersection between the Sets stored at key1, key2, ..., keyN, 
  // and store the resulting Set at dstkey.
  // SINTERSTORE returns the size of the intersection, unlike what the documentation says
  // refer http://code.google.com/p/redis/issues/detail?id=121
  def sinterstore(key: Any, keys: Any*)(implicit format: Format): Option[Int] =
    send("SINTERSTORE", key :: keys.toList)(asInt)

  // SUNION
  // Return the union between the Sets stored at key1, key2, ..., keyN.
  def sunion[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] =
    send("SUNION", key :: keys.toList)(asSet)

  // SUNIONSTORE
  // Compute the union between the Sets stored at key1, key2, ..., keyN, 
  // and store the resulting Set at dstkey.
  // SUNIONSTORE returns the size of the union, unlike what the documentation says
  // refer http://code.google.com/p/redis/issues/detail?id=121
  def sunionstore(key: Any, keys: Any*)(implicit format: Format): Option[Int] =
    send("SUNIONSTORE", key :: keys.toList)(asInt)

  // SDIFF
  // Return the difference between the Set stored at key1 and all the Sets key2, ..., keyN.
  def sdiff[A](key: Any, keys: Any*)(implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] =
    send("SDIFF", key :: keys.toList)(asSet)

  // SDIFFSTORE
  // Compute the difference between the Set key1 and all the Sets key2, ..., keyN, 
  // and store the resulting Set at dstkey.
  def sdiffstore(key: Any, keys: Any*)(implicit format: Format): Option[Int] =
    send("SDIFFSTORE", key :: keys.toList)(asInt)

  // SMEMBERS
  // Return all the members of the Set value at key.
  def smembers[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[Set[Option[A]]] =
    send("SMEMBERS", List(key))(asSet)

  // SRANDMEMBER
  // Return a random element from a Set
  def srandmember[A](key: Any)(implicit format: Format, parse: Parse[A]): Option[A] =
    send("SRANDMEMBER", List(key))(asBulk)
}
