package com.redis.cluster

import java.util.zip.CRC32
import scala.collection.immutable.TreeSet
import scala.collection.mutable.{ArrayBuffer, Map, ListBuffer}

case class HashRing[T](nodes: List[T], replicas: Int) {
  var sortedKeys = new TreeSet[Long]
  val cluster = new ArrayBuffer[T]
  var ring = Map[Long, T]()

  nodes.foreach(addNode(_))

  // adds a node to the hash ring (including a number of replicas)
  def addNode(node: T) = {
    cluster += node
    (1 to replicas).foreach {replica =>
      val key = calculateChecksum((node + ":" + replica).getBytes("UTF-8"))
      ring += (key -> node)
      sortedKeys = sortedKeys + key
    }
  }

  // remove node from the ring
  def removeNode(node: T) {
    cluster -= node
    (1 to replicas).foreach {replica =>
      val key = calculateChecksum((node + ":" + replica).getBytes("UTF-8"))
      ring -= key
      sortedKeys = sortedKeys - key
    }
  }

  // get node for the key
  def getNode(key: Seq[Byte]): T = {
    val crc = calculateChecksum(key)
    if (sortedKeys contains crc) ring(crc)
    else {
      if (crc < sortedKeys.firstKey) ring(sortedKeys.firstKey)
      else if (crc > sortedKeys.lastKey) ring(sortedKeys.lastKey)
      else ring(sortedKeys.rangeImpl(None, Some(crc)).lastKey)
    }
  }

  // Computes the CRC-32 of the given String
  def calculateChecksum(value: Seq[Byte]): Long = {
    val checksum = new CRC32
    checksum.update(value.toArray)
    checksum.getValue
  }
}

