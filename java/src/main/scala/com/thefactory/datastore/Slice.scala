package com.thefactory.datastore

import scala.language.implicitConversions

object SliceImplicits {
  implicit def String2Slice(s: String): Slice = {
    new Slice(s.getBytes())
  }

  implicit def Slice2String(s: Slice): String = {
    new String(s.toUTF8String())
  }    

  implicit def Slice2Int(s: Slice): Int = {
    s.toUTF8String.toInt
  }    

  implicit def Int2Slice(i: Int): Slice = {
    "%d".format(i)
  }    

  def isPrefix(slice: Slice, prefix: Slice) : Boolean = {
    com.thefactory.datastore.Slice.isPrefix(slice, prefix)
  }

  def isPrefix(kv: KV, prefix: Slice) : Boolean = {
    isPrefix(kv.getKey(), prefix)
  }    

  def compare(first: Slice, second: Slice) : Int = {
    com.thefactory.datastore.Slice.compare(first, second)
  }
}
