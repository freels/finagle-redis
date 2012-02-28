package com.twitter.finagle.parser.util

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBufferIndexFinder, ChannelBuffer}


abstract class Matcher extends ChannelBufferIndexFinder {
  /**
   * number of readable bytes this matcher requires in order
   * to provide a conclusive match failure. A successful match
   * is still possible with fewer bytes available.
   */
  def bytesNeeded: Int

  /**
   * Tests whether or not `input` matches this beginning at `offset`
   *
   * returns the number of bytes taken by the match
   * -1 if the match is inconclusive
   * -2 if the match fails
   */
  def bytesMatching(input: ChannelBuffer, offset: Int): Int

  // satisfy ChannelBufferIndexFinder interface
  def find(input: ChannelBuffer, offset: Int) = {
    bytesMatching(input, offset) >= 0
  }

  def negate = new NotMatcher(this)

  def unary_! = negate
}

class BytesMatcher(delimiter: Array[Byte]) extends Matcher {

  def this(s: String) = this(s.getBytes("US-ASCII"))

  val bytesNeeded = delimiter.length

  def bytesMatching(buffer: ChannelBuffer, offset: Int): Int = {

    var i = 0
    while (i < delimiter.length) {
      if (buffer.writerIndex <= offset + i) return -1
      if (delimiter(i) != buffer.getByte(offset + i)) return -2
      i += 1
    }

    delimiter.length
  }

  override def toString = "BytesMatcher("+ (new String(delimiter, "US-ASCII")) +")"
}

object AlternateMatcher {
  def apply(choices: Seq[String]) = {
    new AlternateMatcher(choices map { _.getBytes("US-ASCII") } toArray)
  }
}

class AlternateMatcher(delimiters: Array[Array[Byte]]) extends Matcher {

  val choices = delimiters map { new BytesMatcher(_) }

  val bytesNeeded = delimiters map { _.length } max

  val minBytesNeeded = delimiters map { _.length } min

  def bytesMatching(buffer: ChannelBuffer, offset: Int): Int = {
    if (buffer.writerIndex < offset + minBytesNeeded) return -1

    var i = 0
    var inconclusive = false

    while (i < choices.length) {
      choices(i).bytesMatching(buffer, offset) match {
        case -2      => ()
        case -1      => inconclusive = true
        case isMatch => return isMatch
      }

      i = i + 1
    }

    if (inconclusive) -1 else -2
  }
}

class NotMatcher(inner: Matcher) extends Matcher {
  val bytesNeeded = inner.bytesNeeded

  def bytesMatching(buffer: ChannelBuffer, offset: Int): Int = {
    inner.bytesMatching(buffer, offset) match {
      case -2      => 0
      case -1      => -1
      case matched => -2
    }
  }
}
