package com.twitter.finagle.parser.util

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBufferIndexFinder, ChannelBuffer}


abstract class Matcher extends ChannelBufferIndexFinder {
  /*
   * number of readable bytes this matcher requires in order
   * to provide a conclusive match failure. A successful match
   * is still possible with fewer bytes available.
   */
  def bytesNeeded: Int

  /*
   * Tests whether or not `input` matches this beginning at `offset`
   *
   * returns the number of bytes taken by the match, or -1 if there is no match
   */
  def bytesMatching(input: ChannelBuffer, offset: Int): Int

  def find(input: ChannelBuffer, offset: Int) = {
    bytesMatching(input, offset) >= 0
  }
}

class DelimiterMatcher(delimiter: Array[Byte]) extends Matcher {

  def this(s: String) = this(s.getBytes("US-ASCII"))

  val bytesNeeded = delimiter.length

  def bytesMatching(buffer: ChannelBuffer, offset: Int): Int = {
    if (buffer.writerIndex < offset + delimiter.length) return -1

    var i = 0

    while (i < delimiter.length) {
      if (delimiter(i) != buffer.getByte(offset + i)) return -1
      i = i + 1
    }

    delimiter.length
  }

  override def toString = "DelimiterMatcher("+ (new String(delimiter, "US-ASCII")) +")"
}

object AlternateMatcher {
  def apply(choices: Seq[String]) = {
    new AlternateMatcher(choices map { _.getBytes("US-ASCII") } toArray)
  }
}

class AlternateMatcher(delimiters: Array[Array[Byte]]) extends Matcher {

  val choices = delimiters map { new DelimiterMatcher(_) }

  val bytesNeeded = delimiters map { _.length } max

  val minBytesNeeded = delimiters map { _.length } min

  def bytesMatching(buffer: ChannelBuffer, offset: Int): Int = {
    if (buffer.writerIndex < offset + minBytesNeeded) return -1

    var i = 0

    while (i < choices.length) {
      choices(i).bytesMatching(buffer, offset) match {
        case -1      => i = i + 1
        case isMatch => return isMatch
      }
    }

    -1
  }
}

class NotMatcher(inner: Matcher) extends Matcher {
  val bytesNeeded = inner.bytesNeeded

  def bytesMatching(buffer: ChannelBuffer, offset: Int): Int = {
    if (buffer.writerIndex < offset + bytesNeeded) return -1

    if (inner.bytesMatching(buffer, offset) == -1) 0 else -1
  }
}
