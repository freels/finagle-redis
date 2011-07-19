package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.ParseException


class BacktrackingParser[+Out](inner: Parser[Out], offset: Int) extends Parser[Out] {

  def this(inner: Parser[Out]) = this(inner, 0)

  def decode(buffer: ChannelBuffer) = {
    val start = buffer.readerIndex

    buffer.readerIndex(start + offset)

    // complains that Out is unchecked here, but this cannot fail, so
    // live with the warning.
    inner.decode(buffer) match {
      case r: Return[Out] => r
      case c: Continue[Out] => {
        if (c.next == inner && buffer.readerIndex == (start + offset)) {
          buffer.readerIndex(start)
          Continue(this)
        } else {
          val newOffset = buffer.readerIndex - start
          buffer.readerIndex(start)
          Continue(new BacktrackingParser(c.next, newOffset))
        }
      }
      case e: Fail => {
        buffer.readerIndex(start)
        e
      }
      case e: Error => {
        buffer.readerIndex(start)
        Fail(e.ex)
      }
    }
  }
}
