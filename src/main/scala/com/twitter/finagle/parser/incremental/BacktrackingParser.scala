package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.ChannelBuffer


class BacktrackingParser[+Out](inner: Parser[Out], offset: Int) extends Parser[Out] {

  def this(inner: Parser[Out]) = this(inner, 0)

  def decode(buffer: ChannelBuffer) = {
    val start = buffer.readerIndex

    buffer.readerIndex(start + offset)

    // complains that Out is unchecked here, but this cannot fail, so
    // live with the warning.
    inner.decode(buffer) match {
      case r: Return[Out] => r
      case Continue(next) => {
        if (next == inner && buffer.readerIndex == (start + offset)) {
          buffer.readerIndex(start)
          Continue(this)
        } else {
          val newOffset = buffer.readerIndex - start
          buffer.readerIndex(start)
          Continue(new BacktrackingParser(next, newOffset))
        }
      }
      case e: Fail => {
        buffer.readerIndex(start)
        e
      }
      case Error(message) => {
        buffer.readerIndex(start)
        Fail(message)
      }
    }
  }
}
