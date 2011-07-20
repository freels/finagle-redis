package com.twitter.finagle.parser.test

import scala.annotation.tailrec
import java.nio.charset.Charset
import org.specs.Specification
import org.specs.matcher.Matcher
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.finagle.parser.incremental.{Error => ParseError}
import com.twitter.finagle.parser.incremental._


class ParserSpecification extends Specification {
  class RichParser[Out](p: Parser[Out]) {
    def mustParse(source: ChannelBuffer) = {
      new ParserTest(p, source)
    }

    def mustParse(source: String) = {
      new ParserTest(p, ChannelBuffers.wrappedBuffer(source.getBytes("UTF-8")))
    }
  }

  class ParserTest[Out](p: Parser[Out], source: ChannelBuffer) {
    val in = ChannelBuffers.dynamicBuffer

    @tailrec private def go(rv: ParseResult[Out]): ParseResult[Out] = rv match {
      case e: Fail        => e
      case e: Error       => e
      case Return(o)      => Return(o)
      case Continue(next) => if (source.readableBytes > 0) {
        in.writeByte(source.readByte)
        go(next.decode(in))
      } else {
        Continue(next)
      }
    }

    // start with an empty buffer
    lazy val rv = go(p.decode(in))

    def andReturn(out: Out) = {
      rv mustEqual Return(out)
      this
    }

    def andReturn() = {
      rv must haveClass[Return[Out]]
      this
    }

    def andFail(err: String) = {
      rv mustEqual Fail(err)
      this
    }

    def andFail() = {
      rv must haveClass[Fail]
      this
    }

    def andError(err: String) = {
      rv mustEqual ParseError(err)
      this
    }

    def andError() = {
      rv must haveClass[Error]
      this
    }

    def andContinue(n: Parser[Out]) = {
      rv mustEqual Continue(n)
      this
    }

    def andContinue() = {
      rv must haveClass[Continue[Out]]
      this
    }

    def readingBytes(c: Int) {
      rv
      in.readerIndex mustEqual c
    }

    def leavingBytes(r: Int) {
      rv
      (source.writerIndex - in.readerIndex) mustEqual r
    }
  }

  implicit def parser2Test[T](parser: Parser[T]) = new RichParser(parser)

  def asString(b: ChannelBuffer) = {
    b.toString(Charset.forName("UTF-8"))
  }

  def Buffer(s: String) = {
    ChannelBuffers.wrappedBuffer(s.getBytes("UTF-8"))
  }
}
