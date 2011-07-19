package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBufferIndexFinder, ChannelBuffer}
import com.twitter.finagle.parser.util._
import com.twitter.finagle.ParseException


object Parsers {
  def readTo(choices: String*) = {
    new ConsumingDelimiterParser(AlternateMatcher(choices))
  }

  def readUntil(choices: String*) = {
    new DelimiterParser(AlternateMatcher(choices))
  }

  val readLine = readTo("\r\n", "\n")

  def fail(ex: ParseException) = new ConstParser(Fail(ex))

  def error(ex: ParseException) = new ConstParser(Error(ex))

  def success[T](t: T) = new ConstParser(Return(t))

  def lift[T](f: => T): Parser[T] = {
    try {
      success(f)
    } catch {
      case e: ParseException => fail(e)
    }
  }

  def attempt[T](p: Parser[T]) = new BacktrackingParser(p)

  val unit = success(())

  def readBytes(size: Int) = new FixedBytesParser(size)

  def skipBytes(size: Int) = readBytes(size) append unit

  def accept(m: Matcher) = new ConsumingMatchParser(m)

  implicit def accept(choice: String): Parser[ChannelBuffer] = {
    accept(new DelimiterMatcher(choice))
  }

  def accept(choices: String*): Parser[ChannelBuffer] = {
    accept(AlternateMatcher(choices))
  }

  def guard(m: Matcher) = new MatchParser(m)

  def guard(choice: String): Parser[ChannelBuffer] = {
    guard(new DelimiterMatcher(choice))
  }

  def guard(choices: String*): Parser[ChannelBuffer] = {
    guard(AlternateMatcher(choices))
  }

  def not(m: Parser[Any]) = new NotParser(m)

  def choice[T](choices: (String, Parser[T])*) = {
    val (m, p)          = choices.last
    val last: Parser[T] = accept(m) append p

    (choices.tail.reverse foldRight last) { (choice, rest) =>
      val (m, p) = choice
      (accept(m) append p) or rest
    }
  }

  def rep[T](p: Parser[T]): Parser[List[T]] = {
    def go(): Parser[List[T]] = {
      (for (t <- p; ts <- go) yield (t :: ts)) or success[List[T]](Nil)
    }

    go()
  }

  def rep1[T](p: Parser[T], q: Parser[T]): Parser[List[T]] = {
    val getRest = rep(q)

    for (head <- p; tail <- getRest) yield (head :: tail)
  }

  def rep1[T](p: Parser[T]): Parser[List[T]] = {
    rep1(p, p)
  }

  def rep1sep[T](p: Parser[T], sep: Parser[Any]): Parser[List[T]] = {
    val getRest = repsep(p, sep)

    for (head <- p; tail <- getRest) yield (head :: tail)
  }

  def repsep[T](p: Parser[T], sep: Parser[Any]): Parser[List[T]] = {
    val end = sep append success[List[T]](Nil)

    def go(): Parser[List[T]] = {
      end or (for (t <- p; ts <- go) yield (t :: ts))
    }

    go()
  }

  def repN[T](total: Int, parser: Parser[T]) = {
    def go(i: Int, prev: List[T]): Parser[Seq[T]] = {
      if (i == total) {
        success(prev.reverse)
      } else {
        parser flatMap { rv =>
          go(i + 1, rv :: prev)
        }
      }
    }

    go(0, Nil)
  }


  private[parser] abstract class PrimitiveParser[Out] extends Parser[Out] {
    protected val continue = Continue(this)
  }

  // integral primitives

  val readByte = new PrimitiveParser[Byte] {
    def decode(buffer: ChannelBuffer) = {
      if (buffer.readableBytes >= 1) Return(buffer.readByte) else continue
    }
  }

  val readShort = new PrimitiveParser[Short] {
    def decode(buffer: ChannelBuffer) = {
      if (buffer.readableBytes >= 2) Return(buffer.readShort) else continue
    }
  }

  val readMedium = new PrimitiveParser[Int] {
    def decode(buffer: ChannelBuffer) = {
      if (buffer.readableBytes >= 3) Return(buffer.readMedium) else continue
    }
  }

  val readInt = new PrimitiveParser[Int] {
    def decode(buffer: ChannelBuffer) = {
      if (buffer.readableBytes >= 4) Return(buffer.readInt) else continue
    }
  }

  val readLong = new PrimitiveParser[Long] {
    def decode(buffer: ChannelBuffer) = {
      if (buffer.readableBytes >= 8) Return(buffer.readLong) else continue
    }
  }


  // Unsigned integral primitives

  val readUnsignedByte = new PrimitiveParser[Short] {
    def decode(buffer: ChannelBuffer) = {
      if (buffer.readableBytes >= 1) Return(buffer.readUnsignedByte) else continue
    }
  }

  val readUnsignedShort = new PrimitiveParser[Int] {
    def decode(buffer: ChannelBuffer) = {
      if (buffer.readableBytes >= 2) Return(buffer.readUnsignedShort) else continue
    }
  }

  val readUnsignedMedium = new PrimitiveParser[Int] {
    def decode(buffer: ChannelBuffer) = {
      if (buffer.readableBytes >= 3) Return(buffer.readUnsignedMedium) else continue
    }
  }

  val readUnsignedInt = new PrimitiveParser[Long] {
    def decode(buffer: ChannelBuffer) = {
      if (buffer.readableBytes >= 4) Return(buffer.readUnsignedInt) else continue
    }
  }


  // non-integral primitives

  val readChar = new PrimitiveParser[Char] {
    def decode(buffer: ChannelBuffer) = {
      if (buffer.readableBytes >= 2) Return(buffer.readChar) else continue
    }
  }

  val readDouble = new PrimitiveParser[Double] {
    def decode(buffer: ChannelBuffer) = {
      if (buffer.readableBytes >= 8) Return(buffer.readDouble) else continue
    }
  }

  val readFloat = new PrimitiveParser[Float] {
    def decode(buffer: ChannelBuffer) = {
      if (buffer.readableBytes >= 4) Return(buffer.readFloat) else continue
    }
  }
}
