package com.twitter.finagle.parser.incremental

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBufferIndexFinder, ChannelBuffer}
import com.twitter.finagle.parser.util._


object Parsers {

  // lifting values

  def fail(message: String) = new LiftParser(Fail(message))

  def error(message: String) = new LiftParser(Error(message))

  def success[T](t: T) = new LiftParser(Return(t))

  val unit = success(())

  def liftOpt[T](o: Option[T]): Parser[T] = o match {
    case Some(r) => success(r)
    case None    => fail("Parse failed.")
  }


  // behavior transformation

  def opt[T](p: Parser[T]): Parser[Option[T]] = p map { Some(_) } or success(None)

  def attempt[T](p: Parser[T]) = new BacktrackingParser(p)


  // repetition

  def rep[T](p: Parser[T]): Parser[List[T]] = {
    val optP = opt(p)

    def go(prev: List[T]): Parser[List[T]] = optP into {
      case Some(t) => go(t :: prev)
      case None    => success(prev)
    }

    go(Nil) map { _.reverse }
  }

  def rep1[T](p: Parser[T], q: Parser[T]): Parser[List[T]] = {
    val getRest = rep(q)

    for (head <- p; tail <- getRest) yield (head :: tail)
  }

  def rep1[T](p: Parser[T]): Parser[List[T]] = {
    rep1(p, p)
  }

  def rep1sep[T](p: Parser[T], sep: Parser[Any]): Parser[List[T]] = {
    val optSep = sep then success(true) or success(false)

    def go(prev: List[T]): Parser[List[T]] = {
      p into { t =>
        optSep into {
          case true  => go(t :: prev)
          case false => success(t :: prev)
        }
      }
    }

    go(Nil) map { _.reverse }
  }

  def repsep[T](p: Parser[T], sep: Parser[Any]): Parser[List[T]] = {
    rep1sep(p, sep) or success[List[T]](Nil)
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


  // matching parsers

  def accept(m: Matcher) = new MatchParser(m) into readBytes

  implicit def accept(choice: String): Parser[ChannelBuffer] = {
    accept(new DelimiterMatcher(choice))
  }

  def accept(choices: String*): Parser[ChannelBuffer] = {
    accept(AlternateMatcher(choices))
  }

  def guard(m: Matcher) = new MatchParser(m)

  def guard(choice: String): Parser[Int] = {
    guard(new DelimiterMatcher(choice))
  }

  def guard(choices: String*): Parser[Int] = {
    guard(AlternateMatcher(choices))
  }

  def not(m: Matcher) = guard(m.negate) then unit

  def not(choice: String): Parser[Unit] = {
    not(new DelimiterMatcher(choice))
  }

  def not(choices: String*): Parser[Unit] = {
    not(AlternateMatcher(choices))
  }


  def choice[T](choices: (String, Parser[T])*): Parser[T] = {
    val (m, p)           = choices.head
    val first: Parser[T] = accept(m) then p
    val rest             = choices.tail

    if (rest.isEmpty) first else first or choice(rest: _*)
  }


  // Delimiter parsers

  def readTo(m: Matcher) = new ConsumingDelimiterParser(m)

  def readTo(choice: String): Parser[ChannelBuffer] = {
    readTo(new DelimiterMatcher(choice))
  }

  def readTo(choices: String*): Parser[ChannelBuffer] = {
    readTo(AlternateMatcher(choices))
  }

  def readUntil(m: Matcher) = new DelimiterParser(m)

  def readUntil(choice: String): Parser[ChannelBuffer] = {
    readUntil(new DelimiterMatcher(choice))
  }

  def readUntil(choices: String*): Parser[ChannelBuffer] = {
    readUntil(AlternateMatcher(choices))
  }

  def readWhile(m: Matcher) = readUntil(new NotMatcher(m))

  def readWhile(choice: String): Parser[ChannelBuffer] = {
    readWhile(new DelimiterMatcher(choice))
  }

  def readWhile(choices: String*): Parser[ChannelBuffer] = {
    readWhile(AlternateMatcher(choices))
  }

  val readLine = readTo(Matchers.CRLF)
  val readWord = readUntil(Matchers.WhiteSpace)


  // basic reading parsers

  def readBytes(size: Int) = new BytesParser(size)

  def skipBytes(size: Int) = new SkipBytesParser(size)

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
