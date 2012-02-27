package com.twitter.finagle.parser.incremental

import scala.annotation.tailrec
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.parser.util.ChainableTuple

object ParseState {
  trait StateProcessor
  final object EmptyState extends StateProcessor
  final object FailState  extends StateProcessor
  final object ErrorState extends StateProcessor
  final object ContState extends StateProcessor
  final object RetState extends StateProcessor
}

import ParseState._

final class ParseState {

  var _type: StateProcessor = EmptyState
  var _parser: Parser[_]    = _
  var _value: Any           = _
  var _msg: String          = _

  @inline def cont(p: Parser[_]) { _type = ContState;  _parser = p }
  @inline def ret(r: Any)        { _type = RetState;   _value  = r }
  @inline def fail(msg: String)  { _type = FailState;  _msg    = msg }
  @inline def error(msg: String) { _type = ErrorState; _msg    = msg }

  @inline def isCont  = _type == ContState
  @inline def isRet   = _type == RetState
  @inline def isFail  = _type == FailState
  @inline def isError = _type == ErrorState

  def value[T]      = _value.asInstanceOf[T]
  def nextParser[T] = _parser.asInstanceOf[Parser[T]]
  def errorMessage  = _msg

  def toResult[T]: ParseResult[T] = _type match {
    case RetState   => Return(_value.asInstanceOf[T])
    case ContState  => Continue(_parser.asInstanceOf[Parser[T]])
    case FailState  => Fail(_msg)
    case ErrorState => Error(_msg)
    case EmptyState => sys.error("empty state")
  }
}

abstract class Parser[+Out] {
  import Parsers._

  def decode(buffer: ChannelBuffer): ParseResult[Out] = {
    Return(decodeRaw(buffer))
  }

  def decodeRaw(buffer: ChannelBuffer): Out


  // basic composition

  def then[T](rhs: Parser[T]): Parser[T] = new ThenParser(this, rhs)

  def then[@specialized T](rv: T): Parser[T] = new ThenParser(this, success(rv))

  def through[T](rhs: Parser[T]): Parser[Out] = this flatMap { rhs then success(_) }

  def and[T, C <: ChainableTuple](rhs: Parser[T])(implicit chn: Out => C): Parser[C#Next[T]] = {
    for (tup <- this; next <- rhs) yield chn(tup).append(next)
  }

  def or[O >: Out](rhs: Parser[O]): Parser[O] = new OrParser(this, rhs)

  def flatMap[T](f: Out => Parser[T]): Parser[T] = new FlatMapParser(this, f)

  def map[@specialized T](f: Out => T): Parser[T] = this flatMap { o => success(f(o)) }


  // yay operators...this may be a bad idea.

  def * = rep(this)

  def + = rep1(this)

  def ? = opt(this)

  def <<[T](rhs: Parser[T]) = this through rhs

  def >>[T](rhs: Parser[T]) = this then rhs

  def >>=[T](f: Out => Parser[T]) = this flatMap f

  def ^[T](r: T) = this then r

  def ^^[T](f: Out => T) = this map f

  def |[T](rhs: Parser[T]) = this or rhs

  def &[T, C <: ChainableTuple](rhs: Parser[T])(implicit c: Out => C): Parser[C#Next[T]] = {
    this and rhs
  }

}

final class SuccessParser[@specialized +Out](rv: Out) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer) = rv
}

final class LiftParser[@specialized +Out](r: ParseResult[Out]) extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer) = r match {
    case Return(ret) => ret
  }
}

final class FlatMapParser[@specialized T, @specialized +Out](parser: Parser[T], f: T => Parser[Out])
extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    val next = try f(parser.decodeRaw(buffer)) catch {
      case e => println(e.getMessage); throw e
    }

    try next.decodeRaw(buffer) catch {
      case e => println(e.getMessage); throw e
    }
  }
}

final class ThenParser[@specialized +Out](parser: Parser[_], next: Parser[Out])
extends Parser[Out] {
  def decodeRaw(buffer: ChannelBuffer): Out = {
    parser.decodeRaw(buffer)
    next.decodeRaw(buffer)
  }

  override def then[T](other: Parser[T]): Parser[T] = {
    new ThenParser(parser, next then other)
  }
}


final class RepeatParser[+Out](
  parser: Parser[Out],
  count: Int,
  prevResult: Array[Any] = null,
  currParser: Parser[Out] = null
) extends Parser[Seq[Out]] {

  def decodeRaw(buffer: ChannelBuffer): Seq[Out] = {
    var result = new Array[Any](count)

    var i = 0
    while (i < count) {
      result(i) = parser.decodeRaw(buffer)
      i += 1
    }

    result.toSeq.asInstanceOf[Seq[Out]]

    // var left   = count
    // var result = if (prevResult eq null) new Array[Any](left) else prevResult
    // val p      = if (currParser eq null) parser else currParser
    // val total  = result.size

    // do {
    //   p.decodeWithState(state, buffer)

    //   if (state.isRet) {
    //     result(total - left) = state.value[Any]
    //     left -= 1
    //   } else if (state.isCont) {
    //     state.cont(new RepeatParser(parser, left, result, currParser))
    //     return
    //   }
    // } while (left > 0)

    // state.ret(result.toSeq)
  }
}

final class OrParser[+Out](choice: Parser[Out], tail: Parser[Out], committed: Boolean)
extends Parser[Out] {

  def this(p: Parser[Out], t: Parser[Out]) = this(p, t, false)

  def decodeRaw(buffer: ChannelBuffer): Out = {
    sys.error("Not implemented")
    // val start  = buffer.readerIndex
    // choice.decodeWithState(state, buffer)
    // val newCommitted = committed || buffer.readerIndex > start

    // if (state.isCont) {
    //   state.cont(new OrParser(state.nextParser, tail, newCommitted))
    // } else if (state.isFail) {
    //   if (newCommitted) {
    //     state.error(state.errorMessage)
    //   } else {
    //     tail.decodeWithState(state, buffer)
    //   }
    // }
  }

  override def or[O >: Out](other: Parser[O]): Parser[O] = {
    new OrParser(choice, tail or other)
  }
}

final class NotParser(parser: Parser[_]) extends Parser[Unit] {

  def decodeRaw(buffer: ChannelBuffer) = {
    sys.error("Not implemented")
    // val start     = buffer.readerIndex
    // parser.decodeWithState(state, buffer)
    // val committed = buffer.readerIndex > start

    // if (state.isCont) {
    //   if (committed) {
    //     state.error("Expected "+ parser +" to fail, but already consumed data.")
    //   } else {
    //     state.cont(new NotParser(state.nextParser))
    //   }
    // } else if (state.isRet) {
    //   if (committed) {
    //     state.error("Expected "+ parser +" to fail, but already consumed data.")
    //   } else {
    //     state.fail("Expected "+ parser +" to fail.")
    //   }
    // } else if (state.isFail) {
    //   if (committed) {
    //     state.error(state.errorMessage)
    //   } else {
    //     state.ret(())
    //   }
    // }
  }
}
