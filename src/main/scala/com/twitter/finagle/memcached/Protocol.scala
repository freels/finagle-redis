package com.twitter.finagle.memcached_test

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.Codec
import com.twitter.finagle.parser.incremental._
import com.twitter.finagle.parser.incremental.Parsers._
import com.twitter.finagle.parser.util.DecodingHelpers._
import com.twitter.finagle.parser.util.Matchers._


sealed abstract class Response
case object NotFound  extends Response
case object Exists    extends Response
case object Stored    extends Response
case object NotStored extends Response
case object Deleted   extends Response

abstract class Results[+T] extends Response {
  def items: Seq[T]
}

case class Values(items: Seq[Value]) extends Results[Value]
case class Stats(items: Seq[Stat]) extends Results[Stat]
case object EmptyResult extends Results[Nothing] {
  def items = Nil
}

case class Number(value: Long)       extends Response

case class Error(cause: String)     extends Response
case class Version(version: String) extends Response

case class Value(key: String, value: ChannelBuffer)
case class Stat(name: String, value: ChannelBuffer)


object ResponseDecoder {

  val readError = {
    val readCause = readLine map { b => Error(b.toString("UTF-8")) }

    choice(
      "ERROR\r\n"     -> success(Error("")),
      "SERVER_ERROR " -> readCause,
      "CLIENT_ERROR " -> readCause
    )
  }

  val readValues = {
    val readKey   = readTo(WhiteSpace) map { decodeUTF8String(_) }
    val readFlags = readTo(WhiteSpace) into { b =>
      liftOpt(decodeDecimalInt(b)) } map { decodeFlags(_)
    }

    val readLength = readUntil(WhiteSpace) into { b => liftOpt(decodeDecimalInt(b)) }
    val readCas = choice(
      " "    -> (readLine map { cas => Some(decodeDecimalInt(cas)) }),
      "\r\n" -> success(None)
    )

    val skipCRLF  = skipBytes(2)

    val readValue = "VALUE " then readKey into { key =>
      readFlags then readLength into { length =>
        readCas then readBytes(length) through skipCRLF map { data =>
          Value(key, data)
        }
      }
    }

    rep1(readValue) map { Values(_) } through accept("END\r\n")
  }

  val readStats = {
    val readName = readTo(WhiteSpace) map { decodeUTF8String(_) }

    val readStat = "STAT " then readName into { name =>
      readLine map { value => Stat(name, value) }
    }

    (rep1(readStat) map { Stats(_) }) through "END\r\n"
  }

  val readStorageResponse = choice(
    "STORED\r\n"     -> success(Stored),
    "NOT_STORED\r\n" -> success(NotStored),
    "DELETED\r\n"    -> success(Deleted),
    "NOT_FOUND\r\n"  -> success(NotFound),
    "EXISTS\r\n"     -> success(Exists)
  )

  val readEmptyResult = "END\r\n" then EmptyResult

  val readNumber = {
    val decimalChars = "1234567890-+".split("").tail // drop empty string
    guard(decimalChars: _*) then readLine into { bytes =>
      liftOpt(decodeDecimalInt(bytes)) map { Number(_) }
    }
  }

  val readVersion = readLine map { v => Version(decodeUTF8String(v)) }


  // all together now

  val parser: Parser[Response] = {
    readValues or
    readEmptyResult or
    readStorageResponse or
    readError or
    readStats or
    readVersion or
    readNumber
  }
}

class ResponseDecoder extends ParserDecoder[Response](ResponseDecoder.parser)

// class Memcached extends Codec[Command, Response] {
//   def pipelineFactory = new ChannelPipelineFactory {
//     def getPipeline() = {
//       val pipeline = Channels.pipeline()

//       pipeline
//     }
//   }
// }
