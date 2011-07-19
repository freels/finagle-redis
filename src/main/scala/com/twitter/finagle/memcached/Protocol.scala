package com.twitter.finagle.redis.test.memcached

import org.jboss.netty.channel._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.finagle.Codec
import com.twitter.finagle.parser.incremental._
import com.twitter.finagle.parser.util.DecodingHelpers._


sealed abstract class Response
case class NotFound()                     extends Response
case class Exists()                       extends Response
case class Stored()                       extends Response
case class NotStored()                    extends Response
case class Deleted()                      extends Response
case class Error(cause: String)    extends Response

case class Values(values: Seq[Value]) extends Response
case class Stats(stats: Seq[Stat]) extends Response
case class EmptyResult()                  extends Response
case class Number(value: Long)         extends Response
case class Version(version: ChannelBuffer) extends Response


case class Value(key: ChannelBuffer, value: ChannelBuffer)
case class Stat(name: ChannelBuffer, value: ChannelBuffer)

object ResponseDecoder {
  import Parsers._

  val skipSpace = readByte

  val readErrorCause = readLine map { bytes =>
    Error(bytes.toString("UTF-8"))
  }

  val readValue = {
    val readKey   = readTo(" ") // map { decodeUTF8String(_) }
    val readFlags = readTo(" ") map { bytes =>
      decodeFlags(decodeDecimalInt(bytes))
    }
    val readLength = readUntil(" ", "\r\n") map { decodeDecimalInt(_) }
    val readCas = choice(
      " "    -> (readLine map { cas => Some(decodeDecimalInt(cas)) }),
      "\r\n" -> success(None)
    )

    for {
      key    <- readKey
      flags  <- readFlags
      length <- readLength
      casId  <- readCas
      data   <- readBytes(length)
    } yield Value(key, data)
  }

  val readValues = {
    val readRest = repsep(accept("VALUE ") append readValue, not(guard("END\r\n")))

    readValue flatMap { first =>
      readRest map { rest => Values(first :: rest) }
    }
  }

  val readStat = readTo(" ") flatMap { name =>
    readLine map { value => Stat(name, value) }
  }

  val readStats = {
    val readRest = repsep(accept("STAT ") append readStat, not(guard("END\r\n")))

    readStat flatMap { first =>
      readRest map { rest => Stats(first :: rest) }
    }
  }

  val readVersion = readLine map { Version(_) }

  val readResponse = choice(
    // errors
    "ERROR\r\n"     -> success(Error("")),
    "SERVER_ERROR " -> readErrorCause,
    "CLIENT_ERROR " -> readErrorCause,

    // storage responses
    "STORED\r\n"     -> success(Stored()),
    "NOT_STORED\r\n" -> success(NotStored()),
    "DELETED\r\n"    -> success(Deleted()),
    "NOT_FOUND\r\n"  -> success(NotFound()),
    "EXISTS\r\n"     -> success(Exists()),

    // retrieval responses
    "VALUE "   -> readValues,
    "STAT "    -> readStats,
    "END\r\n"  -> success(EmptyResult()),
    "VERSION " -> readVersion
  )

  val readNumber = readLine map { bytes => Number(decodeDecimalInt(bytes)) }

  val parser: Parser[Response] = readResponse or readNumber
}


// class Memcached extends Codec[Command, Response] {
//   def pipelineFactory = new ChannelPipelineFactory {
//     def getPipeline() = {
//       val pipeline = Channels.pipeline()

//       pipeline
//     }
//   }
// }
