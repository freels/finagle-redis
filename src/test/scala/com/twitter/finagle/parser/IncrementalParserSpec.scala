package com.twitter.finagle.parser.incremental

import org.specs.Specification
import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import com.twitter.finagle.ParseException
import com.twitter.finagle.parser.util.DecodingHelpers._
import com.twitter.finagle.parser.test._


object ParserSpec extends ParserSpecification {
  import Parsers._

  "DelimiterParser" in {
    val parser = readTo("\r\n") map asString

    parser mustParse "hello world\r\n" andReturn "hello world" leavingBytes(0)
    parser mustParse "one\r\ntwo\r\n" andReturn "one" readingBytes(5)
  }

  "FixedBytesParser" in {
    val parser = readBytes(513)
    val input  = ChannelBuffers.dynamicBuffer

    parser.decode(input) mustEqual Continue(parser)
    for (i <- 1 until FixedBytesParser.ChunkSize) {
      input.writeByte('x')
    }

    parser.decode(input) mustEqual Continue(parser)
    input.readerIndex    mustEqual 0

    input.writeByte('x')

    val Continue(next) = parser.decode(input)

    next must notBe(parser)
    input.readerIndex mustEqual FixedBytesParser.ChunkSize
  }

  // "ChainedParser" in {
  //   val first  = readByte
  //   val next   = readUnsignedByte
  //   val parser = new ChainedParser(first, next)

  //   parser mustParse ""   andContinue(parser)
  //   parser mustParse "x"  andContinue(next)
  //   parser mustParse "xx" andReturn()
  // }

  // "BacktrackingParser" in {
  //   val parser = guard("xx") { Parsers.fail(new ParseException("whoops")) }
  //   val backtracking = new BacktrackingParser(parser)

  //   parser       mustParse "xxy" andFail() readingBytes(2)
  //   backtracking mustParse "xxy" andFail() readingBytes(0)

  //   backtracking or const("foo") mustParse "xxy" andReturn("foo") readingBytes(0)
  // }

  "Parsers" in {
    "readTo" in {
      val parser = readTo("baz") map asString
      parser mustParse "foobazbarbaz" andReturn "foo" readingBytes(6)
      parser mustParse "foobar"       andContinue()
    }

    "readLine" in {
      readLine map asString mustParse "before\rstillstill"          andContinue()
      readLine map asString mustParse "before\rstillstill\r\nafter" leavingBytes(5)
    }

    "fail" in {
      val err = new ParseException("whoops")

      Parsers.fail(err) mustParse ""    andFail err readingBytes(0)
      Parsers.fail(err) mustParse "foo" andFail err readingBytes(0)
    }

    "success" in {
      success("as always") mustParse ""     andReturn "as always" readingBytes(0)
      success("as always") mustParse "blah" andReturn "as always" readingBytes(0)
    }

    "unit" in {
      unit mustParse ""     andReturn () readingBytes(0)
      unit mustParse "blah" andReturn () readingBytes(0)
    }

    "readBytes" in {
      readBytes(0) map asString mustParse ""  andReturn "" readingBytes(0)
      readBytes(0) map asString mustParse "a" andReturn "" readingBytes(0)

      for (i <- 0 to 6) {
        readBytes(i) map asString mustParse "aaaaaa" andReturn ("a" * i) readingBytes(i)
      }

      readBytes(7) map asString mustParse "aaaaaa" andContinue()
    }

    "skipBytes" in {
      skipBytes(2) mustParse "abc" andReturn () readingBytes(2)
      skipBytes(4) mustParse "abc" andContinue()
    }

    "accept" in {
      val parser = Parsers.accept("$") append (readBytes(1) map asString)

      parser mustParse "#aa" andFail()     readingBytes(0)
      parser mustParse "$aa" andReturn("a") readingBytes(2)
    }

    "choice" in {
      val parser = choice(
        "a"   -> success("first"),
        "bc"  -> success("second"),
        "def" -> success("third")
      )

      parser mustParse "abcdef" andReturn "first"  readingBytes(1)
      parser mustParse "bcdef"  andReturn "second" readingBytes(2)
      parser mustParse "def"    andReturn "third"  readingBytes(3)

      parser mustParse "xxx" andFail() readingBytes(0)
    }

    "repN" in {
      val parser = repN(2, readLine map asString)
      parser mustParse "one\r\ntwo\r\n" andReturn Seq("one", "two")
    }

    "readByte" in {
      readByte mustParse "xy" andReturn 'x' readingBytes(1)
      readByte mustParse "" andContinue()
    }

    "readShort" in {
      val input = ChannelBuffers.dynamicBuffer
      input.writeShort(27)

      readShort mustParse input andReturn 27 readingBytes(2)
      readShort mustParse ""  andContinue()
      readShort mustParse "b" andContinue()
    }

    "readMedium" in {
      val input = ChannelBuffers.dynamicBuffer
      input.writeMedium(27)

      readMedium mustParse input andReturn 27 readingBytes(3)
      readMedium mustParse ""   andContinue()
      readMedium mustParse "by" andContinue()
    }

    "readInt" in {
      val input = ChannelBuffers.dynamicBuffer
      input.writeInt(27)

      readInt mustParse input andReturn 27 readingBytes(4)
      readInt mustParse ""    andContinue()
      readInt mustParse "byt" andContinue()
    }

    "readLong" in {
      val input = ChannelBuffers.dynamicBuffer
      input.writeLong(27)

      readLong mustParse input andReturn 27 readingBytes(8)
      readLong mustParse ""        andContinue()
      readLong mustParse "bytebyt" andContinue()
    }

    "readUnsignedByte" in {
      val input = ChannelBuffers.dynamicBuffer
      input.writeByte(-1)

      readUnsignedByte mustParse input andReturn 255 readingBytes(1)
      readUnsignedByte mustParse "" andContinue()
    }

    "readUnsignedShort" in {
      val input = ChannelBuffers.dynamicBuffer
      input.writeShort(-1)

      readUnsignedShort mustParse input andReturn 65535 readingBytes(2)
      readUnsignedShort mustParse ""  andContinue()
      readUnsignedShort mustParse "b" andContinue()
    }

    "readUnsignedMedium" in {
      val input = ChannelBuffers.dynamicBuffer
      input.writeMedium(-1)

      readUnsignedMedium mustParse input andReturn 16777215 readingBytes(3)
      readUnsignedMedium mustParse ""   andContinue()
      readUnsignedMedium mustParse "by" andContinue()
    }

    "readUnsignedInt" in {
      val input = ChannelBuffers.dynamicBuffer
      input.writeInt(-1)

      readUnsignedInt mustParse input andReturn 4294967295L readingBytes(4)
      readUnsignedInt mustParse ""    andContinue()
      readUnsignedInt mustParse "byt" andContinue()
    }

    "readChar" in {
      val input = ChannelBuffers.dynamicBuffer
      input.writeChar('a')

      readChar mustParse input andReturn 'a' readingBytes(2)
      readChar mustParse ""  andContinue()
      readChar mustParse "b" andContinue()
    }

    "readDouble" in {
      val input = ChannelBuffers.dynamicBuffer
      input.writeDouble(300.0)

      readDouble mustParse input andReturn 300.0 readingBytes(8)
      readDouble mustParse ""        andContinue()
      readDouble mustParse "bytebyt" andContinue()
    }

    "readFloat" in {
      val input = ChannelBuffers.dynamicBuffer
      input.writeFloat(300.0F)

      readFloat mustParse input andReturn 300.0F readingBytes(4)
      readFloat mustParse ""    andContinue()
      readFloat mustParse "byt" andContinue()
    }
  }

  def time[T](f: => T) = {
    val s = System.currentTimeMillis
    f
    val e = System.currentTimeMillis
    e - s
  }

  "performance" in {
    val readInt = readLine map { bytes => decodeDecimalInt(bytes) }
    val readBulk = Parsers.accept("$") append (readInt into { length =>
      readBytes(length) into { bytes =>
        readBytes(2) append success(bytes)
      }
    })

    val test1 = Parsers.accept("*") append (readInt into { count =>
      repN(count, readBulk)
    })

    val count = 100
    val buf1 = ChannelBuffers.wrappedBuffer(("*"+count+"\r\n" + ("$6\r\nfoobar\r\n" * count)).getBytes)

    println(test1.decode(buf1))

    for (x <- 1 to 100) {
      val rv = time { for (i <- 1 to 100000) {
        buf1.resetReaderIndex
        test1.decode(buf1)
      } }

      println("test 1: "+ rv +" ("+ (rv / 100000.0) +")")
    }
  }
}
