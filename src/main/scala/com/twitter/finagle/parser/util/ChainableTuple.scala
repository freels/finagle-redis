package com.twitter.finagle.parser.util


protected trait ChainableTupleLowImplicits {
  implicit def refAsChainable[T](t: T): ChainableTuple1[T] = {
    new ChainableTuple1[T](Tuple1(t))
  }
}

object ChainableTuple extends ChainableTupleLowImplicits {
  implicit def asChainable1[T1](t: Tuple1[T1]) = {
    new ChainableTuple1(t)
  }

  implicit def asChainable2[T1,T2](t: Tuple2[T1,T2]) = {
    new ChainableTuple2(t)
  }

  implicit def asChainable3[T1,T2,T3](t: Tuple3[T1,T2,T3]) = {
    new ChainableTuple3(t)
  }

  implicit def asChainable4[T1,T2,T3,T4](t: Tuple4[T1,T2,T3,T4]) = {
    new ChainableTuple4(t)
  }

  implicit def asChainable5[T1,T2,T3,T4,T5](t: Tuple5[T1,T2,T3,T4,T5]) = {
    new ChainableTuple5(t)
  }

  implicit def asChainable6[T1,T2,T3,T4,T5,T6](t: Tuple6[T1,T2,T3,T4,T5,T6]) = {
    new ChainableTuple6(t)
  }

  implicit def asChainable7[T1,T2,T3,T4,T5,T6,T7](t: Tuple7[T1,T2,T3,T4,T5,T6,T7]) = {
    new ChainableTuple7(t)
  }

  implicit def asChainable8[T1,T2,T3,T4,T5,T6,T7,T8](t: Tuple8[T1,T2,T3,T4,T5,T6,T7,T8]) = {
    new ChainableTuple8(t)
  }

  implicit def asChainable9[T1,T2,T3,T4,T5,T6,T7,T8,T9](t: Tuple9[T1,T2,T3,T4,T5,T6,T7,T8,T9]) = {
    new ChainableTuple9(t)
  }
}

sealed abstract class ChainableTuple {
  type Prepend[N] <: Product
  type Append[N]  <: Product
  type Next[N] = Append[N]

  def prepend[N](n: N): Prepend[N]
  def append[N](n: N): Append[N]

  def +:[N](n: N) = prepend(n)
  def :+[N](n: N) = append(n)
}

class ChainableTuple1[T1](t: Tuple1[T1])
extends ChainableTuple {
  type Prepend[N] = Tuple2[N,T1]
  type Append[N]  = Tuple2[T1,N]

  def prepend[N](n: N) = (n,t._1)
  def append[N](n: N)  = (t._1,n)
}

class ChainableTuple2[T1,T2](t: Tuple2[T1,T2])
extends ChainableTuple {
  type Prepend[N] = Tuple3[N,T1,T2]
  type Append[N]  = Tuple3[T1,T2,N]

  def prepend[N](n: N) = (n,t._1,t._2)
  def append[N](n: N)  = (t._1,t._2,n)
}

class ChainableTuple3[T1,T2,T3](t: Tuple3[T1,T2,T3])
extends ChainableTuple {
  type Prepend[N] = Tuple4[N,T1,T2,T3]
  type Append[N]  = Tuple4[T1,T2,T3,N]

  def prepend[N](n: N) = (n,t._1,t._2,t._3)
  def append[N](n: N)  = (t._1,t._2,t._3,n)
}

class ChainableTuple4[T1,T2,T3,T4](t: Tuple4[T1,T2,T3,T4])
extends ChainableTuple {
  type Prepend[N] = Tuple5[N,T1,T2,T3,T4]
  type Append[N]  = Tuple5[T1,T2,T3,T4,N]

  def prepend[N](n: N) = (n,t._1,t._2,t._3,t._4)
  def append[N](n: N)  = (t._1,t._2,t._3,t._4,n)
}

class ChainableTuple5[T1,T2,T3,T4,T5](t: Tuple5[T1,T2,T3,T4,T5])
extends ChainableTuple {
  type Prepend[N] = Tuple6[N,T1,T2,T3,T4,T5]
  type Append[N]  = Tuple6[T1,T2,T3,T4,T5,N]

  def prepend[N](n: N) = (n,t._1,t._2,t._3,t._4,t._5)
  def append[N](n: N)  = (t._1,t._2,t._3,t._4,t._5,n)
}

class ChainableTuple6[T1,T2,T3,T4,T5,T6](t: Tuple6[T1,T2,T3,T4,T5,T6])
extends ChainableTuple {
  type Prepend[N] = Tuple7[N,T1,T2,T3,T4,T5,T6]
  type Append[N]  = Tuple7[T1,T2,T3,T4,T5,T6,N]

  def prepend[N](n: N) = (n,t._1,t._2,t._3,t._4,t._5,t._6)
  def append[N](n: N)  = (t._1,t._2,t._3,t._4,t._5,t._6,n)
}

class ChainableTuple7[T1,T2,T3,T4,T5,T6,T7](t: Tuple7[T1,T2,T3,T4,T5,T6,T7])
extends ChainableTuple {
  type Prepend[N] = Tuple8[N,T1,T2,T3,T4,T5,T6,T7]
  type Append[N]  = Tuple8[T1,T2,T3,T4,T5,T6,T7,N]

  def prepend[N](n: N) = (n,t._1,t._2,t._3,t._4,t._5,t._6,t._7)
  def append[N](n: N)  = (t._1,t._2,t._3,t._4,t._5,t._6,t._7,n)
}

class ChainableTuple8[T1,T2,T3,T4,T5,T6,T7,T8](t: Tuple8[T1,T2,T3,T4,T5,T6,T7,T8])
extends ChainableTuple {
  type Prepend[N] = Tuple9[N,T1,T2,T3,T4,T5,T6,T7,T8]
  type Append[N]  = Tuple9[T1,T2,T3,T4,T5,T6,T7,T8,N]

  def prepend[N](n: N) = (n,t._1,t._2,t._3,t._4,t._5,t._6,t._7,t._8)
  def append[N](n: N)  = (t._1,t._2,t._3,t._4,t._5,t._6,t._7,t._8,n)
}

class ChainableTuple9[T1,T2,T3,T4,T5,T6,T7,T8,T9](t: Tuple9[T1,T2,T3,T4,T5,T6,T7,T8,T9])
extends ChainableTuple {
  type Prepend[N] = Tuple10[N,T1,T2,T3,T4,T5,T6,T7,T8,T9]
  type Append[N]  = Tuple10[T1,T2,T3,T4,T5,T6,T7,T8,T9,N]

  def prepend[N](n: N) = (n,t._1,t._2,t._3,t._4,t._5,t._6,t._7,t._8,t._9)
  def append[N](n: N)  = (t._1,t._2,t._3,t._4,t._5,t._6,t._7,t._8,t._9,n)
}
