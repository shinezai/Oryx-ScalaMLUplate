package com.shinezai.oryx.utils

import java.util.function.Function

/**
  * Created by wuxuan on 2017/7/19 0019.
  */
object ScalaAndJavaFuncTrans {
 implicit def toJavaFunction[U, V](f:Function1[U,V]): Function[U, V] = new Function[U, V] {
    override def apply(t: U): V = f(t)

    override def compose[T](before:Function[_ >: T, _ <: U]):
    Function[T, V] = toJavaFunction(f.compose(x => before.apply(x)))

    override def andThen[W](after:Function[_ >: V, _ <: W]):
    Function[U, W] = toJavaFunction(f.andThen(x => after.apply(x)))
  }

 implicit def fromJavaFunction[U, V](f:Function[U,V]): Function1[U, V] = f.apply

}
