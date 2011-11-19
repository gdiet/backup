package net.diet_rich.util

object Choice {

  def nullIsNone[T,U](t: T)(f: T => U) : Option[U] = if (t == null) None else Some(f(t))
  def nullIsLeft[T,U,V](t: T, u: U)(f: T => V) : Either[U,V] = if (t == null) Left(u) else Right(f(t))
  
}