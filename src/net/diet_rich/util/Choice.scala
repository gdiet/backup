package net.diet_rich.util

object Choice {

  def nullIsNone[T](t: T): Option[T] = if (t == null) None else Some(t)
  def nullIsLeft[T,U](t: T, u: U): Either[T,U] = if (u == null) Left(t) else Right(u)
  def mapRight[T,U,V](either: Either[T,U])(f: U => V): Either[T,V] = either.fold(Left(_), x => Right(f(x))) 

//  EVENTUALLY decide whether to use - is right associative!
//  implicit def nullIsNone_!:[T](t: T) = new {
//    def !:() = if (t == null) None else Some(t)
//  }
  
}