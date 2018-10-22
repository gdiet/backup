package net.diet_rich.util

/** Non-empty list */
case class Nel[A](head: A, tail: List[A] = Nil) {
  def toList: List[A] = head :: tail
  def ::[B >: A] (x: B): Nel[B] = Nel(x, toList)
}
object Head { self =>
  def unapply[A](nel: Nel[A]): Option[A] = Some(nel.head)
}
