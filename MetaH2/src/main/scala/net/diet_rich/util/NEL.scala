package net.diet_rich.util

/** Non-empty list */
case class NEL[A](head: A, tail: List[A] = Nil) {
  def toList: List[A] = head :: tail
  def ::[B >: A] (x: B): NEL[B] = NEL(x, toList)
}