package net.diet_rich.bytestore

package object file {
  /** Note: This function is designed only for positive values. */
  def min(s1: IntSize, s2: Size, s3: Size): IntSize = IntSize(math.min(s1.value, math.min(s2.value, s3.value)).toInt)
  /** Note: This function is designed only for positive values. */
  def min(s1: IntSize, s2: Long): Int = math.min(s1.value, s2).toInt

  def printOf(string: String): Print = Print(net.diet_rich.common printOf string) // TODO define Print in Common
}