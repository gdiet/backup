package net.diet_rich

package object util {
  def Good[Good, Bad](good: Good) = new Result[Good, Bad](Right(good))
  def Bad[Good, Bad](bad: Bad) = new Result[Good, Bad](Left(bad))
}
