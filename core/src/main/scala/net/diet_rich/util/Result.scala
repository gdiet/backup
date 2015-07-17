package net.diet_rich.util

class Result[+Good, Bad] (val asEither: Either[Bad, Good]) extends AnyVal {
  def map[NewGood](f: Good => NewGood): Result[NewGood, Bad] = asEither.fold(Bad, f andThen Good)
  def flatMap[NewGood](f: Good => Result[NewGood, Bad]): Result[NewGood, Bad] = asEither fold (Bad, f)
  def fold[X](f: Good => X)(g: Bad => X) = asEither fold (g, f)
  def good: Good = asEither.fold(bad => throw new NoSuchElementException(s"The result is bad: $bad"), identity)
}
