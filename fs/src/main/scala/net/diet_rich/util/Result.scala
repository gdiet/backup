package net.diet_rich.util

class Result[+Good, Bad] (val asEither: Either[Bad, Good]) extends AnyVal {
  def map[NewGood](f: Good => NewGood): Result[NewGood, Bad] = asEither.fold(Bad.apply, f andThen Good.apply)
  def flatMap[NewGood](f: Good => Result[NewGood, Bad]): Result[NewGood, Bad] = asEither fold (Bad.apply, f)
  def fold[X](f: Good => X)(g: Bad => X) = asEither fold (g, f)
  def good: Good = asEither.fold(bad => throw new NoSuchElementException(s"The result is bad: $bad"), identity)
}

object Good {
  def apply[Good, Bad](good: Good): Result[Good, Bad] = new Result[Good, Bad](Right(good))
  def unapply[Good, Bad](result: Result[Good, Bad]): Option[Good] =
    result.fold(Option(_))(_ => None)
}

object Bad {
  def apply[Good, Bad](bad: Bad): Result[Good, Bad] = new Result[Good, Bad](Left(bad))
  def unapply[Good, Bad](result: Result[Good, Bad]): Option[Bad] =
    result.fold(_ => Option.empty[Bad])(Option(_))
}
