package net.diet_rich

import net.diet_rich.util._

package object fs {
  type FSResult[Good] = Result[Good, FSProblem]

  import scala.language.implicitConversions
  implicit def fsProblemIsBad[Good, Problem <: FSProblem](problem: Problem): FSResult[Good] = Bad(problem)
}
