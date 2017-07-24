package net.diet_rich

package object common {
  def init[T](t: T)(f: T => Unit): T = { f(t); t }
}
