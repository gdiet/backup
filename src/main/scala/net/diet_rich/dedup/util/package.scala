package net.diet_rich.dedup

package object util {
  def init[T](t: T)(f: T => Unit): T = { f(t); t }
}