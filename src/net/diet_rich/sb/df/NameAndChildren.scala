package net.diet_rich.sb.df

import scala.collection.immutable.Iterable

case class NameAndChildren(name: String, children: Iterable[IdAndName])