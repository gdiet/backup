package dedup
package cache

import scala.reflect._

def °[T: ClassTag]: String = classTag[T].runtimeClass.getTypeName

extension(l: LazyList[(Long, Either[Long, Array[Byte]])])
  def _seq = l.map { case (pos, Right(data)) => pos -> Right(data.toSeq); case other => other }
