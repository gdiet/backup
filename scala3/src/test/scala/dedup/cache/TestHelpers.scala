package dedup
package cache

import scala.reflect._

def °[T: ClassTag]: String = classTag[T].runtimeClass.getTypeName
