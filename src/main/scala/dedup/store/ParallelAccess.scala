package dedup.store

import java.util.concurrent.locks.ReentrantLock

import scala.collection.mutable

/** This trait manages thread safe parallel access to the data files so that only a limited number
 *  of random access files is kept open, and that write access is only used where necessary.
 */
trait ParallelAccess[R] extends AutoCloseable {
  protected val parallelOpenResources = 5
  private val openResources: mutable.LinkedHashMap[String, (ReentrantLock, Boolean, R)] = mutable.LinkedHashMap()
  private val mapLock = new ReentrantLock()

  protected def openResource(path: String, forWrite: Boolean): R
  protected def closeResource(path: String, r: R): Unit

  @annotation.tailrec
  final def access[T](path: String, write: Boolean)(f: R => T): T = {
    mapLock.lock()
    openResources.get(path) match {
      case Some(entry @ (resourceLock, isForWrite, resource)) =>
        resourceLock.lock()
        if (!write || isForWrite) {
          // TODO maybe java LinkedHashMap is better? https://docs.oracle.com/javase/8/docs/api/java/util/LinkedHashMap.html#removeEldestEntry-java.util.Map.Entry-
          openResources.remove(path); openResources.put(path, entry) // remove and add for LRU functionality
          mapLock.unlock()
          f(resource).tap(_ => resourceLock.unlock())
        }
        else { closeResource(path, resource); openResources.remove(path); mapLock.unlock(); access(path, write)(f) }
      case None =>
        if (openResources.size < parallelOpenResources) {
          val resource -> resourceLock = openResource(path, write) -> new ReentrantLock().tap(_.lock())
          openResources.put(path, (resourceLock, write, resource))
          mapLock.unlock(); f(resource).tap(_ => resourceLock.unlock())
        } else {
          val (pathToClose, (_, _, resource)) = openResources
            .find { case (_, (fileLock, _, _)) =>  fileLock.tryLock() }
            .getOrElse { openResources.head.tap { case (_, (fileLock, _, _)) => fileLock.lock() } }
          closeResource(pathToClose, resource)
          openResources.remove(pathToClose)
          mapLock.unlock()
          access(path, write)(f)
        }
    }
  }
  
  def close(): Unit = {
    mapLock.lock()
    openResources.foreach { case (path, (fileLock, _, resource)) => fileLock.lock(); closeResource(path, resource) }
  }
}
