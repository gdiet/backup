package net.diet_rich.dedup.core

import java.io.{RandomAccessFile, File}
import java.util.concurrent.Executors

import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util._
import net.diet_rich.dedup.util.io.{RichFile, using}

import Source.RandomAccessFileSource

import scala.concurrent.{Future, ExecutionContext}

object BackupApp extends ConsoleApp {
  checkUsage("parameters: <repository path> <source:path> <target:path> [reference:path] [DEFLATE] [FASTREFERENCECHECK]")
  val source = new File(option("source:"))
  val target = Path(option("target:").withDateStringReplaced)
  val referencePath = optional("reference:") map Path.apply
  val storeMethod = if (options contains "DEFLATE") StoreMethod.DEFLATE else StoreMethod.STORE
  val ignorePrint = options contains "FASTREFERENCECHECK"
  require(source canRead, s"can't read source $source")

  Repository(repositoryDirectory, storeMethod, readonly = false) { filesystem =>
    val reference = referencePath flatMap filesystem.firstEntryWithWildcards
    if (referencePath.isDefined) {
      require (reference isDefined, s"reference $referencePath not found in file system.")
      reference foreach { referenceNode =>
        println(s"using reference ${filesystem path referenceNode.id}")
        if (source.isDirectory) {
          require(referenceNode.data isEmpty, "source is a directory, but reference is not")
          val sourceChildren = source.list().toSet
          val referenceChildren = filesystem.children(referenceNode.id).map(_.name).toSet
          println(s"matching source / reference entries on 1st level: ${sourceChildren & referenceChildren}")
          println(s"entries in source, not in reference on 1st level: ${sourceChildren &~ referenceChildren}")
          println(s"entries in reference, not in source on 1st level: ${referenceChildren &~ sourceChildren}")
        } else {
          require(referenceNode.data isDefined, "source is a file, but reference is not")
        }
      }
    }

    val parent = filesystem.entries(target parent).headOption getOrElse filesystem.createWithPath(target parent)
    val name = target.name

//    println("start backup? [y/n]")
//    require(scala.io.StdIn.readChar == 'y', "aborted")

    val readThreads = 4
    val executor = Executors.newFixedThreadPool(readThreads)
    implicit val executionContext = ExecutionContext fromExecutorService executor
    def wrapped(f: => Unit) = resultOf(Future(f))

    val time = System.currentTimeMillis()
    store(parent, name, source, reference)
    println(s"time: ${System.currentTimeMillis() - time} ms")
    executor.shutdown()

    // FIXME multi-threaded store?
    // FIXME store progress
    // FIXME stopping store (shutdown hook)
    def store(parent: TreeEntry, name: String, source: File, reference: Option[TreeEntry]): Unit = wrapped {
      if (source.isDirectory) {
        val newParent = filesystem.create(parent.id, name, Some(Time now))
        source.listFiles foreach { child =>
          val childReference = reference flatMap (reference => filesystem.children(reference id, child getName).headOption)
          store(newParent, child getName, child, childReference)
        }
      } else reference match {
        // check time stamp
        case Some(TreeEntry(_, _, _, Some(changed), Some(dataid), _)) if changed === source.changed =>
          filesystem dataEntry dataid match {
            // check size
            case Some(DataEntry(`dataid`, Size(length), print, hash, _)) if length === source.length =>
              // check print?
              if (ignorePrint) link(parent, name, source changed, dataid)
              else checkPrint(parent, name, source, dataid, print)
            case _ =>
              store(parent, name, source, None)
          }
        case _ =>
          using(new RandomAccessFile(source, "r").asSource) { sourceData =>
            filesystem.storeUnchecked(parent id, name, sourceData, source changed)
          }
      }
    }

    def checkPrint(parent: TreeEntry, name: String, source: File, dataid: DataEntryID, expectedPrint: Print): Unit =
      using(new RandomAccessFile(source, "r").asSource) { sourceData =>
        val header = sourceData read FileSystem.PRINTSIZE
        val print = Print(header)
        if (print === expectedPrint) link(parent, name, source changed, dataid)
        else {
          filesystem.storeUnchecked(parent id, name, header, print, sourceData, source changed)
        }
      }

    def link(parent: TreeEntry, name: String, time: Time, dataid: DataEntryID): Unit = {
      filesystem.create(parent id, name, Some(time), Some(dataid))
    }
  }
}
