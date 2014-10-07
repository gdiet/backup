package net.diet_rich.dedup.core

import java.io.{RandomAccessFile, File}

import net.diet_rich.dedup.core.values._
import net.diet_rich.dedup.util.{init, Equal, ConsoleApp}
import net.diet_rich.dedup.util.io.{RichFile, using}

import Source.RandomAccessFileSource

object BackupApp extends ConsoleApp {
  checkUsage("parameters: <repository path> <source:path> <target:path> [reference:path] [DEFLATE] [FASTREFERENCECHECK]")
  val source = new File(option("source:"))
  val target = Path(option("target:"))
  val referencePath = optional("reference:") map Path.apply
  val storeMethod = if (options contains "DEFLATE") StoreMethod.DEFLATE else StoreMethod.STORE
  val ignorePrint = options contains "FASTREFERENCECHECK"
  require(source canRead, s"can't read source $source")

  Repository(repositoryDirectory, storeMethod, readonly = false) { filesystem =>
    val reference = referencePath flatMap (filesystem.entries(_).headOption)
    if (referencePath.isDefined) require (reference isDefined, s"reference $referencePath not found in file system.")
    val parent = filesystem.entries(target parent).headOption getOrElse filesystem.createWithPath(target parent)
    val name = target.name

    store(parent, name, source, reference)

    def store(parent: TreeEntry, name: String, source: File, reference: Option[TreeEntry]): Unit = {
      println(s"------ storing $parent $name $source $reference")
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
          println(s"****** storing $source")
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
          println(s"****** storing because of print mismatch: $source")
          filesystem.storeUnchecked(parent id, name, header, print, sourceData, source changed)
        }
      }

    def link(parent: TreeEntry, name: String, time: Time, dataid: DataEntryID): Unit = {
      println(s"****** linking $dataid to $source")
      filesystem.create(parent id, name, Some(time), Some(dataid))
    }
  }
}
