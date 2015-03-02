package net.diet_rich.dedup.core

import java.io.{FileOutputStream, File}

import net.diet_rich.dedup.core.data.StoreMethod
import net.diet_rich.dedup.core.meta.TreeEntry
import net.diet_rich.dedup.util.io._

object Main extends App {
  if (args.length < 2) println("arguments: <command> <repository> [key:value options]") else {
    val command :: repoPath :: options = args.toList

    def optional(key: String): Option[String] = options find (_ startsWith s"$key:") map (_ substring key.length+1)
    def required(key: String): String = optional(key) getOrElse (throw new IllegalArgumentException(s"option '$key' is mandatory"))
    def intOptional(key: String) = optional(key) map (_ toInt)

    command match {
      case "create" =>
        Repository.create(
          new File(repoPath),
          optional("repositoryid"),
          optional("hashAlgorithm"),
          intOptional("storeBlockSize")
        )

      case "backup" =>
        val source = new File(required("source"))
        val target = required("target")
        val threads = intOptional("storeThreads") // FIXME rename to parallel (everywhere)
        val storeMethod = optional("storeMethod") map StoreMethod.named
        using(Repository open (new File(repoPath), readonly = false, storeMethod, threads)) { repository =>
          using(new BackupAlgorithm(repository, threads)) { backupAlgorithm =>
            backupAlgorithm.backup(source, target)
          }
        }

      case "restore" =>
        val source = required("source")
        val target = new File(required("target"))
        require(target.isDirectory, s"Target $target must be a directory")
        require(target.listFiles.isEmpty, s"Target directory $target must be empty")
        using(Repository open (new File(repoPath), readonly = true)) { repository =>
          // FIXME move to RestoreAlgorithm and introduce parallel option
          val metaBackend = repository.metaBackend
          metaBackend.entries(source) match {
            case List(entry) =>
              def restore(target: File, entry: TreeEntry): Unit = {
                val file = target / entry.name
                (metaBackend children entry.id, entry.data) match {
                  case (Nil, Some(dataid)) =>
                    using(new FileOutputStream(file)) { out =>
                      repository read dataid foreach { b => out.write(b.data, b.offset, b.length) }
                    }
                    entry.changed foreach file.setLastModified
                  case (children, data) =>
                    if (data isDefined) warn(s"Data entry for directory $file is ignored")
                    if (file.mkdir()) children foreach (restore(file, _))
                    else warn(s"Could not create directory $file its children during restore")
                }
              }
              restore(target, entry)
            case Nil => require(requirement = false, s"Source $source not found in repository")
            case list => require(requirement = false, s"Multiple entries found in repository for source $source")
          }
        }


      case _ => println(s"unknown command $command")
    }
  }
}
