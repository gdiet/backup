# DedupFS: Deduplicating File System

DedupFS is a file system with transparent file content deduplication. This means that if you store multiple files containing the same sequence of bytes in the file system, the content is stored only once and is referenced from multiple files. Of course DedupFS allows to update the content of any file without any impact on the content of other files.

## Status Of The DedupFS Software

No guarantees. Author uses for main backups.

## What DedupFS Can Be Used For

Backup of files.

## Why Is DedupFS Better Than ...

lightweight
simple storage format
delete is a two-step process
database backups
easy to keep second copy up-to-date

## What DedupFS Should Not Be Used For

everyday file system
security critical things

## Caveats

MD5 hash collision attack
deleting to free space is a two-step process

## Basic Steps To Use DedupFS

The following are the basic steps needed to use DedupFS. For details, see the [How To ...](#how-to-...) section of this document.

* Initialize a new dedup file system storage in an empty directory, e.g. on an external drive.
* Mount the file system, then use it to store a backup of your files.
* Stop the file system.
* If required, update the copy of the dedup storage directory that is stored e.g. on a different external drive.
* If required, reclaim space by trashing files that have been marked deleted in the dedup file system.

## How To ...

### Initialize The File System

Before it can be used, the storage directory for the dedup file system needs to be initialized:

* Create an empty target directory that will be used to store all management files of the dedup file system.
* On the command line, change to this target directory.
* There, execute the `repo-init` utility from the DedupFS distribution.
* Check the log output printed to the command line.
* If successful, this command creates the database folder `fsdb` and the log files folder `log`.

Notes:

* The syntax `repo-init <target directory>` is not supported.

### Mount The File System With A GUI

mount with GUI

mount headless

stop

start options

copy-on-move

restore database backup

reclaim space

## Storage Format


