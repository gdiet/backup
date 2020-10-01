# DedupFS: Deduplicating File System

DedupFS is a file system with transparent file content deduplication. This means that if you store multiple files containing the same sequence of bytes, the file system stores the contents only once and references them multiple times. Of course, you can still update the contents of any file without impact on the contents of other files.

## Status Of The DedupFS Software

No guarantees. Author uses for main backups.

## What DedupFS Can Be Used For

Backup of files. Each time a backup is created, use a new folder with the date - that way, data from old backups can be easily retrieved. Doesn't eat up unnecessary space.

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

Linux scripts not yet checked

## System Requirements

Windows: WinFSP

Linux: Fuse

Free space on target drive

128 MB free RAM - more is better

## Basic Steps To Use DedupFS

The following are the basic steps needed to use DedupFS. For details, see the [How To ...](#how-to-) section of this document.

* Initialize a new dedup file system storage in an empty directory, e.g. on an external drive.
* Mount the file system, then use it to store backups of your files.
* Stop the file system.
* If required, update the copy of the dedup storage directory that is stored e.g. on a different external drive.
* If required, reclaim space by trashing files that have been marked deleted in the dedup file system.

## How To ...

### Install DedupFS

unzip to place with write access. prefer SSD.

### Initialize The File System

Before it can be used, the storage directory for the dedup file system needs to be initialized:

* Create an empty target directory that will be used to store all management files of the dedup file system.
* On the command line, execute the "repo-init" utility from the DedupFS distribution with the "repo" parameter like this: `repo-init repo=<target directory>`.
* Check the log output printed to the command line.
* If successful, this command creates the database directory `<target directory>/fsdb`.

### Mount The File System With A GUI

mount with GUI

mount headless

stop

start options

copy-on-move

find the logs

restore database backup

reclaim space

## Storage Format


