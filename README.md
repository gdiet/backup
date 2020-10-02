# DedupFS: Deduplicating File System

DedupFS is a file system with transparent file content deduplication. This means that if you store multiple files containing the same sequence of bytes, the file system stores the contents only once and references them multiple times. Of course, you can still update the contents of any file without impact on the contents of other files.

#### Table of contents
* [Status Of The DedupFS Software](#status-of-the-dedupfs-software)
* [What DedupFS Can Be Used For](#what-dedupfs-can-be-used-for)
* [Why Is DedupFS Better Than ...](#why-is-dedupfs-better-than-)
* [What DedupFS Should Not Be Used For](#what-dedupfs-should-not-be-used-for)
* [Caveats](#caveats)
* [System Requirements](#system-requirements)
* [Basic Steps To Use DedupFS](#basic-steps-to-use-dedupfs)
* [How To ...](#how-to-)
* [Storage Format](#storage-format)
* [License](#license)

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

* Initialize the DedupFS data repository, e.g. on an external drive.
* Mount the file system, then use it to store backups of your files.
* Stop the file system.
* If required, update the copy of the DedupFS repository that is stored e.g. on a different external drive.
* If required, reclaim space by trashing files that have been marked deleted in the dedup file system.

## How To ...

### Install DedupFS

Installing DedupFS is easy: Unpack the DedupFS archive to a place where you have write access. I recommend unpacking it to the data repository, that is, the directory where the dedup file system data will be stored. For details, see the next paragraph.

### Initialize The File System

The dedup file system stores all its data in a repository directory, inside the subdirectories `fsdb` and `data`. Before the dedup file system can be used, the `fsdb` database needs to be initialized:

* Create an repository directory for the dedup file system data, e.g. on an external backup drive.
* Unpack the DedupFS archive to that repository directory. That way, the DedupFS software is always available together with the DedupFS data. After unpacking, the DedupFS utility scripts like `repo-init` and `server-write` should be located directly in the repository directory.
* Start the DedupFS `repo-init` utility in the repository directory, e.g. by double-clicking.
* Check the log output printed to the console where `repo-init` is executed.
* If successful, this command creates in the repository directory the database directory `fsdb` and the log files directory `logs`.

Note:

* By default, `repo-init` initializes the current working directory as DedupFS repository. If you run the script from the command line, you can add a `repo=<target directory>` parameter in order to initialize the repository in a different directory. The `logs` directory is always located in the directory containing the DedupFS utility scripts.

### Mount The File System With A GUI

mount with GUI

mount headless

stop

start options

temp directory

copy-on-move

find the logs

restore database backup

reclaim space

## Storage Format

## License

MIT License, see file LICENSE.
