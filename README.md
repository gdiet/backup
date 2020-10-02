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
* [Story: How I Use DedupFS](#story-how-i-use-dedupfs)
* [Storage Format](#storage-format)
* [License](#license)

## Status Of The DedupFS Software

No guarantees. Author uses for main backups.

## What DedupFS Can Be Used For

Backup of files. Each time a backup is created, use a new folder with the date - that way, data from old backups can be easily retrieved. Doesn't eat up unnecessary space.

## Why Is DedupFS Better Than ...

fast yet lightweight (little RAM)

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

While DedupFS is known to run on Linux, no Linux DedupFS utility scripts are available. You can run it if you know what you are doing :confused:.

## System Requirements

Windows 10. For Linux see [Caveats](#caveats).

DedupFS uses FUSE (Filesystem in Userspace). This is preinstalled in most Linux installations. On Windows, install [WinFSP](https://github.com/billziss-gh/winfsp) to make FUSE available. It can be downloaded on the [WinFSP Releases](https://github.com/billziss-gh/winfsp/releases) page. Currently I use `WinFsp 2020` a.k.a `winfsp-1.6.20027` for running DedupFS on Windows.

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

Windows: Make sure WinFSP is installed, see [System Requirements](#system-requirements).

Installing DedupFS is easy: Unpack the DedupFS archive to a place where you have write access. I recommend unpacking it to the data repository, that is, the directory where the dedup file system data will be stored. For details, see the next paragraph.

### Initialize The File System

The dedup file system stores all its data in a repository directory, inside the subdirectories `fsdb` and `data`. Before the dedup file system can be used, the repository needs to be initialized:

* Create a repository directory for the dedup file system data, e.g. on an external backup drive.
* Unpack the DedupFS archive to that repository directory. That way, the DedupFS software is always available together with the DedupFS data. After unpacking, the DedupFS utility scripts like `repo-init` and `server-write` should be located directly in the repository directory.
* Start the DedupFS `repo-init` utility in the repository directory, e.g. by double-clicking.
* Check the log output printed to the console where `repo-init` is executed.
* If successful, this command creates in the repository directory the database directory `fsdb` and the log files directory `logs`.

Note:

* By default, `repo-init` initializes the current working directory as DedupFS repository. If you run the script from the command line, you can add a `repo=<target directory>` parameter in order to initialize the repository in a different directory. DedupFS always creates its `logs` directory in the directory containing the DedupFS utility scripts.

### Mount The File System With A GUI

If you want to write, update, or read files in the dedup file system, you have to "mount" it first. Note that the dedup file system must be initialized before you can mount it, see above. Here are the steps to mount the dedup file system:

* If you have installed DedupFS in the repository directory as recommended, start the dedup file system by running `write-dedupfs` in the repository directory, e.g. by double-clicking.
* After some time the DedupFS GUI will open, showing log entries.
* After some time a log entry will tell you that the dedup file system is started.
* In the log entries, you see among others which repository is used and where the dedup file system is mounted.

Notes:

* The `write-dedupfs` creates a database backup before mounting the file system, so you can restore the previous state of the file system if something goes wrong.
* By default, `write-dedupfs` uses the current working directory as DedupFS repository. If you run the script from the command line, you can add a `repo=<target directory>` parameter in order use a different repository directory.
* TODO write about RAM - the Java `Xmx` option.
* For additional options, read the `write-dedupfs` script.

### Mount The File System Without GUI

If you want to mount the dedup file system without a GUI, run `write-dedupfs-console`. This behaves like `write-dedupfs` except that it does not start a GUI. So see above for more details of `write-dedupfs-console`.

### Start Options

temp directory

copy-on-move

find the logs

restore database backup

reclaim space

## Story: How I Use DedupFS

store every few weeks everything

name folders by backup date

for folders too big for my laptop, keep a "current" copy of everything in the dedupfs, so I don't have to browse all date-folders ... uses copywhenmoving option

two external backup drives

temp directory on fast drive (ssd)

## Storage Format

## License

MIT License, see file LICENSE.
