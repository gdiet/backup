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
* [Upgrading And Version History](#upgrading-and-version-history)
* [Storage Format](#storage-format)
* [License](#license)

## Status Of The DedupFS Software

As the license states, DedupFS is provided "as is", without warranty of any kind. That being said, I use DedupFS now for more than two years for backing up my private files, and my backup repository has grown to approx. 1.8 Million files/folders with 400.000 file contents stored comprising 1.5 TB of data. The largest file stored has a size of approx 7.5 GB.

Bottom line: For my personal use it is mature. Decide for yourself.

## What DedupFS Can Be Used For

The DedupFS dedup file system is good for keeping a backup archive of your files. Its main advantage is that you don't have to worry storing the same file two, three, or ten times, because after the first copy of each file, additional copies take up only little space.

For example, you can once a week just copy "everything" into the dedup file system - each week into a folder that is named by the current date. That way, you have a nice backup and can even access earlier versions of your files by date.

## Why Is DedupFS Better Than ...

Whether DedupFS really is better than any other backup software depends mostly on how you like it and how you use it. Here are the main things why **I** like DedupFS better than all alternatives I have found so far:

* DedupFS is fast at writing and reading backups (at least for personal requirements).
* DedupFS is lightweight, meaning it's easy to install and to run, and it needs little RAM compared to other deduplication software.
* DedupFS uses a simple storage format, so you know that IF something goes horribly wrong there is still a good chance to retrieve most of the stored data.
* "Delete" in DedupFS is a two-step process, so if you accidentally deleted important files from your backups, they are not lost until you explicitly run the "reclaim space" utilities.
* DedupFS automatically creates and keeps backups of the file tree and metadata database, so if necessary you can restore the dedup file system to earlier states.
* DedupFS is designed to make it fast and easy to keep second offline copy of your backup repository up-to-date, even if the repository is terabytes in size.
* DedupFS is open source. It consists of less than 1500 lines of production code.

## What DedupFS Should Not Be Used For

Don't use the DedupFS dedup file system as your everyday file system. It is not fully posix compatible. Locking a file for example will probably not work at all. When a file is closed after writing, immediately opening it for reading will show the old file contents - the new contents are available only after some (short) time. Last but not least changing file contents often leads to a large amount of unused data entries that eat up space unless you use the "reclaim space" utilities regularly.

Don't use the DedupFS dedup file system for really security critical things. For example, DedupFS uses MD5 hashes to find duplicate content, and there is no safeguard implemented against hash collisions. Note that this does not pose a problem when you store e.g. backups of your holiday photos...

## Caveats

* The DedupFS dedup file system only supports regular directories and regular files. It does not support for example soft or hard links or sparse files.
* Deleting files in DedupFS is a two-step process. Don't expect that the repository size shrinks if you delete files. Even if you run the "reclaim space" utilities, the repository size will not shrink. Instead, it will not grow further for some time if you store new files...
* As already mentioned, DedupFS uses MD5 hashes to find duplicate content, and there is no safeguard implemented against hash collisions.
* Since DedupFS has been used less on Linux, there might be additional issues there.
* To support a special operation mode, if data files go missing, DedupFS replaces the missing bytes more or less silently with '0' values.

## System Requirements

### General

DedupFS needs a Java 11 runtime. The app comes bundled with a suitable Java runtime for Windows x64 and Linux x64.

DedupFS needs disk space for its repository. If you back up lots of data, it will need lots of space. Keep an eye on available disk space when using.

DedupFS runs fine with approximately 128 MB RAM assigned to its process. See below for details.

### Windows

Tested on Windows 10 64-bit.

Download and install a [WinFSP Release](https://github.com/billziss-gh/winfsp/releases) to make FUSE (Filesystem in Userspace) available. I use `WinFsp 2020` a.k.a `winfsp-1.6.20027` for running DedupFS.

### Linux

Smoke tested on Ubuntu 64-bit.

DedupFS needs *libfuse* to create a filesystem in userspace. *libfuse* is preinstalled in most Linux distributions.

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

Installing DedupFS is easy: Unpack the DedupFS archive to a place where you have write access. I recommend unpacking it to the repository directory where the dedup file system data will be stored. For details, see the next paragraph.

### Initialize The File System

The dedup file system stores all its data in a repository directory, inside the subdirectories `fsdb` and `data`. Before the dedup file system can be used, the repository needs to be initialized:

* Create a repository directory for the dedup file system data, e.g. on an external backup drive.
* Unpack the DedupFS archive to that repository directory. That way, the DedupFS software is always available together with the DedupFS data. After unpacking, the DedupFS utility scripts like `repo-init` and `dedupfs` should be located directly in the repository directory.
* Start the DedupFS `repo-init` utility in the repository directory, e.g. by double-clicking.
* Check the log output printed to the console where `repo-init` is executed.
* If successful, this command creates in the repository directory the database directory `fsdb` and the log files directory `logs`.

Note:

* By default, `repo-init` initializes the current working directory as DedupFS repository. If you run the script from the command line, you can add a `repo=<target directory>` parameter in order to initialize the repository in a different directory. DedupFS always creates its `logs` directory in the directory containing the DedupFS utility scripts.

### Mount The File System With A GUI

If you want to write, update, or read files in the dedup file system, you have to "mount" it first. Note that the dedup file system must be initialized before you can mount it, see above. Here are the steps to mount the dedup file system:

* If you have installed DedupFS in the repository directory as recommended, start the dedup file system by running `gui-dedupfs` in the repository directory, e.g. by double-clicking.
* After some time the DedupFS GUI will open, showing log entries.
* After some time a log entry will tell you that the dedup file system is started.
* In the log entries, you see among others which repository is used and where the dedup file system is mounted.

Notes:

* The default mount point on Windows is `J:\`, on Linux `tmp/mnt`. To mount the file system somewhere else, call the script with a `mount=<mount target>` parameter.
* On Linux, mount the dedup file system to an existing empty writable directory.
* Don't mount more than one dedup file system if you can avoid it. If you cannot avoid it, make sure the dedup file systems have unique mount points configured - see below.
* The `gui-dedupfs` creates a database backup before mounting the file system, so you can restore the previous state of the file system if something goes wrong.
* By default, `gui-dedupfs` uses the current working directory as DedupFS repository. If you run the script from the command line, you can add a `repo=<target directory>` parameter in order use a different repository directory.
* For additional options, read the `gui-dedupfs` script and the paragraphs below.

### Mount The File System Without GUI

If you want to mount the dedup file system without a GUI, run `dedupfs`. This behaves like `gui-dedupfs` except that it does not start a GUI. So see above for more details on how `dedupfs` works.

### Mount The File System Read-Only

If you want to mount the dedup file system read-only, use the `gui-readonly` or `readonly` utility. These utilities work analog to the write utilities.

Why mount read-only? This can be handy if for example you want to look up files in your backups while making sure that you cannot accidentally add, change or delete files in the backup.

### Read Basic File System Statistics

The `stats` utility allows you to read basic file system statistics. Like the other utilities, it accepts the optional `repo=<target directory>` parameter.

### Configure Memory Settings

The DedupFS utilities use the default Java 11 memory settings. You can change these by editing the utility scripts. Let's start with some rules of thumb:

* It does not hurt to assign much RAM to the DedupFS utilities - unless the operating system or other software running on the same computer doesn't have enough free RAM left for good operation.
* `repo-init` does not need more than ~64 MB RAM.
* `gui-dedupfs` and `dedupfs` need at least ~96 MB RAM for good operation. When storing large files, additional RAM improves performance, so you might want to assign (size of large files to store + 64 MB) RAM to the write utilities. Assigning more will not improve performance.
* `db-backup` (which is called first in the write utilities) and `db-restore` do not need more than ~64 MB RAM.
* `gui-readonly` and `readonly` work fine with ~80 MB RAM. Assigning more will not improve performance.
* The `reclaim-space` utilities need about ((number of data entries) * 64 B + 64 MB) RAM.

To change the RAM assignment of a utility, open it in a text editor. After the `java` or `javaw` call, add the `-Xmx` maximum heap memory setting. In the following example, it is set to 200 MB:

```batch
start "DedupFS" "%~dp0jre\bin\javaw" "-DLOG_BASE=%~dp0\" ...
```

... insert `-Xmx200m` after 'javaw' ...

```batch
start "DedupFS" "%~dp0jre\bin\javaw" -Xmx200m "-DLOG_BASE=%~dp0\" ...
```

### Configure The Temp Directory

When large files are written to the dedup file system so that DedupFS cannot cache them in memory, it caches them in a "temp" directory. By default, it uses a subdirectory of the "temp" directory configured for the user in the operating system.

If you try to get maximum write performance, make sure the temp directory is on a fast (SSD) drive that and is not on the same physical drive as either the repository or the source from which you copy the files. The write utilities accept a `temp=<temp directory>` option, so you can override the default.

### Copy When Moving

The dedup file system can be operated in a "copy when moving" state. In this state, whenever a command is issued that to move a file (not a directory) within the dedup file system from A to B, the file is not moved but copied. What the heck?

Moving files within the dedup file system is very fast. Copying files is much slower, because the operating system reads the file contents and writes them to the new file. In the "copy when moving" state, copy-moving files is just as fast as moving files.

This can be used to first create in the dedup file system a copy of an existing backup (fast) and then only to update those files that have changed (fast if few files have changed).

To enable the "copy when moving" state, either tick the checkbox in the GUI or specify the `copywhenmoving=true` option when starting the dedup file system.

### Log Files

DedupFS writes log files that contain all log entries visible on the console and additionally DEBUG level log entries. DedupFS always creates its `logs` directory in the directory containing the DedupFS utility scripts.

### Create A Database Backup

It is not necessary to call `db-backup` explicitly because it is called first in all utilities where the database might change.

### Restore The Database From A Backup

The `db-restore` utility is for restoring previous versions of the DedupFS database. It accepts the usual `repo=<target directory>` parameter. If run without additional `from=...` parameter, it restores the database to the way it was before the last write operation was started, thus effectively resetting the dedup file system to an earlier state. Alternatively, you can use the `from=...` parameter to point the utility to earlier database backups (zip files) that can be found in the `fsdb` subdirectory of the repository.

### Reclaim Space

Deleting files in DedupFS is a multi-step process: First you delete a file in the dedup file system. This makes the file unavailable in the dedup file system. However, if you restore a previous database backup, the file will be available again in the dedup file system.

If you want to re-use the space the deleted files take up, you can run the `reclaim-space-1` and `reclaim-space-2` utilities. Don't expect that this shrinks the repository size. Instead, the repository size will not increase for some time if you store new files.

Note that running the `reclaim-space-2` utility partially invalidates previous database backups: Files are reclaimed by the utility can't be restored correctly anymore even if you restore the database to an earlier state from backup.

### Clean Up Repository Files

In the `log` subdirectory of the installation directory, up to 1 GB of log files are stored. They are useful for getting insights into how DedupFS was used. You can delete them if you don't need them.

In the `fsdb` subdirectory of the repository directory, DedupFS stores database backup zip files that can be used to reset the dedup file system to an earlier state. You can delete older database backup zip files if you don't need them.

### Run A Shallow Copy Of The File System

Advanced usage pattern, only use if you understand what you are doing!

If you have the repository on a removable device, you can create a copy of it on a local device without the data files in the `data` directory. This "shallow" copy of the file system allows you to browse the directories and files, but it will show '0' bytes instead of the actual content when you open files.

If you

* create a "database only" copy of the file system,
* copy (at least) the last active data file in the right location in the `data` directory,
* be careful: If you ran `reclaim-space-2` some time ago, the last active data file might not be the last data file. In case of doubt, use the `stats` utility to check the data storage size,
* only use the shallow copy of the file system for some time (and not the original),
* don't use the `reclaim-space-2` utility on the shallow copy of the file system,

then you can merge the shallow repository back to the original file system using standard file sync tools.

## Story: How I Use DedupFS

### (Some Of) The Problems DedupFS Solves For Me

* At home, I use a laptop computer with a 1 TB SSD. My private collection of photos, videos, audio and document files is larger than 1 TB. Duh.
* Although I try to avoid it, many photos and videos are stored in two places in my collection. This also bloats backups. Urg.
* The laptop actually is our family computer. Sometimes, somebody accidentially deletes or overwrites files we would have liked to keep, or moves them to a place where we will never find them again. Gah.
* One backup isn't enough for me to feel good, I like to keep two backups, and the second backup should be synchronized with the first, at least from time to time. Acg.

### (Some Of) The Solutions DedupFS Provides

* Large media files that I don't need regularly only reside in the backups.
* DedupFS takes care of deduplicating duplicate files.
* *"I know that in our photos of the 2010 summer holidays, we had this lovely picture of..."* - Let's look in the backup I stored two years ago.
* For synchronizing the second backup, standard file copy tools are enough. I only need to copy those parts of the repository that have changed, and that is easy.

### How I Do It

I have two large external storage drives, drive A and drive B.

Every few weeks,

* I store "everything" from the family laptop to a new folder in the dedup file system residing on drive A. This new folder I name by current date, for example "backups_laptop/2020-10-21".
* To speed things up, before actually creating the new backup, I create a duplicate of the previous backup in a folder with the current date with the `copywhenmoving=true` option enabled. This is fast. After this, I use a tree sync software that mirrors the source to the target, overwriting only those files that have changed (changed size or last modified time) and deleting files in the target that are not present in the source. If few things have changed, this is also fast.

Every few months,

* I use a tree sync software that mirrors the source to the target to update the drive B with the new contents of drive A. If few things have changed, this is pretty fast.

Additionally,

* in the dedup file system there is a "current" folder where I keep (mostly media) files that are just too big for my laptop and that I need only occasionally.

For maximum safety,

* TODO shallow copy of the repository that is auto-sync'ed to a cloud with the last increments, so I can backup my files even more often without having to go fetch the external drive each time.

## Upgrading And Version History

To upgrade a DedupFS installation to a newer version:

* From the installation directory move all files and folders except `data`, `fsdb` and `log` to a separate directory. (The `data` and `fsdb` are only present if you installed DedupFS to the repository directory.)
* Unpack the new DedupFS archive to the installation directory.
* Follow any release specific upgrade instructions (see below).
* Check whether everything works as expected. If yes, you can delete the old app files from the separate directory.

### Version History And Release Specific Update Instructions

#### May Come Eventually

* Blacklist files that should not be stored at all.
* Optionally store packed (gz or similar).
* Reclaim finds & cleans up data entries duplicates.

#### Coming In 2.5

* New `stats` utility.
* In read-only mode, log no error when trying to update last modified time of a file.
* Support running the dedup file system with data files missing.
* On Windows, the utilities support space characters in the app path.

#### 2.4 (2020.10.18)

First public release

## Storage Format

### Tree And File Meta Data

DedupFS stores the tree and file metadata in an [H2](http://h2database.com) SQL database. Use the h2 JDBC driver `lib/h2-<version>.jar` in case you want to inspect the database contents. The database schema is approximately:

```sql
CREATE TABLE TreeEntries (
  id           BIGINT PRIMARY KEY,
  parentId     BIGINT NOT NULL,
  name         VARCHAR(255) NOT NULL,
  time         BIGINT NOT NULL,
  deleted      BIGINT NOT NULL DEFAULT 0,
  dataId       BIGINT DEFAULT NULL
);
CREATE TABLE DataEntries (
  id     BIGINT PRIMARY KEY,
  seq    INTEGER NOT NULL,
  length BIGINT NULL,
  start  BIGINT NOT NULL,
  stop   BIGINT NOT NULL,
  hash   BINARY NULL
);
```

`TreeEntries`: The tree root entry has `id` 0. The tree structure is defined by providing the `parentId` for each node. `time` is the last modified unix timestamp in milliseconds. `deleted` is set to the deletion unix timestamp in milliseconds; for existing (= not deleted) files it is `0`. `dataId` is `null` for directories. For files, it is a reference to `DataEntries.id`. If no matching `DataEntries.id` exists, the file size is zero.

`DataEntries`: Each sequence of bytes that can be referenced as a file content is assigned a unique `id`. The bytes are stored in 1 to n non-contiguous parts, where `seq` of the first part is `1` and so on. `start` and `stop` define the storage position of the part, where `stop` points to the position **after** the respective part. `length` is the combined length of all parts, `hash` the MD5 hash of the full sequence, that is, of all parts concatenated. `length` and `hash` are only set for the first part of each data entry, for subsequent parts they are `null`.

### File Contents

DedupFS stores the file contents in the `data` subdirectory of the repository. The data is distributed to files of 100.000.000 Bytes each. The file names of the data files denote the position of the first byte stored in the respective file. These positions are referenced by `DataEntries.start` and `DataEntries.stop`, see above.

## License

MIT License, see file LICENSE.
