# DedupFS: A Lightweight Deduplicating File System

For a quick start, take a look at [QUICKSTART.html](QUICKSTART.html) or the German [SCHNELLSTART.html](SCHNELLSTART.html).

DedupFS is a file system for storing many backups of large collections of files - for example your photo collection. The stored files are deduplicated, meaning: If you store the same files multiple times, the storage (almost) doesn't grow! For example, today you can store a backup of all your documents in DedupFS in the `/documents/2022.12.30` directory. If next week you store a backup of all your documents in DedupFS again, this time in the `/documents/2023.01.06` directory, this needs almost no additional space on the drive where your DedupFS data folder is located. So, in general, you can regard DedupFS as a backup storage drive where you can store considerably more files than on an ordinary drive. And if you like to script your backups, DedupFS comes with nice utilities for that, too.

Technically speaking, DedupFS is a file system with transparent file content deduplication: If you store multiple files containing exactly the same sequence of bytes, DedupFS stores the contents only once and references them multiple times. Of course, you can still update the contents of any file without impact on the contents of other files.

#### Table of contents
* [Status Of The DedupFS Software](#status-of-the-dedupfs-software)
* [What DedupFS Is Best For](#what-dedupfs-is-best-for)
* [Why Is DedupFS Better Than ...](#why-is-dedupfs-better-than-)
* [What DedupFS Should Not Be Used For](#what-dedupfs-should-not-be-used-for)
* [Caveats](#caveats)
* [System Requirements](#system-requirements)
* [Basic Steps To Use DedupFS](#basic-steps-to-use-dedupfs)
* [How To ...](#how-to-)
* [How I Do It](#how-i-do-it)
* [Upgrading And Version History](#upgrading-and-version-history)
* [Storage Format](#storage-format)
* [License](#license)

## Status Of The DedupFS Software

DedupFS is provided "as is", without any warranty. That being said, I use DedupFS since 2018 for backing up my private files. End of 2022 my backup repository contains 4.5 Million files/directories with 630.000 file contents stored comprising 1.7 TB of data. The largest file stored has a size of about 7.5 GB.

Summary: For my personal use it is mature. Decide for yourself.

## What DedupFS Is Best For

The DedupFS dedup file system is good for keeping a backup archive of your files. Its main advantage is that you don't have to worry storing the same file multiple times, because additional copies of a file need very little space.

For example, each week you can copy "everything" into the dedup file system, into a directory that is named by the current date. That way, you have a nice backup and can access earlier versions of your files by date.

## Why Is DedupFS Better Than ...

Whether DedupFS is better than any other backup software depends mostly on how you like it and how you use it. Here are the main things why I like DedupFS better than all alternatives I have found so far:

* DedupFS is fast at writing and reading backups (at least for personal requirements).
* DedupFS is lightweight, meaning it's easy to install and to run, and it needs little RAM compared to other deduplication software.
* DedupFS uses a simple storage format, so you know that if something goes horribly wrong there is still a good chance to retrieve most of the stored data.
* "Delete" in DedupFS is a two-step process, so if you accidentally deleted important files from your backups, they are not lost until you explicitly run the "reclaim space" utility.
* DedupFS automatically creates and keeps backups of the file tree and metadata database, so if necessary you can restore the dedup file system to earlier states.
* DedupFS is designed to make it fast and easy to keep a second offline copy of your backup repository up-to-date, even if the repository is terabytes in size.
* DedupFS is open source, see its [GitHub repository](https://github.com/gdiet/backup). It consists of ~2000 lines of production code.

## What DedupFS Should Not Be Used For

Don't use DedupFS as your everyday file system. It is not fully POSIX compatible. Locking a file for example will probably not work at all. If you change file contents often, that leads to a large amount of unused data entries that eat up space unless you use the "reclaim space" utility.

Don't use DedupFS for security critical things. One reason for that: DedupFS uses MD5 hashes to find duplicate content, and there is no safeguard implemented against hash collisions. Note that this is not a problem when you store backups of your holiday photos...

## Caveats

* DedupFS only supports regular directories and regular files. It does not support soft or hard links or sparse files. Support for soft links is planned for future versions.
* Deleting files in DedupFS is a two-step process. Don't expect that the repository size shrinks if you delete files. Even if you run the "reclaim space" utility, the repository size will not shrink. Instead, it will not grow further for some time if you store new files.
* DedupFS uses MD5 hashes to find duplicate content, and there is no safeguard implemented against hash collisions.
* Since DedupFS has been used less on Linux, there might be additional issues there.
* To support a special operation mode, if data files go missing, DedupFS replaces the missing bytes more or less silently with zero values.
* On Linux (e.g. WSL/Debian), CTRL-C might not correctly unmount the file system. The cause might be `/etc/mtab` not being available. In this case the following might help: `sudo ln -s /proc/self/mounts /etc/mtab`

## System Requirements

### General

DedupFS needs a Java 17 runtime. The application comes bundled with a suitable Java runtime for Windows x64 and Linux x64.

DedupFS needs disk space for its repository. If you back up lots of data, it will need lots of space. Keep an eye on available disk space when using.

DedupFS runs fine with approximately 128 MB RAM assigned to its process. See below for details.

### Windows

Tested on Windows 10 64-bit.

Download and install a [WinFSP Release](https://github.com/billziss-gh/winfsp/releases) to make FUSE (Filesystem in Userspace) available. I use `WinFsp 2021` a.k.a. `winfsp-1.9.21096` for running DedupFS.

### Linux

Tested on Debian 64-bit.

DedupFS needs *libfuse* to create a filesystem in userspace. *libfuse* is pre-installed in most Linux distributions.

## Basic Steps To Use DedupFS

The following are the basic steps needed to use DedupFS. For details, see the [How To ...](#how-to-) section of this document.

* Initialize the DedupFS data repository, for example on an external drive.
* Mount the file system, then use it to store backups of your files.
* Stop the file system.
* If required, update the copy of the DedupFS repository that is stored on a different external drive.
* If required, reclaim space by trashing files that have been marked deleted in the dedup file system.

## How To ...

### Install DedupFS

Windows: Make sure WinFSP is installed, see [System Requirements](#system-requirements).

Windows / Linux: Unpack the DedupFS archive. I recommend unpacking it to the repository directory where the dedup file system data will be stored. For details, see the next paragraph.

### Initialize The File System

The dedup file system stores all its data in a repository directory, inside the subdirectories `fsdb` and `data`. Before the dedup file system can be used, the repository needs to be initialized:

* Create a repository directory for the dedup file system data, for example on an external backup drive.
* Unpack the DedupFS archive to that repository directory. That way, the DedupFS software is always available together with the DedupFS data. After unpacking, the DedupFS utility scripts like `repo-init` and `dedupfs` should be located in the `dedupfs-[version]` directory in the repository directory.
* Start the DedupFS `repo-init` utility in the `dedupfs-[version]` directory, for example by double-clicking.
* Check the log output printed to the console where `repo-init` is executed.
* If successful, this command creates in the repository directory the database directory `fsdb` and in the `dedupfs-[version]` directory the log files directory `logs`.

Note:

* By default, `repo-init` and all other DedupFS utilities regard the parent of the current working directory as the DedupFS repository directory. If you run the script from the command line, you can add a `repo=<target directory>` parameter in order to point the utilities to a different repository directory. DedupFS always creates its `logs` directory in the directory containing the DedupFS utility scripts.

### Mount The File System With A GUI

If you want to write, update, or read files in the dedup file system, you have to "mount" it first. Note that the dedup file system must be initialized before you can mount it, see above. Here are the steps to mount the dedup file system:

* If you have installed DedupFS in the repository directory as recommended, start the dedup file system by running `gui-dedupfs` in the `dedupfs` directory, for example by double-clicking.
* After some time the DedupFS GUI will open, showing log entries.
* Some time later a log entry will tell you that the dedup file system is started: `Mounting the dedup file system now...`
* In the log entries, you see among others which repository directory is used and where the dedup file system is mounted.

Notes:

* The default mount point on Windows is `J:\`, on Linux `/mnt/dedupfs`. To mount the file system somewhere else, call the script with a `mount=<mount point>` parameter (the `mount=` part of the parameter can be omitted).
* On Windows, mount the dedup file system to a file system root like `J:\` or to a directory like `C:\myFiles\dedupfs`, where `C:\myFiles` must be an existing directory and `C:\myFiles\dedupfs` must not exist yet.
* On Linux, mount the dedup file system to an existing empty writable directory.
* Don't mount more than one dedup file system if you can avoid it. If you cannot avoid it, make sure the dedup file systems have different `mount=<mount point>` mount points configured.
* `gui-dedupfs` creates a database backup before mounting the file system, so you can restore the previous state of the file system if something goes wrong. To suppress database backup creation, call the script with the `dbBackup=false` parameter.
* By default, `gui-dedupfs` uses the parent of the current working directory as DedupFS repository. If you run the script from the command line, you can add a `repo=<target directory>` parameter in order use a different repository directory.
* For additional options see the paragraphs below.

### Mount The File System Without GUI

If you want to mount the dedup file system without a GUI, run `dedupfs`. This behaves like `gui-dedupfs` except that it does not start a GUI. So see above for more details on how `dedupfs` works.

### Mount The File System Read-Only

If you want to mount the dedup file system read-only, use the `gui-readonly` or `readonly` utility. These utilities work the same way the write enabled utilities do, except that they don't create a database backup before starting.

Why mount read-only? This can be handy if for example you want to look up files in your backups while making sure that you cannot accidentally add, change or delete files in the backup.

### Read Basic File System Statistics

The `stats` utility allows you to read basic file system statistics. Like the other utilities, it accepts the optional `repo=<target directory>` parameter.

### Specify Command Parameters

The following rules apply for command parameters of the DedupFS utilities:
* The keys of `<key>=<value>` parameters are evaluated case-insensitively.
* A backslash '`\`' before an equals sign '`=`' is interpreted as escape character (possibly preventing interpretation of the parameter as `<key>=<value>` pair) and is removed before evaluating the actual parameter.

### Use The `fsc` Command Line Utilities

A number of **command line** utilities for the dedup file system is available through the `fsc` command. Like the other utilities, `fsc` accepts the optional `repo=<target directory>` parameter. `fsc` is not meant to be run without additional parameters.

#### Copy Files And Directories To The DedupFS

Use `fsc backup <source> <target> [reference]` to copy files and directories to the dedup file system without having to mount it first.

This is an **experimental** utility introduced with version 5.1, meaning it might not work in all details as expected, but it does **not** mean that it might corrupt your repository.

**Syntax:**

`fsc [repo=<repository directory>] [dbBackup=false] backup <source> [<source2> [<source...N>]] <target> [reference=<reference>] [forceReference=true]`

**Example:**

`fsc backup /docs /notes/* /backup/?[yyyy]/![yyyy.MM.dd_HH.mm]/ reference=/backup/????/????.??.??_*`

**The `source` parameters:**

In each `source` parameter's last path element the wildcards "`?`" and "`*`" are resolved to a list of matching files / directories. The resolved sources must be readable files or directories on your computer.

**The `target` parameter:**

The `target` parameter specifies the DedupFS directory to copy the source files / directories to. Only the forward slash "`/`" is interpreted as path separator. The backslash "`\`" is used as escape character.

In the `target` parameter's path elements, everything enclosed by square brackets `[...]` is interpreted as [java.text.SimpleDateFormat](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/text/SimpleDateFormat.html) for formatting the current date/time, unless the opening square bracket is escaped with a backslash "`\`".

If a `target` path element starts with the question mark "`?`", the question mark is removed and the corresponding target
directory and its children are created if missing.

If a `target` path element starts with the exclamation mark "!", the exclamation mark is removed. It is ensured that the corresponding
target directory does not exist, then it and its children are created. The exclamation mark can be escaped with a backslash `\`.

**The `reference` and `forceReference` parameters:**

If a directory containing many and / or large files has been stored in the DedupFS before and most files have not been changed in the meantime, creating another backup can be **significantly accelerated** by using the `reference` parameter. This parameter tells the backup utility to first compare the file size and time stamp of the file to store with the reference file stored previously. If they are found to be the same, the backup tool creates a copy of the reference file in the target location instead of copying the source contents there. Note that using a `reference`, if a file's contents have changed, but its size and time stamp have not changed, **the changed contents are not stored** in the backup.

Example:
* Create a first backup of `docs`:
* `fsc backup /docs /backup/?[yyyy]/![yyyy.MM.dd_HH.mm]/`
* Provide a reference for subsequent backups of `docs`:
* `fsc backup /docs /backup/?[yyyy]/![yyyy.MM.dd_HH.mm]/ reference=/backup/????/????.??.??_*`

When a `reference` is provided, the backup utility looks for the reference directory in the DedupFS, resolving '`*`' and '`?`' wildcards with the alphabetically last match. Then the backup utility checks whether the source and the reference directory "look similar". This is to reduce the probability of accidentally specifying an incorrect `reference`. Use the `forceReference=true` parameter to skip this check.

**Excluding files / directories from the backup:**

To exclude files or directories from the backup, proceed as follows:
* Either put an **empty** file `.backupignore` into a source directory to ignore.
* Or put into a source directory a text file `.backupignore` containing 'ignore' rules, one per line. These rules define which entries the backup utility will ignore.
  * Lines are trimmed, empty lines and lines starting with "`#`" are ignored.
  * `*` is the "anything" wildcard.
  * `?` is the "any single character" wildcard.
  * Rules for directories end with "`/`", rules for files without.
  * The rules are matched against the relative source path.

Example for a `.backupignore` file:

    # Do not store the .backupignore itself in the backup.
    .backupignore
    # Do not store the 'temp' subdirectory in the backup.
    temp/
    # In any subdirectories named 'log*', do not store
    # files in the backup that are named '*.log'.
    log*/*.log

#### Create A Database Backup

Use `fsc db-backup` to create a database backup. The database backup is created in the repository's `fsdb` directory.

#### Restore The Database From Backup

Use `fsc db-restore` to restore a previous versions of the DedupFS database, thus effectively resetting the dedup file system to an earlier state. **This command overwrites the current database without further confirmation.**

If run without additional `[file name]` parameter, it restores the database to the way it was before the last write operation was started. Provide a `[file name]` parameter to point the utility to an earlier database backup zip file located in the `fsdb` subdirectory of the repository.

#### Compact The Database File

Use `fsc db-compact` to compact the database file.

#### Find Files By Name Pattern

Use `fsc find <name pattern>` to find files matching the name pattern. The name pattern supports '`%`' as wildcard for any number of characters and '`_`' as wildcard for a single character.

#### List Files

Use `fsc list <path>` to list the contents of the directory denoted by `<path>`.

#### Delete A File Or A Directory

Use `fsc del <path>` to delete the file or **recursively** delete the directory denoted by `<path>`. **This utility does not create a database backup.** If required, use `fsc backup` before using `fsc del <path>`.

### Configure Memory Settings

The DedupFS utilities come with reasonable default memory settings. You can change these by editing the utility scripts. Let's start with some rules of thumb:

* It does not hurt to assign much RAM to the DedupFS utilities. That is, unless the operating system or other software running on the same computer doesn't have enough free RAM left.
* `repo-init` does not need more than ~64 MB RAM.
* `gui-dedupfs` and `dedupfs` need at least ~96 MB RAM for good operation. When storing large files or using a slow storage device, additional RAM improves performance.
* `db-restore` might need more than 64 MB RAM, it depends on the database size.
* `gui-readonly` and `readonly` work fine with ~80 MB RAM. Assigning more will not improve performance.
* The `reclaim-space` utility need about ((number of data entries) * 64 B + 64 MB) RAM.

To change the RAM assignment of a utility, open it in a text editor. After the `$JAVA` or `%JAVA%` call, change `-Xmx` maximum heap memory setting.

### Configure The Temp Directory

When large files are written to the dedup file system and DedupFS cannot cache them in memory, it caches them in a "temp" directory. By default, it uses a subdirectory of the "temp" directory configured for the user in the operating system.

To get maximum write performance, make sure the temp directory is on a fast (SSD) drive that and is not on the same physical drive as either the repository or the source from which you copy the files. The write utilities accept a `temp=<temp directory>` option, so you can override the default.

### Copy When Moving

The dedup file system can be operated in a "copy when moving" state. In this state, whenever a command is issued to move a file (not a directory) within the dedup file system from directory A to directory B, the file is not moved but copied. What the heck?

Moving files within the dedup file system is very fast. Copying files is much slower, because the operating system reads the file contents and writes them to the new file. In the "copy when moving" state, copy-moving files is just as fast as moving files.

This can be used to first create in the dedup file system a copy of an existing backup (fast) and then only to update those files that have changed (fast if few files have changed).

To enable the "copy when moving" state, either tick the checkbox in the GUI or specify the `copyWhenMoving=true` option when starting the dedup file system.

### Find The Log Files

DedupFS writes log files that contain all log entries visible on the console and additionally DEBUG level log entries. DedupFS always creates its `logs` directory in the directory containing the DedupFS utility scripts.

### Create A Database Backup Or Restore The Database

The `db-backup` and `db-restore` utilities are convenience entry points for the `fsc db-backup` and `fsc db-restore` commands. Other than the plain commands, these utilities ask for confirmation before execution.

### Blacklist Files

The `blacklist` utility is for blacklisting files that should be removed from the dedup file system if they are currently stored and should not be stored even when added later to the file system. Reading a blacklisted file yields [file length] zeros. In addition to the usual `repo=<target directory>` parameter, the utility accepts the following parameters:
* `dbBackup=false` (optional, default true): Create a database backup before starting the blacklisting process.
* `blacklistDir=<directory name>` (optional, default `blacklist`): If the subdirectory of `repo` with this name contains files, those are added to the blacklist in the repository, in a directory named with the current timestamp.
* `deleteFiles=false` (optional, default true): If true, the files in `blacklistDir` are deleted once they have been added to the blacklist in the repository.
* `dfsBlacklist=<directory name>` (optional, default `blacklist`): Name of the base blacklist directory in the dedup file system, resolved against root.
* `deleteCopies=true` (optional, default false): If true, mark deleted all blacklisted occurrences except for the original entries in the `dfsBlacklist` directory.

After running the `blacklist` utility, as long as you do not store new files in the dedup file system you can still restore previous file system states by restoring the database from backups. Once new files are stored, restoring a database backup from before the blacklisting process will result in partial data corruption.

### Reclaim Space

When you delete a file in the dedup file system, internally the file is marked as "deleted" and nothing more. This means, that the dedup file system will **not** free that file's storage space, and that you can make the file available again by restoring a previous state of the database from backup.

If you want to re-use the space deleted files take up, run the `reclaim-space` utility. Note that this will not shrink the repository size. Instead, the repository size will not increase for some time if you store new files.

The `reclaim-space` utility purges deleted and orphan entries from the database. After running it, as long as you do not store new files in the dedup file system you can still restore previous file system states by restoring the database from backups. Once new files are stored, restoring a database backup from before the reclaim process will result in partial data corruption.

In addition to the usual `repo=<target directory>` parameter, the `reclaim-space` utility accepts an optional `keepDays=[number]` parameter (the `keepDays=` part can be omitted) that can be used to specify that recently deleted files should not be reclaimed. Without this parameter, all deleted files are reclaimed.

When the reclaim process is finished, the `reclaim-space` utility compacts the database file, then exits.

### Clean Up Repository Files

In the `log` subdirectory of the installation directory, up to 1 GB of log files are stored. They are useful for getting insights into how DedupFS was used. You can delete them if you don't need them.

In the `fsdb` subdirectory of the repository directory, DedupFS stores database backup zip files that can be used to reset the dedup file system to an earlier state. You can delete older database backup zip files if you don't need them.

### Run A Shallow Copy Of The File System

Advanced usage pattern, only use if you understand what you are doing!

If you have the repository on a removable device, you can create a copy of it on a local device without the data files in the `data` directory. This "shallow" copy of the file system allows you to browse the directories and files, but it will show '0' bytes instead of the actual content when you open files.

If you

* create a "database only" copy of the file system,
* copy (at least) the last active data file in the right location in the `data` directory,
* be careful: If you ran `reclaim-space` some time ago, the last active data file might not be the last data file. In case of doubt, use the `stats` utility to check the data storage size,
* only use the shallow copy of the file system for some time (and not the original),
* don't use the `reclaim-space` utility on the shallow copy of the file system,

then you can merge the shallow repository back to the original file system using standard file sync tools.

### Continue On Errors

Hopefully, you will never see an `EnsureFailed` exception the logs. However, if you are reproducibly blocked by an `EnsureFailed` exception, you can tell the dedup file system not to stop processing when the offending condition occurs. (Do this at you own risk!) For this, add `-Dsuppress.[marker]` to the java options in the script, where `[marker]` is the marker string prepended to the actual exception message, e.g. `-Dsuppress.cache.keep` or `-Dsuppress.utility.restore`.

### Run The H2 Database Server In TCP Mode

For demonstration, investigation or debugging it might be interesting to examine the DedupFS database while the DedupFS file system is mounted. For this, run a H2 TCP server locally, e.g.

    java -cp "h2-2.1.214.jar" org.h2.tools.Server -tcp -tcpPort 9876

and add `-DH2.TcpPort=<TCP port>` to the java options in the script. When running in this mode, at startup time the JDBC connection information used is logged on WARN level.

## Story: How I Use DedupFS

### (Some Of) The Problems DedupFS Solves For Me

* At home, I use a laptop computer with a 1 TB SSD. My private collection of photos, videos, audio and document files is larger than 1 TB. Duh.
* Although I try to avoid it, many photos and videos are stored in two places in my collection. This also bloats backups. Urg.
* The laptop actually is our family computer. Sometimes, somebody accidentally deletes or overwrites files we would have liked to keep, or moves them to a place where we will never find them again. Gah.
* One backup isn't enough for me to feel good, I like to keep two backups, and the second backup should be synchronized with the first, at least from time to time. Acg.

### (Some Of) The Solutions DedupFS Provides

* Large media files that I don't need regularly only reside in the backups.
* DedupFS takes care of deduplicating duplicate files.
* *"I know that in our photos of the 2010 summer holidays, we had this lovely picture of..."* - Let's look in the backup I stored two years ago.
* For synchronizing the second backup, standard file copy tools are enough. I only need to copy those parts of the repository that have changed, and that is easy.

### How I Do It

I have two large external storage drives, drive A and drive B.

Every few weeks,

* I store "everything" from the family laptop to a new directory in the dedup file system residing on drive A. This new directory I name by current date, for example "backups_laptop/2020-10-21".
* To speed things up, before actually creating the new backup, I create a duplicate of the previous backup in a directory with the current date with the `copywhenmoving=true` option enabled. This is fast. After this, I use a tree sync software that mirrors the source to the target, overwriting only those files that have changed (changed size or last modified time) and deleting files in the target that are not present in the source. If few things have changed, this is also fast.

Every few months,

* I use a tree sync software that mirrors the source to the target to update the drive B with the new contents of drive A. If few things have changed, this is pretty fast.

Additionally,

* in the dedup file system there is a "current" directory where I keep (mostly media) files that are just too big for my laptop and that I need only occasionally.

For maximum safety,

* TODO shallow copy of the repository that is auto-synchronized to a cloud with the last increments, so I can back up my files even more often without having to go fetch the external drive each time.

## Upgrading And Version History

To upgrade a DedupFS installation to a newer version:

* Unpack the new DedupFS archive to the installation directory, next to the existing installation.
* Follow any release specific upgrade instructions (see below).
* Check whether everything works as expected. If yes, you can delete the old installation directory.

### Version History And Release Specific Update Instructions

#### May Come Eventually

* In some way give access to deleted files and directories.
* Development: Try out scoverage instead of jacoco (a spike 2022.10 didn't work well).
* Create sql db backup from the file backup and start the file system in parallel.
* Backup script for backing up directories without needing to mount the file system, optionally referencing an existing backup.
* Change database backup, no need to have the full backup as default every time?
* Support for soft links.
* Optionally store packed (gz or similar).
* The reclaim utility finds & cleans up data entry duplicates.

#### 5.2.0 (2023.01.20)

* Updated Java to 17.0.6_10, some libraries, and SBT.
* Added QUICKSTART.html and German SCHNELLSTART.html documentation.
* Rewritten experimental `fsc backup` command, changed functionality.
* Rewritten write cache handling for improved maintainability.
* Added option to run with a H2 TCP server.

#### 5.1.0 (2022.11.25)

* Separate Windows and Linux packages, with a minified JRE, to reduce package size from about 100MB to less than 50 MB.
* Experimental `fsc backup` command for backing up directories without needing to mount the dedup file system.
* Added explicit memory settings to all start scripts.
* Unified and cleaner logging and console output; showing log level WARN and ERROR on console for the utilities.
* Performance improvement: Start the file system or run the write command while in parallel creating script database backup from the plain backup.
* Updated Scala to 3.2.1, Java to 17.0.5_8, libraries, SBT and sbt-updates plugin.
* Full database compaction only when running `reclaim-space` or `fsc db-compact`, for other write commands compact database for at most 2 seconds.
* Added `fsc db-compact` command.
* When running `db-backup`, print to console the corresponding restore command.
* Added `db-backup` utility script.
* Introduced `fsc db-restore` command.
* Renamed `fsc backup` to `fsc db-backup`.

#### 5.0.1 (2022.10.19)

* Fixed a failing test for the blacklisting code that probably is a bug, although the exact faulty behavior has not been researched.
* Check for children before marking entries deleted. Mainly prevents replacing non-empty directories.
* Fixed bug that mounting the file system read-only failed.
* Updated Scala to version 3.2 and Java to 17.0.4.1_1, updated many libraries.
* Updated H2 database from 2.1.212 to 2.1.214 - binary compatible, no migration necessary.

#### 5.0.0 (2022.06.09)

* Upgrade H2 database from 1.4.200 to 2.1.212.

**Migration from 4.x to 5.x:**

* **Direct migration of repositories from versions prior to 4.x is not supported.**
* Use the 4.x dedupfs software to run the command `fsc backup` in order to make sure a current database backup exists.
* Unpack the 5.x dedupfs software into the repository.
* Use the 5.x `db-restore` utility like this:<br> `db-restore from=dedupfs_[timestamp].zip`<br> to point the utility to the database backup zip file created above. Look for the exact timestamp to use in the `fsdb` subdirectory of the repository.
* The migration is complete. Don't use dedupfs versions previous to 5.0.0 anymore with the repository.
* Eventually, manually delete the final version of the 4.x database, that is, the files `dedupfs.mv.db` and `dedupfs.mv.db.backup` in the `fsdb` subdirectory of the repository.

#### 4.1.0 (2022.02.19)

* New `blacklist` utility for blacklisting files that should not be stored at all.
* Simplified reclaim space process: After running the `reclaim-space` utility, freed up space is automatically used when storing new files.
* Compact database when unmounting the dedup file system and after `blacklist` and `reclaim-space`. (git 548f1803)
* `db-restore`, `mount` and `reclaim-space` accept an unnamed parameter.

#### 4.0.0 (2021.12.30)

* Updated Java 11 to Java 17.
* Updated dedupfs database version from 2 to 3 to prepare upgrading H2 database to 2.0.202, which will come with dedupfs 5.
* Added `fsc` utilities to create a database backup or to list, find, or delete files without mounting the repository.

**Migration from 3.x to 4.x:**

* Start dedupfs 4.x with repository write access once to migrate the repository database.
* After migration, dedupfs version prior to 4.x can't be used anymore with the repository.
* To fall back to an earlier software version, restore the repository database from a backup created before upgrading to 4.x.

#### 3.0.1 (2021.12.24)

* On Windows, fixed dedupfs problems caused by missing executable flag. (git 1c69bf58)

#### 3.0.0 (2021.07.31)

Named "3" because this release is a Scala 3 re-write of dedupfs.

* On Windows, named the dedup file system volume "DedupFS". (git 46a076d)
* Fixed occasional out-of-memory or deadlock condition (rewrite of parallel handling).
* Restructured installation directory, ".." instead of "." is the default repository path.

#### 2.6 (2020.11.15)

* Fixed deadlock when moving files in the dedup file system along with the dirty corner cases when moving files. (git 7e6d858)

Known problems:

* Occasional deadlock and out-of-memory conditions.

#### 2.5 (2020.10.23)

* New `stats` utility. (git a11229b)
* In read-only mode, log no error when trying to update last modified time of a file. (git 1bbdcc7)
* Support running the dedup file system with data files missing (for the "Shallow Copy" feature). (git 013fc7d)
* On Windows, the utilities support space characters in the app path. (git 1632819)
* Fixed that background process can go on for a long time.

Known problems:

* Deadlock for example when moving files in the dedup file system.
* Dirty corner cases when moving a file that currently is just written to.

#### 2.4 (2020.10.18)

First public release.

Known problems:

* If the drive the temp directory resides in is faster than the target drive, when copying many files the background write-to-target process may still take a long time when the foreground copy-to-DedupFS process has finished.
* Dirty corner cases when moving a file that currently is just written to.

## Storage Format

### Tree And File Metadata

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
