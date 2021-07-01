# DedupFS: A Lightweight Deduplicating File System

DedupFS is a file system well suited for storing many backups of large collections of files - for example your photo collection. Yet it is not a full backup solution. Better regard DedupFS as a backup storage drive where you can store considerably more files than on an ordinary drives.

Technically speaking, DedupFS is a file system with transparent file content deduplication. This means that if you store multiple files containing exactly the same sequence of bytes, DedupFS stores the contents only once and references them multiple times. Of course, you can still update the contents of any file without impact on the contents of other files.

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

DedupFS is provided "as is", without any warranty. That being said, I use DedupFS since 2018 for backing up my private files, and end of 2020 my backup repository contains 1.8 Million files/folders with 400.000 file contents stored comprising 1.5 TB of data. The largest file stored has a size of about 7.5 GB.

Summary: For my personal use it is mature. Decide for yourself.

## What DedupFS Can Be Used For

The DedupFS dedup file system is good for keeping a backup archive of your files. Its main advantage is that you don't have to worry storing the same file multiple times, because additional copies of a file need very little space.

For example, can once a week you can copy "everything" into the dedup file system - each week into a folder that is named by the current date. That way, you have a nice backup and can access earlier versions of your files by date.

## Why Is DedupFS Better Than ...

Whether DedupFS is better than any other backup software depends mostly on how you like it and how you use it. Here are the main things why I like DedupFS better than all alternatives I have found so far:

* DedupFS is fast at writing and reading backups (at least for personal requirements).
* DedupFS is lightweight, meaning it's easy to install and to run, and it needs little RAM compared to other deduplication software.
* DedupFS uses a simple storage format, so you know that if something goes horribly wrong there is still a good chance to retrieve most of the stored data.
* "Delete" in DedupFS is a two-step process, so if you accidentally deleted important files from your backups, they are not lost until you explicitly run the "reclaim space" utilities.
* DedupFS automatically creates and keeps backups of the file tree and metadata database, so if necessary you can restore the dedup file system to earlier states.
* DedupFS is designed to make it fast and easy to keep a second offline copy of your backup repository up-to-date, even if the repository is terabytes in size.
* DedupFS is open source. It consists of less than 1500 lines of production code.

## What DedupFS Should Not Be Used For

Don't use DedupFS as your everyday file system. It is not fully POSIX compatible. Locking a file for example will probably not work at all. When a file is closed after writing, immediately opening it for reading will show the old file contents - the new contents are available only after some (short) time. Last but not least if you change file contents often this leads to a large amount of unused data entries that eat up space unless you use the "reclaim space" utilities.

Don't use DedupFS for security critical things. One reason for that: DedupFS uses MD5 hashes to find duplicate content, and there is no safeguard implemented against hash collisions. Note that this is not a problem when you store backups of your holiday photos...

## Caveats

* DedupFS only supports regular directories and regular files. It does not support soft or hard links or sparse files. Support for soft links is planned for future versions.
* Deleting files in DedupFS is a two-step process. Don't expect that the repository size shrinks if you delete files. Even if you run the "reclaim space" utilities, the repository size will not shrink. Instead, it will not grow further for some time if you store new files.
* DedupFS uses MD5 hashes to find duplicate content, and there is no safeguard implemented against hash collisions.
* Since DedupFS has been used less on Linux, there might be additional issues there.
* To support a special operation mode, if data files go missing, DedupFS replaces the missing bytes more or less silently with '0' values.
* On Linux (e.g. WSL/Debian), CTRL-C might not correctly unmount the file system. The cause might be `/etc/mtab` not being available. In this case the following might help: `sudo ln -s /proc/self/mounts /etc/mtab`




---
TODO remove below

On Linux (e.g. WSL/Debian), `/etc/mtab` might not be available and CTRL-C will not correctly unmount the file system. This might help: `sudo ln -s /proc/self/mounts /etc/mtab`.

```
c:\dateien\Computer\bin\idea\jbr\bin\java.exe -cp * dedup.init c:\dateien\Temp\repo
c:\dateien\Computer\bin\idea\jbr\bin\java.exe -cp * dedup.mount c:\dateien\Temp\repo J:\
java -cp .:* dedup.init /home/georg/repo
java -cp .:* dedup.mount /home/georg/repo /home/georg/dfs

~/git/backup/scala3$
~/sbt/bin/sbt "~ createApp"
java -cp .:target/app/lib/* dedup.mount /home/georg/repo /home/georg/dfs
java -cp .:target/app/lib/* ru.serce.jnrfuse.examples.MemoryFS
```
