# DedupFS: Deduplicating File System

DedupFS is a file system with transparent file content deduplication. This means that if you store multiple files containing the same sequence of bytes in the file system, the content is stored only once and is referenced from multiple files. Of course DedupFS allows to update the content of any file without any impact on the content of other files.

## Status Of The DedupFS Software

No guarantees. Author uses for main backups.

## What DedupFS Can Be Used For

Backup of files.

## Why Is DedupFS Better Than ...

lightweight
simple storage format
easy to keep second copy up-to-date

## What DedupFS Should Not Be Used For

everyday file system
security critical things, among others MD5 hash collision attack

## How To ...

initialize file system

run with GUI

run headless

stop

start options

copy-on-move

reclaim space

## Storage Format


