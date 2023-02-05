# Using The Dedup Filesystem

This is a quick start guide. The full description is here: [README](README.md).

If you have found this text in an archive file like `dedupfs-[version].zip` and you want to get started quickly, read [Start A New Dedup Filesystem](#start-a-new-dedup-filesystem). If you are reading this text because you have found it in a directory that looks more or less like this

```
[directory]  data
[directory]  dedupfs-[version]
[directory]  fsdb
[  file   ]  QUICKSTART.html
[  file   ]  README.html
[  file   ]  SCHNELLSTART.html
```

and you want to use what's in it, then read on. 

## Use An Existing Dedup Filesystem

You have probably found a directory containing a filesystem, and that filesystem may contain interesting things. To take a look at them, proceed as follows:

* Open the `dedupfs-[version]` directory.
* Run the `readonly.bat` script (`readonly` on Linux).
* Now things may go wrong. If they do, and you are on Windows, you may not have `WinFSP` installed. Good luck reading the [README](README.md).
* If all goes well, you should see output like this:

```
[...] - Dedup file system settings:
[...] - Repository:  [some directory]
[...] - Mount point: J:\
[...] - Readonly:    true
[...] - Starting the dedup file system now...
The service java has been started.
```

The most interesting thing is the message "`Mount point: J:\`". In this example (it is from a Windows system) the message tells you that you can open the `J:\` drive to find those interesting things like backups or other files. Have fun!

When you are finished browsing the files in the `J:\` drive, switch to the window with the output above and press `CTRL-C` to stop the file system. You will see output like this:

```
The service java has been stopped.
[...] - Stopping dedup file system...
[...] - Dedup file system is stopped.
[...] - Shutdown complete.
```

That's all for a quick start. The full description is here: [README](README.md).

## Start A New Dedup Filesystem

If you have found this text in an archive file like `dedupfs-[version].zip` and you want to get started quickly, do the following:

* If you are running Windows, download and install [WinFSP](https://github.com/billziss-gh/winfsp/releases).
* Create a new repository directory called e.g. `dedup_storage` somewhere, for example on your backup USB hard drive, and extract the archive into it. The repository directory now should contain this:

```
[directory]  dedupfs-[version]
[  file   ]  QUICKSTART.html
[  file   ]  README.html
[  file   ]  SCHNELLSTART.html
```

* Open the `dedupfs-[version]` directory.
* Run the `repo-init.bat` script (`repo-init` on Linux). This will initialize the Dedup filesystem in the repository directory.
* Run the `dedupfs.bat` script (`dedupfs` on Linux). This will start the dedup filesystem.
* If all goes well, you will see output like this:

```
[...] - Dedup file system settings:
[...] - Repository:  [some directory]
[...] - Mount point: J:\
[...] - Readonly:    false
[...] - Starting the dedup file system now...
The service java has been started.
```

The most interesting thing is the "`Mount point: J:\`" message. In this example (it is from a Windows system) the message tells you that you can open the `J:\` drive and copy some files you want to back up there. Enjoy!

When you have finished copying files to the `J:\` drive, switch to the window with the output and press `CTRL-C` to stop the file system. You will see output like this:

```
The service java has been stopped.
[...] - Stopping dedup file system...
[...] - Dedup file system is stopped.
[...] - Shutdown complete.
```

Whenever you want to add more backups or access your existing backups, run the `dedupfs.bat` script (`dedupfs` on Linux) again.

The nice thing about using DedupFS for backups is that stored files are deduplicated, meaning: If you store the same files more than once, the storage space (almost) does not grow! For example, today you can store a backup of all your documents in DedupFS in the `/documents/2022.12.30` directory. If next week you store another backup of all your documents in DedupFS, this time in the `/documents/2023.01.06` directory, it will take up almost no additional space on the drive where your `dedup_storage` folder is located. So, in general, you can think of DedupFS as a backup storage drive where you can store considerably more files than on a normal drive.

That's all for a quick start. The full description is here: [README](README.md).
