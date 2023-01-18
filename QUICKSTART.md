# Using The DedupFS File System

This is a quickstart description. Find the full description here: [README.html](README.html).

If you have found this text in an archive file like `dedupfs-[version].zip` and you want to get started quickly then continue reading at [Start A New DedupFS File System](#start-a-new-dedupfs-file-system). If you are reading this text because you have found it in a directory that looks more or less like this

    [directory]  data
    [directory]  dedupfs-[version]
    [directory]  fsdb
    [  file   ]  QUICKSTART.html

and you want to use what's in here, then keep reading. 

## Use An Existing DedupFS File System

You have probably found a directory containing a file system, and that file system might contain interesting things. To take a look at these things, proceed as follows:

* Open the directory `dedupfs-[version]`.
* Start the script `readonly.bat` (on Linux: `readonly`).
* Things might go wrong now. If they do, and you are on Windows, possibly `WinFSP` is not installed. Good look reading the [README.html](README.html).
* If all goes well, you see output like this:


    [...] - Dedup file system settings:
    [...] - Repository:  [some directory]
    [...] - Mount point: J:\
    [...] - Readonly:    true
    [...] - Starting the dedup file system now...
    The service java has been started.

The most interesting thing is the "`Mount point: J:\`" message. In this example (it's from a Windows system) the message tells you that you can open the `J:\` drive to find those interesting things like backups or other files. Enjoy!

When you have finished browsing the files in the `J:\` drive, switch to the window with the output and press `CTRL-C` to stop the file system. You will see output like this:

    The service java has been stopped.
    [...] - Stopping dedup file system...
    [...] - Dedup file system is stopped.
    [...] - Shutdown complete.

That's all for a quick start. Find the full description here: [README.html](README.html).

## Start A New DedupFS File System

TODO
