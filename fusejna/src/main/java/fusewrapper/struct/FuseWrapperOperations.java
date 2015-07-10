package fusewrapper.struct;

import java.util.Arrays;
import java.util.List;

import com.sun.jna.Callback;
import com.sun.jna.Structure;

import fusewrapper.Fuse;

/**
 * Most of these should work very similarly to the well known UNIX
 * file system operations.  A major exception is that instead of
 * returning an error in 'errno', the operation should return the
 * negated error value (-errno) directly.
 *
 * All methods are optional, but some are essential for a useful
 * filesystem (e.g. getattr).  Open, flush, release, fsync, opendir,
 * releasedir, fsyncdir, access, create, ftruncate, fgetattr, lock,
 * init and destroy are special purpose methods, without which a full
 * featured filesystem can still be implemented.
 */
@SuppressWarnings("unused")
public class FuseWrapperOperations extends Structure {
    /**
     * Get file attributes.
     *
     * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
     * ignored.	 The 'st_ino' field is ignored except if the 'use_ino'
     * mount option is given.
     */
    public Fuse.GetAttr getattr = null;
    public Callback readlink = null;
    public Callback getdir = null;
    public Callback mknod = null;
    public Callback mkdir = null;
    public Callback unlink = null;
    public Callback rmdir = null;
    public Callback symlink = null;
    public Callback rename = null;
    public Callback link = null;
    public Callback chmod = null;
    public Callback chown = null;
    public Callback truncate = null;
    public Callback utime = null;
    /**
     * File open operation
     *
     * No creation (O_CREAT, O_EXCL) and by default also no
     * truncation (O_TRUNC) flags will be passed to open(). If an
     * application specifies O_TRUNC, fuse first calls truncate()
     * and then open(). Only if 'atomic_o_trunc' has been
     * specified and kernel version is 2.6.24 or later, O_TRUNC is
     * passed on to open.
     *
     * Unless the 'default_permissions' mount option is given,
     * open should check if the operation is permitted for the
     * given flags. Optionally open may also return an arbitrary
     * filehandle in the fuse_file_info structure, which will be
     * passed to all file operations.
     */
    public Fuse.Open open = null;
    /**
     * Read data from an open file
     *
     * Read should return exactly the number of bytes requested except
     * on EOF or error, otherwise the rest of the data will be
     * substituted with zeroes.	 An exception to this is when the
     * 'direct_io' mount option is specified, in which case the return
     * value of the read system call will reflect the return value of
     * this operation.
     */
    public Fuse.Read read = null;
    public Callback write = null;
    public Callback statfs = null;
    public Callback flush = null;
    public Callback release = null;
    public Callback fsync = null;
    public Callback setxattr = null;
    public Callback getxattr = null;
    public Callback listxattr = null;
    public Callback removexattr = null;
    public Callback opendir = null;
    /**
     * Read directory
     *
     * Writes the directory information into {@code directoryBuffer}.
     * The standard behavior is to first write a "." and a ".." entry,
     * followed by the actual directory content.
     */
    public Fuse.ReadDir readdir = null;
    public Callback releasedir = null;
    public Callback fsyncdir = null;
    /**
     * Initialize filesystem
     *
     * Called on filesystem creation. The return value will be passed in
     * the {@code private_data} field of {@code fuse_context} to all file operations
     * and as a parameter to the {@link #destroy} callback.
     */
    public Fuse.Init init = null;
    /**
     * Clean up filesystem
     *
     * Called on filesystem exit.
     */
    public Fuse.Destroy destroy = null;
    public Callback access = null;
    public Callback create = null;
    public Callback ftruncate = null;
    public Callback fgetattr = null;
    public Callback lock = null;
    public Callback utimens = null;
    public Callback bmap = null;
    public int flags = 0; // bitfield
    public Callback ioctl = null;
    public Callback poll = null;
    public Callback write_buf = null;
    public Callback read_buf = null;
    public Callback flock = null;
    public Callback fallocate = null;

    protected List<?> getFieldOrder() {
        return Arrays.asList(
            "getattr", "readlink", "getdir", "mknod", "mkdir", "unlink", "rmdir", "symlink", "rename", "link", "chmod",
            "chown", "truncate", "utime", "open", "read", "write", "statfs", "flush", "release", "fsync", "setxattr",
            "getxattr", "listxattr", "removexattr", "opendir", "readdir", "releasedir", "fsyncdir", "init", "destroy",
            "access", "create", "ftruncate", "fgetattr", "lock", "utimens", "bmap", "flags", "ioctl", "poll",
            "write_buf", "read_buf", "flock", "fallocate"
        );
    }
}
