package fusewrapper.struct;

import com.sun.jna.NativeLong;
import com.sun.jna.Structure;
import fusewrapper.ctype.Unsigned;

import java.util.Arrays;
import java.util.List;

public class FuseFileInfoReference extends Structure implements Structure.ByReference {
    /** Open flags.	 Available in open() and release(). */
    public int flags;
    /** Old file handle, don't use. */
    public NativeLong fh_old;
    /** In case of a write operation indicates if this was caused by a writepage. */
    public int writepage;
    /**
     * direct_io (bit): Can be filled in by open, to use direct I/O on this file.
     * keep_cache (bit): Can be filled in by open, to indicate, that cached file data need not be invalidated.
     * flush (bit): Indicates a flush operation.  Set in flush operation, also maybe set in highlevel lock operation and lowlevel release operation.
     * nonseekable (bit): Can be filled in by open, to indicate that the file is not seekable.
     * flock_release (bit): Indicates that flock locks for this file should be released.  If set, lock_owner shall contain a valid value. May only be set in ->release().
     * padding (27 bits): Padding.  Do not use.
     */
    public int more_flags;
    /** File handle.  May be filled in by filesystem in open(). Available in all other file operations. */
    public long fh;
    /** Lock owner id.  Available in locking operations and flush. */
    public long lock_owner;

    protected List<?> getFieldOrder() {
        return Arrays.asList(
            "flags", "fh_old", "writepage", "more_flags", "fh", "lock_owner"
        );
    }
}
