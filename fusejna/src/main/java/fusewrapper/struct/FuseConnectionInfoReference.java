package fusewrapper.struct;

import java.util.Arrays;
import java.util.List;

import com.sun.jna.Structure;

import fusewrapper.ctype.Unsigned;

public class FuseConnectionInfoReference extends Structure implements Structure.ByReference {
    /** Major version of the protocol (read-only) */
    public Unsigned proto_major;
    /** Minor version of the protocol (read-only) */
    public Unsigned proto_minor;
    /** Is asynchronous read supported (read-write) */
    public Unsigned async_read;
    /** Maximum size of the write buffer */
    public Unsigned max_write;
    /** Maximum readahead */
    public Unsigned max_readahead;
    /** Capability flags, that the kernel supports */
    public Unsigned capable;
    /** Capability flags, that the filesystem wants to enable */
    public Unsigned want;
    /** Maximum number of backgrounded requests */
    public Unsigned max_background;
    /** Kernel congestion threshold parameter */
    public Unsigned congestion_threshold;
    /** For future use */
    public Unsigned[] reserved = new Unsigned[23];

    protected List<?> getFieldOrder() {
        return Arrays.asList(
            "proto_major", "proto_minor", "async_read", "max_write", "max_readahead", "capable", "want", "max_background",
            "congestion_threshold", "reserved"
        );
    }
}
