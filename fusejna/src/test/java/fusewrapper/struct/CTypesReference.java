package fusewrapper.struct;

import com.sun.jna.Structure;

import java.util.Arrays;
import java.util.List;

import fusewrapper.ctype.*;

@SuppressWarnings("unused")
public class CTypesReference extends Structure implements Structure.ByReference {
    public Dev_t dev_t;
    public Gid_t gid_t;
    public Ino_t ino_t;
    public Mode_t mode_t;
    public Nlink_t nlink_t;
    public Off_t off_t;
    public Size_t size_t;
    public Uid_t uid_t;
    public Unsigned unsigned;
    public Unsigned endOfStructure;

    protected List<?> getFieldOrder() {
        return Arrays.asList(
            "dev_t", "gid_t", "ino_t", "mode_t", "nlink_t", "off_t", "size_t", "uid_t", "unsigned", "endOfStructure"
        );
    }
}
