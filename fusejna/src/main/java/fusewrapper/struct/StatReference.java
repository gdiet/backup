package fusewrapper.struct;

import java.util.Arrays;
import java.util.List;

import com.sun.jna.Structure;

import fusewrapper.ctype.*;

public class StatReference extends Structure implements Structure.ByReference {
    /** ID of device containing file */
    public Dev_t st_dev;
    /** inode number */
    public Ino_t st_ino;
    /** number of hard links */
    public Nlink_t st_nlink;
    /** protection */
    public Mode_t st_mode;
    /** user ID of owner */
    public Uid_t st_uid;
    /** group ID of owner */
    public Gid_t st_gid;
    /** other */
    public int diverse_1;
    /** device ID (if special file) */
    public Dev_t st_rdev;
    /** total size, in bytes */
    public Off_t st_size;
    /** other */
    public int[] diverse_2 = new int[22];

    protected List<?> getFieldOrder() {
        return Arrays.asList(
            "st_dev", "st_ino", "st_nlink", "st_mode", "st_uid", "st_gid", "diverse_1", "st_rdev", "st_size", "diverse_2"
        );
    }
}
