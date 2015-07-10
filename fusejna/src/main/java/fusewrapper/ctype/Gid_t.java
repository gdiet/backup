package fusewrapper.ctype;

import com.sun.jna.IntegerType;

public class Gid_t extends IntegerType {
    public Gid_t() { this(0); }
    public Gid_t(long value) {
        super(4, value, true);
    }
}
