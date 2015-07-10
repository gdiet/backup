package fusewrapper.ctype;

import com.sun.jna.IntegerType;

public class Uid_t extends IntegerType {
    public Uid_t() { this(0); }
    public Uid_t(long value) {
        super(4, value, true);
    }
}
