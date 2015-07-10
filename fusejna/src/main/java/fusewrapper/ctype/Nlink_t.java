package fusewrapper.ctype;

import com.sun.jna.IntegerType;

public class Nlink_t extends IntegerType {
    public Nlink_t() { this(0); }
    public Nlink_t(long value) { super(8, value, true); }
}
