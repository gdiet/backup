package fusewrapper.ctype;

import com.sun.jna.IntegerType;

public class Dev_t extends IntegerType {
    public Dev_t() { this(0); }
    public Dev_t(long value) { super(8, value, true); }
}
