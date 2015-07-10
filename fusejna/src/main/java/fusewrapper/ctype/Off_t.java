package fusewrapper.ctype;

import com.sun.jna.IntegerType;

public class Off_t extends IntegerType {
    public Off_t() { this(0); }
    public Off_t(long value) { super(8, value, true); }
}
