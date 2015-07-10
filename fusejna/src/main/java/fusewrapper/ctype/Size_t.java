package fusewrapper.ctype;

import com.sun.jna.IntegerType;

public class Size_t extends IntegerType {
    public Size_t() { this(0); }
    public Size_t(long value) { super(8, value, true); }
}
