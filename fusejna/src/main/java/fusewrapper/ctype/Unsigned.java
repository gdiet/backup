package fusewrapper.ctype;

import com.sun.jna.IntegerType;

public class Unsigned extends IntegerType {
    public Unsigned() { this(0); }
    public Unsigned(long value) {
        super(4, value, true);
    }
}
