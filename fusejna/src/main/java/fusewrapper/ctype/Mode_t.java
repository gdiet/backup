package fusewrapper.ctype;

import com.sun.jna.IntegerType;

public class Mode_t extends IntegerType {
    public Mode_t() { this(0); }
    public Mode_t(long value) {
        super(4, value, true);
    }
}
