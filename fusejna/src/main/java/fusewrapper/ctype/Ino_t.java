package fusewrapper.ctype;

import com.sun.jna.IntegerType;

public class Ino_t extends IntegerType {
    public Ino_t() { this(0); }
    public Ino_t(long value) {
        super(8, value, true);
    }
}
