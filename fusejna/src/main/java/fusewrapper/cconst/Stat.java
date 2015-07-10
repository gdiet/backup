package fusewrapper.cconst;

public interface Stat {
    /** These bits determine file type */
    int S_IFMT  = 0170000;
    int S_IFDIR = 0040000;
    int S_IFREG = 0100000;
}
