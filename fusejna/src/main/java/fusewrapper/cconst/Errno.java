package fusewrapper.cconst;

public interface Errno {
    int OK = 0;
    /** No such file or directory */
    int ENOENT = 2;
    /** Permission denied */
    int EACCES = 13;
    /** Not a directory */
    int ENOTDIR = 20;
    /** Is a directory */
    int EISDIR = 21;
}
