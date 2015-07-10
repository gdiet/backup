package fusewrapper;

import java.util.Arrays;

import com.sun.jna.Pointer;

import fusewrapper.cconst.Errno;
import fusewrapper.cconst.Fcntl;
import fusewrapper.cconst.Stat;
import fusewrapper.ctype.Mode_t;
import fusewrapper.ctype.Off_t;
import fusewrapper.struct.FuseWrapperOperations;

public class FuseMainExample {
    // Call fuse_main_real with "-f" (foreground) or "-d" (debug) as argument,
    // else it never returns and the Java process is not ended correctly on file system unmount.
    public static void main(String[] args) {
        String[] fuseArgs = args.length == 0 ? new String[] {"fusewrapper", "-f", "/tmp/fuse"} : args;
        System.out.println("Starting fuse wrapper with the args: " + Arrays.toString(fuseArgs));

        FuseWrapperOperations ops = new FuseWrapperOperations();

        System.out.println("*** the operations structure reports as size: " + ops.size());

        String helloContent = "hello world.";

        ops.init = info -> {
            System.out.println("*** sample output from init callback");
            System.out.printf("*** protocol %s.%s - async read = %s\n", info.proto_major, info.proto_minor, info.async_read);
            return null;
        };
        ops.readdir = (path, directoryBuffer, fillerFunction, ignoredOffset, fileInfo) -> {
            if (path.equals("/")) {
                fillerFunction.invoke(directoryBuffer, ".", null, 0);
                fillerFunction.invoke(directoryBuffer, "..", null, 0);
                fillerFunction.invoke(directoryBuffer, "hello", null, 0);
            } else return -Errno.ENOENT;
            return 0;
        };
        ops.getattr = (path, fileInfo) -> {
            fileInfo.clear();
            switch(path) {
                case "/":
                    fileInfo.st_mode = new Mode_t(Stat.S_IFDIR | 0777);
                    break;
                case "/hello":
                    fileInfo.st_mode = new Mode_t(Stat.S_IFREG | 0444);
                    fileInfo.st_size = new Off_t(helloContent.length());
                    break;
                default:
                    return -Errno.ENOENT;
            }
            return 0;
        };
        ops.open = (path, fileInfo) -> {
            if (!path.equals("/hello")) return -Errno.ENOENT;
            if ((fileInfo.flags & Fcntl.O_ACCMODE) != Fcntl.O_RDONLY) return -Errno.EACCES;
            return 0;
        };
        ops.read = (path, data, size, offset, fileInfo) -> {
            if (!path.equals("/hello")) return -Errno.ENOENT;
            if (offset.intValue() < helloContent.length()) {
                int actualSize = Math.min(size.intValue(), helloContent.length() - offset.intValue());
                data.write(0, helloContent.getBytes(), offset.intValue(), actualSize);
                return actualSize;
            } else {
                return 0;
            }
        };
        int status = Fuse.INSTANCE.fuse_main_real(fuseArgs.length, fuseArgs, ops, ops.size(), Pointer.NULL);

        System.out.println("fuse wrapper returned " + status);
        System.exit(status);
    }
}
