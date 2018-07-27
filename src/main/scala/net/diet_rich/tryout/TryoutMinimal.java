package net.diet_rich.tryout;

import jnr.ffi.Platform;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.Statvfs;

import java.nio.file.Paths;
import java.util.Objects;

import static jnr.ffi.Platform.OS.WINDOWS;

public class TryoutMinimal extends FuseStubFS {
    public static void main(String[] args) {
        TryoutMinimal memfs = new TryoutMinimal();
        try {
            String path;
            switch (Platform.getNativePlatform().getOS()) {
                case WINDOWS:
                    path = "J:\\";
                    break;
                default:
                    path = "/tmp/mntm";
            }
            memfs.mount(Paths.get(path), true, false);
        } finally {
            memfs.umount();
        }
    }


    @Override
    public int statfs(String path, Statvfs stbuf) {
        System.out.println("statfs " + path);
        if (Platform.getNativePlatform().getOS() == WINDOWS) {
            // statfs needs to be implemented on Windows in order to allow for copying
            // data from other devices because winfsp calculates the volume size based
            // on the statvfs call.
            // see https://github.com/billziss-gh/winfsp/blob/14e6b402fe3360fdebcc78868de8df27622b565f/src/dll/fuse/fuse_intf.c#L654
            if ("/".equals(path)) {
                stbuf.f_blocks.set(1024 * 1024); // total data blocks in file system
                stbuf.f_frsize.set(1024);        // fs block size
                stbuf.f_bfree.set(1024 * 1024);  // free blocks in fs
            }
        }
        return super.statfs(path, stbuf);
    }

    @Override
    public int getattr(String path, FileStat stat) {
        System.out.println("getattr " + path);
        int res = 0;
        if (Objects.equals(path, "/")) {
            stat.st_mode.set(FileStat.S_IFDIR | 493); // 0755
            stat.st_nlink.set(2);
        } else {
            res = -ErrorCodes.ENOENT();
        }
        return res;
    }

}
