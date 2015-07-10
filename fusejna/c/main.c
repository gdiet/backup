#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <string.h>

int fuseWrapperTest_sizeOfFuseOperations()
{
    return sizeof(struct fuse_operations);
}

int fuseWrapperTest_checkForFuseConnectionInfo(int expectedSize, struct fuse_conn_info *info)
{
    if (expectedSize == sizeof(*info))
    {
        memset(info, 255, sizeof(*info));
        info->proto_major = 1;
        info->proto_minor = 2;
        info->async_read = 3;
        info->max_write = 4;
        info->max_readahead = 5;
        info->capable = 6;
        info->want = 7;
        info->max_background = 8;
        info->congestion_threshold = 9;
    }
    return sizeof(*info);
}

int fuseWrapperTest_checkForStat(int expectedSize, struct stat *stat)
{
    if (expectedSize == sizeof(*stat))
    {
        memset(stat, 255, sizeof(*stat));
        stat->st_dev = 1;
        stat->st_ino = 2;
        stat->st_nlink = 3;
        stat->st_mode = 4;
        stat->st_uid = 5;
        stat->st_gid = 6;
        stat->st_rdev = 7;
        stat->st_size = 8;
//        stat->st_blksize = 9;
//        stat->st_blocks = 10;
    }
    return sizeof(*stat);
}

int fuseWrapperTest_checkForFuseFileInfo(int expectedSize, struct fuse_file_info *info)
{
    if (expectedSize == sizeof(*info))
    {
        memset(info, 255, sizeof(*info));
        info->flags = 1;
        info->fh_old = 2;
        info->writepage = 3;
        info->fh = 4;
        info->lock_owner = 5;
    }
    return sizeof(*info);
}

struct ctypes_teststruct
{
    dev_t dev_t;
    gid_t gid_t;
    ino_t ino_t;
    mode_t mode_t;
    nlink_t nlink_t;
    off_t off_t;
    size_t size;
    uid_t uid_t;
    unsigned unsign;
    unsigned endOfStructure;
};

int fuseWrapperTest_checkForCTypes(int expectedSize, struct ctypes_teststruct *ctypes)
{
    if (expectedSize == sizeof(*ctypes))
    {
        ctypes->dev_t = 1;
        ctypes->gid_t = 2;
        ctypes->ino_t = 3;
        ctypes->mode_t = 4;
        ctypes->nlink_t = 5;
        ctypes->off_t = 6;
        ctypes->size = 7;
        ctypes->uid_t = 8;
        ctypes->unsign = 9;
        ctypes->endOfStructure = 10;
    }
    return sizeof(*ctypes);
}
