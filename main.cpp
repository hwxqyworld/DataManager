#define FUSE_USE_VERSION 35
#include <fuse3/fuse.h>

#include "local_chunk_store.h"
#include "raid_chunk_store.h"
#include "rs_coder.h"
#include "file_manager.h"

#include <memory>
#include <vector>
#include <string>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <vector>
#include <sys/stat.h>

static std::shared_ptr<FileManager> g_fm;

// 只支持单个文件 "/file"
static const char *SINGLE_FILE_PATH = "/file";

static int raidfs_getattr(const char *path, struct stat *stbuf,
                          struct fuse_file_info *fi)
{
    (void)fi;
    std::memset(stbuf, 0, sizeof(struct stat));

    if (std::strcmp(path, "/") == 0) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }

    if (std::strcmp(path, SINGLE_FILE_PATH) == 0) {
        stbuf->st_mode = S_IFREG | 0644;
        stbuf->st_nlink = 1;
        uint64_t size = g_fm->get_size(SINGLE_FILE_PATH);
        stbuf->st_size = (off_t)size;
        return 0;
    }

    return -ENOENT;
}

static int raidfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                          off_t offset, struct fuse_file_info *fi,
                          enum fuse_readdir_flags flags)
{
    (void)offset;
    (void)fi;
    (void)flags;

    if (std::strcmp(path, "/") != 0)
        return -ENOENT;

    filler(buf, ".", nullptr, 0, FUSE_FILL_DIR_PLUS);
    filler(buf, "..", nullptr, 0, FUSE_FILL_DIR_PLUS);
    filler(buf, SINGLE_FILE_PATH + 1, nullptr, 0, FUSE_FILL_DIR_PLUS); // "file"

    return 0;
}

static int raidfs_open(const char *path, struct fuse_file_info *fi)
{
    if (std::strcmp(path, SINGLE_FILE_PATH) != 0)
        return -ENOENT;

    // 不做权限检查，全部允许
    return 0;
}

static int raidfs_read(const char *path, char *buf, size_t size, off_t offset,
                       struct fuse_file_info *fi)
{
    (void)fi;

    if (std::strcmp(path, SINGLE_FILE_PATH) != 0)
        return -ENOENT;

    std::string out;
    if (!g_fm->read(SINGLE_FILE_PATH, (uint64_t)offset, size, out))
        return -EIO;

    if (out.size() > size) {
        // 理论上不会发生
        out.resize(size);
    }

    std::memcpy(buf, out.data(), out.size());
    return (int)out.size();
}

static int raidfs_write(const char *path, const char *buf, size_t size,
                        off_t offset, struct fuse_file_info *fi)
{
    (void)fi;

    if (std::strcmp(path, SINGLE_FILE_PATH) != 0)
        return -ENOENT;

    if (!g_fm->write(SINGLE_FILE_PATH, (uint64_t)offset, buf, size))
        return -EIO;

    return (int)size;
}

static int raidfs_truncate(const char *path, off_t size,
                           struct fuse_file_info *fi)
{
    (void)fi;

    if (std::strcmp(path, SINGLE_FILE_PATH) != 0)
        return -ENOENT;

    if (!g_fm->truncate(SINGLE_FILE_PATH, (uint64_t)size))
        return -EIO;

    return 0;
}

static const struct fuse_operations raidfs_ops = {
    .getattr    = raidfs_getattr,
    .readlink   = nullptr,
    .mknod      = nullptr,
    .mkdir      = nullptr,
    .unlink     = nullptr,
    .rmdir      = nullptr,
    .symlink    = nullptr,
    .rename     = nullptr,
    .link       = nullptr,
    .chmod      = nullptr,
    .chown      = nullptr,
    .truncate   = raidfs_truncate,
    .open       = raidfs_open,
    .read       = raidfs_read,
    .write      = raidfs_write,
    .statfs     = nullptr,
    .flush      = nullptr,
    .release    = nullptr,
    .fsync      = nullptr,
    .setxattr   = nullptr,
    .getxattr   = nullptr,
    .listxattr  = nullptr,
    .removexattr= nullptr,
    .opendir    = nullptr,
    .readdir    = raidfs_readdir,
    .releasedir = nullptr,
    .fsyncdir   = nullptr,
    .init       = nullptr,
    .destroy    = nullptr,
    .access     = nullptr,
    .create     = nullptr,
    .lock       = nullptr,
    .utimens    = nullptr,
    .bmap       = nullptr,
    .ioctl      = nullptr,
    .poll       = nullptr,
    .write_buf  = nullptr,
    .read_buf   = nullptr,
    .flock      = nullptr,
    .fallocate  = nullptr,
};


int main(int argc, char *argv[])
{
    // ./cloudraidfs <mountpoint> <dir0> <dir1> <dir2> <dir3> <dir4> [FUSE options...]

    if (argc < 7) {
        fprintf(stderr,
                "用法: %s <mountpoint> <dir0> <dir1> <dir2> <dir3> <dir4> [FUSE options]\n",
                argv[0]);
        return 1;
    }

    const char *mountpoint = argv[1];

    // 解析 RAID 后端目录
    std::vector<std::shared_ptr<ChunkStore>> backends;
    for (int i = 2; i < 7; i++) {
        backends.push_back(std::make_shared<LocalChunkStore>(argv[i]));
    }

    int k = 4;
    int m = 1;

    auto coder = std::make_shared<RSCoder>();
    auto raid = std::make_shared<RAIDChunkStore>(backends, k, m, coder);
    g_fm = std::make_shared<FileManager>(raid);

    // 构造 FUSE 参数：progname, mountpoint, 其余 FUSE options
    std::vector<char*> fuse_argv;
    fuse_argv.push_back(argv[0]);       // 程序名
    fuse_argv.push_back(argv[1]);       // 挂载点

    // 其余参数（从 argv[7] 开始）是 FUSE options
    for (int i = 7; i < argc; i++) {
        fuse_argv.push_back(argv[i]);
    }

    struct fuse_args args = FUSE_ARGS_INIT(
        (int)fuse_argv.size(),
        fuse_argv.data()
    );

    // 让 FUSE 解析 mountpoint 和选项
    fuse_opt_parse(&args, NULL, NULL, NULL);

    int ret = fuse_main(args.argc, args.argv, &raidfs_ops, nullptr);
    fuse_opt_free_args(&args);

    return ret;
}

