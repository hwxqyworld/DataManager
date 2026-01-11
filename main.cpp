#define FUSE_USE_VERSION 35
#include <fuse3/fuse.h>

#include "local_chunk_store.h"
#include "raid_chunk_store.h"
#include "rs_coder.h"
#include "file_manager.h"
#include "metadata_manager.h"

#include <memory>
#include <vector>
#include <string>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <sys/stat.h>

static std::shared_ptr<FileManager> g_fm;
static std::shared_ptr<MetadataManager> g_meta;

// ------------------------------------------------------------
// getattr
// ------------------------------------------------------------
static int raidfs_getattr(const char *path, struct stat *stbuf,
                          struct fuse_file_info *fi)
{
    (void)fi;
    memset(stbuf, 0, sizeof(struct stat));

    std::string p(path);

    if (p == "/") {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }

    if (g_meta->exists(p)) {
        stbuf->st_mode = S_IFREG | 0644;
        stbuf->st_nlink = 1;
        stbuf->st_size = (off_t)g_meta->get_size(p);
        return 0;
    }

    // 目录判断：如果 Trie 中存在该前缀，则视为目录
    auto children = g_meta->list_dir(p);
    if (!children.empty()) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }

    return -ENOENT;
}

// ------------------------------------------------------------
// readdir
// ------------------------------------------------------------
static int raidfs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                          off_t offset, struct fuse_file_info *fi,
                          enum fuse_readdir_flags flags)
{
    (void)offset;
    (void)fi;
    (void)flags;

    std::string p(path);

    auto list = g_meta->list_dir(p);
    if (list.empty() && p != "/")
        return -ENOENT;

    filler(buf, ".", nullptr, 0, FUSE_FILL_DIR_PLUS);
    filler(buf, "..", nullptr, 0, FUSE_FILL_DIR_PLUS);

    for (auto &name : list) {
        filler(buf, name.c_str(), nullptr, 0, FUSE_FILL_DIR_PLUS);
    }

    return 0;
}

// ------------------------------------------------------------
// create
// ------------------------------------------------------------
static int raidfs_create(const char *path, mode_t mode,
                         struct fuse_file_info *fi)
{
    (void)mode;
    (void)fi;

    std::string p(path);
    g_meta->create_file(p);
    return 0;
}

// ------------------------------------------------------------
// unlink
// ------------------------------------------------------------
static int raidfs_unlink(const char *path)
{
    std::string p(path);

    if (!g_meta->exists(p))
        return -ENOENT;

    g_meta->remove_file(p);
    return 0;
}

// ------------------------------------------------------------
// open
// ------------------------------------------------------------
static int raidfs_open(const char *path, struct fuse_file_info *fi)
{
    std::string p(path);

    if (!g_meta->exists(p))
        return -ENOENT;

    return 0;
}

// ------------------------------------------------------------
// read
// ------------------------------------------------------------
static int raidfs_read(const char *path, char *buf, size_t size, off_t offset,
                       struct fuse_file_info *fi)
{
    (void)fi;

    std::string p(path);
    if (!g_meta->exists(p))
        return -ENOENT;

    std::string out;
    if (!g_fm->read(p, (uint64_t)offset, size, out))
        return -EIO;

    memcpy(buf, out.data(), out.size());
    return (int)out.size();
}

// ------------------------------------------------------------
// write
// ------------------------------------------------------------
static int raidfs_write(const char *path, const char *buf, size_t size,
                        off_t offset, struct fuse_file_info *fi)
{
    (void)fi;

    std::string p(path);
    if (!g_meta->exists(p))
        return -ENOENT;

    if (!g_fm->write(p, (uint64_t)offset, buf, size))
        return -EIO;

    return (int)size;
}

// ------------------------------------------------------------
// truncate
// ------------------------------------------------------------
static int raidfs_truncate(const char *path, off_t size,
                           struct fuse_file_info *fi)
{
    (void)fi;

    std::string p(path);
    if (!g_meta->exists(p))
        return -ENOENT;

    if (!g_fm->truncate(p, (uint64_t)size))
        return -EIO;

    return 0;
}

// ------------------------------------------------------------
// FUSE ops
// ------------------------------------------------------------
static const struct fuse_operations raidfs_ops = {};
static void init_ops() {
    raidfs_ops.getattr    = raidfs_getattr,
    raidfs_ops.readdir    = raidfs_readdir,
    raidfs_ops.create     = raidfs_create,
    raidfs_ops.unlink     = raidfs_unlink,
    raidfs_ops.truncate   = raidfs_truncate,
    raidfs_ops.open       = raidfs_open,
    raidfs_ops.read       = raidfs_read,
    raidfs_ops.write      = raidfs_write;
}

// ------------------------------------------------------------
// main
// ------------------------------------------------------------
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

    // 初始化 RAID 后端
    std::vector<std::shared_ptr<ChunkStore>> backends;
    for (int i = 2; i < 7; i++) {
        backends.push_back(std::make_shared<LocalChunkStore>(argv[i]));
    }

    int k = 4;
    int m = 1;

    auto coder = std::make_shared<RSCoder>();
    auto raid = std::make_shared<RAIDChunkStore>(backends, k, m, coder);

    // 初始化元数据与文件管理器
    g_meta = std::make_shared<MetadataManager>();
    g_fm   = std::make_shared<FileManager>(raid, g_meta);

    // 构造 FUSE 参数
    std::vector<char*> fuse_argv;
    fuse_argv.push_back(argv[0]);
    fuse_argv.push_back(argv[1]);

    for (int i = 7; i < argc; i++) {
        fuse_argv.push_back(argv[i]);
    }

    struct fuse_args args = FUSE_ARGS_INIT(
        (int)fuse_argv.size(),
        fuse_argv.data()
    );

    fuse_opt_parse(&args, NULL, NULL, NULL);

    int ret = fuse_main(args.argc, args.argv, &raidfs_ops, nullptr);
    fuse_opt_free_args(&args);

    return ret;
}
