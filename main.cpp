#define FUSE_USE_VERSION 35
#include <fuse3/fuse.h>

#include "local_chunk_store.h"
#include "webdav_chunk_store.h"
#include "raid_chunk_store.h"
#include "rs_coder.h"
#include "file_manager.h"
#include "metadata_manager.h"
#include "yml_parser.h"

#include <memory>
#include <vector>
#include <string>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <functional>

static std::shared_ptr<FileManager> g_fm;
static std::shared_ptr<MetadataManager> g_meta;

// 和 MetadataManager 中保持一致
static const char* META_PATH = "/.__cloudraidfs_meta";

// ------------------------------------------------------------
// 辅助：是否是内部元数据文件
// ------------------------------------------------------------
static inline bool is_internal_meta(const std::string& p) {
    return p == META_PATH;
}

// ------------------------------------------------------------
// getattr
// ------------------------------------------------------------
static int raidfs_getattr(const char *path, struct stat *stbuf,
                          struct fuse_file_info *fi)
{
    (void)fi;
    memset(stbuf, 0, sizeof(struct stat));

    std::string p(path);

    // 屏蔽内部元数据文件
    if (is_internal_meta(p)) {
        return -ENOENT;
    }

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
        // 不把内部元数据文件暴露出来
        if (p == "/" && name == META_PATH + 1) {
            continue;
        }
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

    if (is_internal_meta(p))
        return -EACCES;

    g_meta->create_file(p);
    return 0;
}

// ------------------------------------------------------------
// unlink
// ------------------------------------------------------------
static int raidfs_unlink(const char *path)
{
    std::string p(path);

    if (is_internal_meta(p))
        return -EACCES;

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

    if (is_internal_meta(p))
        return -EACCES;

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

    if (is_internal_meta(p))
        return -EACCES;

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

    if (is_internal_meta(p))
        return -EACCES;

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

    if (is_internal_meta(p))
        return -EACCES;

    if (!g_meta->exists(p))
        return -ENOENT;

    if (!g_fm->truncate(p, (uint64_t)size))
        return -EIO;

    return 0;
}

// ------------------------------------------------------------
// FUSE ops
// ------------------------------------------------------------
static struct fuse_operations raidfs_ops = {};
static void init_ops() {
    raidfs_ops.getattr    = raidfs_getattr;
    raidfs_ops.readdir    = raidfs_readdir;
    raidfs_ops.create     = raidfs_create;
    raidfs_ops.unlink     = raidfs_unlink;
    raidfs_ops.truncate   = raidfs_truncate;
    raidfs_ops.open       = raidfs_open;
    raidfs_ops.read       = raidfs_read;
    raidfs_ops.write      = raidfs_write;
}

// ------------------------------------------------------------
// main
// ------------------------------------------------------------
int main(int argc, char *argv[])
{
    init_ops();

    if (argc < 2) {
        std::fprintf(stderr, "用法: %s <config.yml> [FUSE options]\n", argv[0]);
        return 1;
    }

    // ------------------------------------------------------------
    // 读取 config.yml
    // ------------------------------------------------------------
    YmlParser parser;
    if (!parser.load_file(argv[1])) {
        std::fprintf(stderr, "无法读取配置文件: %s\n", argv[1]);
        return 1;
    }

    const YmlNode &root = parser.root();

    // mountpoint
    std::string mountpoint = root.map.at("mountpoint").value;

    // k, m
    int k = std::stoi(root.map.at("k").value);
    int m = std::stoi(root.map.at("m").value);

    // backends
    std::vector<std::shared_ptr<ChunkStore>> backends;

    // backends 以 map 形式存储：backend0, backend1, ...
    const auto &backend_map = root.map.at("backends").map;
    for (const auto &kv : backend_map) {
        const YmlNode &node = kv.second;

        std::string type = node.map.at("type").value;

        if (type == "local") {
            std::string path = node.map.at("path").value;
            backends.push_back(std::make_shared<LocalChunkStore>(path));
        }
        else if (type == "webdav") {
            std::string url  = node.map.at("url").value;
            std::string user = node.map.count("username")
                               ? node.map.at("username").value : "";
            std::string pass = node.map.count("password")
                               ? node.map.at("password").value : "";
            backends.push_back(std::make_shared<WebDavChunkStore>(url, user, pass));
        }
        else {
            std::fprintf(stderr, "未知后端类型: %s\n", type.c_str());
            return 1;
        }
    }

    // ------------------------------------------------------------
    // 构建 RAID 层
    // ------------------------------------------------------------
    auto coder = std::make_shared<RSCoder>();
    auto raid  = std::make_shared<RAIDChunkStore>(backends, k, m, coder);

    // ------------------------------------------------------------
    // 初始化元数据与文件管理器
    // ------------------------------------------------------------
    g_meta = std::make_shared<MetadataManager>();
    g_fm   = std::make_shared<FileManager>(raid, g_meta);

    // 元数据存储在 CloudRaidFS 内部文件中
    g_meta->load_from_backend(g_fm.get());

    // 更新 next_stripe_id，避免与已有 stripe 冲突
    {
        uint64_t max_stripe_id = 100;  // 保留 0-99 给元数据
        std::function<void(const std::string&)> scan_stripes;
        scan_stripes = [&](const std::string& dir) {
            auto entries = g_meta->list_dir(dir);
            for (const auto& name : entries) {
                std::string full_path = (dir == "/") ? ("/" + name) : (dir + "/" + name);
                if (g_meta->exists(full_path)) {
                    const auto& stripes = g_meta->get_stripes(full_path);
                    for (auto sid : stripes) {
                        if (sid >= max_stripe_id) {
                            max_stripe_id = sid + 1;
                        }
                    }
                }
                scan_stripes(full_path);
            }
        };
        scan_stripes("/");
        raid->set_next_stripe_id(max_stripe_id);
    }

    // ------------------------------------------------------------
    // 构造 FUSE 参数
    // ------------------------------------------------------------
    std::vector<char*> fuse_argv;
    fuse_argv.push_back(argv[0]);
    fuse_argv.push_back((char*)mountpoint.c_str());

    for (int i = 2; i < argc; i++) {
        fuse_argv.push_back(argv[i]);
    }

    struct fuse_args args = FUSE_ARGS_INIT(
        (int)fuse_argv.size(),
        fuse_argv.data()
    );

    fuse_opt_parse(&args, NULL, NULL, NULL);

    int ret = fuse_main(args.argc, args.argv, &raidfs_ops, nullptr);

    // 退出前保存元数据到 CloudRaidFS
    g_meta->save_to_backend(g_fm.get());
    fuse_opt_free_args(&args);

    return ret;
}
