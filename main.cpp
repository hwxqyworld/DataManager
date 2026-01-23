#define FUSE_USE_VERSION 35
#include <fuse3/fuse.h>

#include "local_chunk_store.h"
#include "webdav_chunk_store.h"
#include "s3_chunk_store.h"
#include "raid_chunk_store.h"
#include "rs_coder.h"
#include "file_manager.h"
#include "metadata_manager.h"
#include "file_cache.h"
#include "chunk_cache.h"
#include "async_uploader.h"
#include "yml_parser.h"

#include <memory>
#include <vector>
#include <string>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <csignal>

static std::shared_ptr<FileManager> g_fm;
static std::shared_ptr<MetadataManager> g_meta;
static std::shared_ptr<AsyncUploader> g_async_uploader;

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

    // 根目录
    if (p == "/") {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }

    // 检查是否是文件
    if (g_meta->exists(p)) {
        stbuf->st_mode = S_IFREG | 0644;
        stbuf->st_nlink = 1;
        stbuf->st_size = (off_t)g_meta->get_size(p);
        return 0;
    }

    // 检查是否是目录
    if (g_meta->is_dir(p)) {
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

    // 检查目录是否存在
    if (p != "/" && !g_meta->is_dir(p)) {
        return -ENOENT;
    }

    filler(buf, ".", nullptr, 0, FUSE_FILL_DIR_PLUS);
    filler(buf, "..", nullptr, 0, FUSE_FILL_DIR_PLUS);

    auto list = g_meta->list_dir(p);
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

    // 检查父目录是否存在
    size_t last_slash = p.rfind('/');
    if (last_slash != std::string::npos && last_slash > 0) {
        std::string parent = p.substr(0, last_slash);
        if (!g_meta->is_dir(parent)) {
            return -ENOENT;
        }
    }

    // 检查是否已存在同名目录
    if (g_meta->is_dir(p)) {
        return -EISDIR;
    }

    g_meta->create_file(p);
    return 0;
}

// ------------------------------------------------------------
// mkdir
// ------------------------------------------------------------
static int raidfs_mkdir(const char *path, mode_t mode)
{
    (void)mode;

    std::string p(path);

    if (is_internal_meta(p))
        return -EACCES;

    // 检查是否已存在同名文件
    if (g_meta->exists(p)) {
        return -EEXIST;
    }

    // 检查是否已存在同名目录
    if (g_meta->is_dir(p)) {
        return -EEXIST;
    }

    if (!g_meta->create_dir(p)) {
        return -ENOENT;  // 父目录不存在
    }

    return 0;
}

// ------------------------------------------------------------
// rmdir
// ------------------------------------------------------------
static int raidfs_rmdir(const char *path)
{
    std::string p(path);

    if (is_internal_meta(p))
        return -EACCES;

    if (p == "/")
        return -EACCES;

    // 检查是否是目录
    if (!g_meta->is_dir(p)) {
        return -ENOTDIR;
    }

    // 检查目录是否为空
    if (!g_meta->is_empty_dir(p)) {
        return -ENOTEMPTY;
    }

    if (!g_meta->remove_dir(p)) {
        return -ENOENT;
    }

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
// rename
// ------------------------------------------------------------
static int raidfs_rename(const char *from, const char *to, unsigned int flags)
{
    (void)flags;

    std::string old_path(from);
    std::string new_path(to);

    if (is_internal_meta(old_path) || is_internal_meta(new_path))
        return -EACCES;

    // 如果目标已存在，先删除
    if (g_meta->exists(new_path)) {
        g_meta->remove_file(new_path);
    } else if (g_meta->is_dir(new_path)) {
        if (!g_meta->is_empty_dir(new_path)) {
            return -ENOTEMPTY;
        }
        g_meta->remove_dir(new_path);
    }

    if (!g_meta->rename(old_path, new_path)) {
        return -ENOENT;
    }

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
// utimens - 设置文件时间戳（许多程序需要此功能）
// ------------------------------------------------------------
static int raidfs_utimens(const char *path, const struct timespec ts[2],
                          struct fuse_file_info *fi)
{
    (void)fi;
    (void)ts;

    std::string p(path);

    if (is_internal_meta(p))
        return -EACCES;

    // 检查路径是否存在（文件或目录）
    if (!g_meta->exists(p) && !g_meta->is_dir(p) && p != "/") {
        return -ENOENT;
    }

    // 目前不存储时间戳，直接返回成功
    return 0;
}

// ------------------------------------------------------------
// statfs - 文件系统统计信息
// ------------------------------------------------------------
static int raidfs_statfs(const char *path, struct statvfs *stbuf)
{
    (void)path;

    memset(stbuf, 0, sizeof(struct statvfs));

    // 返回一些合理的默认值
    stbuf->f_bsize = 4096;           // 块大小
    stbuf->f_frsize = 4096;          // 片段大小
    stbuf->f_blocks = 1024 * 1024;   // 总块数 (4GB)
    stbuf->f_bfree = 512 * 1024;     // 空闲块数 (2GB)
    stbuf->f_bavail = 512 * 1024;    // 可用块数
    stbuf->f_files = 1000000;        // 总 inode 数
    stbuf->f_ffree = 500000;         // 空闲 inode 数
    stbuf->f_favail = 500000;        // 可用 inode 数
    stbuf->f_namemax = 255;          // 最大文件名长度

    return 0;
}

// ------------------------------------------------------------
// access - 检查文件访问权限
// ------------------------------------------------------------
static int raidfs_access(const char *path, int mask)
{
    std::string p(path);

    if (is_internal_meta(p))
        return -ENOENT;

    // 根目录
    if (p == "/") {
        return 0;
    }

    // 检查文件或目录是否存在
    if (g_meta->exists(p) || g_meta->is_dir(p)) {
        return 0;
    }

    return -ENOENT;
}

// ------------------------------------------------------------
// chmod - 修改文件权限（目前不实际存储权限）
// ------------------------------------------------------------
static int raidfs_chmod(const char *path, mode_t mode,
                        struct fuse_file_info *fi)
{
    (void)fi;
    (void)mode;

    std::string p(path);

    if (is_internal_meta(p))
        return -EACCES;

    // 检查路径是否存在
    if (!g_meta->exists(p) && !g_meta->is_dir(p) && p != "/") {
        return -ENOENT;
    }

    // 目前不存储权限，直接返回成功
    return 0;
}

// ------------------------------------------------------------
// chown - 修改文件所有者（目前不实际存储所有者）
// ------------------------------------------------------------
static int raidfs_chown(const char *path, uid_t uid, gid_t gid,
                        struct fuse_file_info *fi)
{
    (void)fi;
    (void)uid;
    (void)gid;

    std::string p(path);

    if (is_internal_meta(p))
        return -EACCES;

    // 检查路径是否存在
    if (!g_meta->exists(p) && !g_meta->is_dir(p) && p != "/") {
        return -ENOENT;
    }

    // 目前不存储所有者，直接返回成功
    return 0;
}

// ------------------------------------------------------------
// flush - 刷新文件（在 close 之前调用）
// ------------------------------------------------------------
static int raidfs_flush(const char *path, struct fuse_file_info *fi)
{
    (void)path;
    (void)fi;
    return 0;
}

// ------------------------------------------------------------
// release - 关闭文件
// ------------------------------------------------------------
static int raidfs_release(const char *path, struct fuse_file_info *fi)
{
    (void)path;
    (void)fi;
    return 0;
}

// ------------------------------------------------------------
// fsync - 同步文件数据
// ------------------------------------------------------------
static int raidfs_fsync(const char *path, int isdatasync,
                        struct fuse_file_info *fi)
{
    (void)path;
    (void)isdatasync;
    (void)fi;
    
    // 可选：等待异步上传完成
    // 注意：这会阻塞直到所有数据都上传完成
    // 如果需要更细粒度的控制，可以只等待特定文件的条带
    // g_fm->flush();
    
    return 0;
}

// ------------------------------------------------------------
// opendir - 打开目录
// ------------------------------------------------------------
static int raidfs_opendir(const char *path, struct fuse_file_info *fi)
{
    (void)fi;

    std::string p(path);

    if (is_internal_meta(p))
        return -ENOENT;

    if (p == "/") {
        return 0;
    }

    if (!g_meta->is_dir(p)) {
        return -ENOTDIR;
    }

    return 0;
}

// ------------------------------------------------------------
// releasedir - 关闭目录
// ------------------------------------------------------------
static int raidfs_releasedir(const char *path, struct fuse_file_info *fi)
{
    (void)path;
    (void)fi;
    return 0;
}

// ------------------------------------------------------------
// destroy - FUSE 文件系统卸载时调用
// ------------------------------------------------------------
static void raidfs_destroy(void *private_data)
{
    (void)private_data;
    
    fprintf(stderr, "CloudRaidFS: 正在关闭...\n");
    
    // 停止异步上传器（等待队列完成）
    if (g_async_uploader) {
        fprintf(stderr, "CloudRaidFS: 等待异步上传完成...\n");
        g_async_uploader->stop();
    }
    
    // 保存元数据
    if (g_meta && g_fm) {
        fprintf(stderr, "CloudRaidFS: 保存元数据...\n");
        g_meta->save_to_backend(g_fm.get());
    }
    
    fprintf(stderr, "CloudRaidFS: 已关闭\n");
}

// ------------------------------------------------------------
// FUSE ops
// ------------------------------------------------------------
static struct fuse_operations raidfs_ops = {};
static void init_ops() {
    raidfs_ops.getattr    = raidfs_getattr;
    raidfs_ops.readdir    = raidfs_readdir;
    raidfs_ops.create     = raidfs_create;
    raidfs_ops.mkdir      = raidfs_mkdir;
    raidfs_ops.rmdir      = raidfs_rmdir;
    raidfs_ops.unlink     = raidfs_unlink;
    raidfs_ops.rename     = raidfs_rename;
    raidfs_ops.truncate   = raidfs_truncate;
    raidfs_ops.open       = raidfs_open;
    raidfs_ops.read       = raidfs_read;
    raidfs_ops.write      = raidfs_write;
    raidfs_ops.utimens    = raidfs_utimens;
    raidfs_ops.statfs     = raidfs_statfs;
    raidfs_ops.access     = raidfs_access;
    raidfs_ops.chmod      = raidfs_chmod;
    raidfs_ops.chown      = raidfs_chown;
    raidfs_ops.flush      = raidfs_flush;
    raidfs_ops.release    = raidfs_release;
    raidfs_ops.fsync      = raidfs_fsync;
    raidfs_ops.opendir    = raidfs_opendir;
    raidfs_ops.releasedir = raidfs_releasedir;
    raidfs_ops.destroy    = raidfs_destroy;
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
        else if (type == "s3") {
            std::string endpoint   = node.map.at("endpoint").value;
            std::string access_key = node.map.at("access_key").value;
            std::string secret_key = node.map.at("secret_key").value;
            std::string bucket     = node.map.at("bucket").value;
            bool use_ssl = node.map.count("use_ssl")
                           ? (node.map.at("use_ssl").value == "true") : true;
            std::string region = node.map.count("region")
                                 ? node.map.at("region").value : "";
            backends.push_back(std::make_shared<S3ChunkStore>(
                endpoint, access_key, secret_key, bucket, use_ssl, region));
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
    // 初始化文件缓存
    // ------------------------------------------------------------
    CacheConfig cache_config;
    
    // 从配置文件读取缓存参数（可选）
    if (root.map.count("cache")) {
        const auto &cache_node = root.map.at("cache");
        
        // max_cache_size: 最大缓存大小（MB），默认 256MB
        if (cache_node.map.count("max_cache_size")) {
            cache_config.max_cache_size = 
                std::stoull(cache_node.map.at("max_cache_size").value) * 1024 * 1024;
        }
        
        // max_file_size: 最大可缓存文件大小（MB），默认 32MB
        if (cache_node.map.count("max_file_size")) {
            cache_config.max_file_size = 
                std::stoull(cache_node.map.at("max_file_size").value) * 1024 * 1024;
        }
        
        // cache_ttl: 缓存过期时间（秒），默认 60 秒
        if (cache_node.map.count("cache_ttl")) {
            cache_config.cache_ttl_seconds = 
                std::stoull(cache_node.map.at("cache_ttl").value);
        }
    }
    
    std::fprintf(stderr, "文件缓存配置: max_cache_size=%lluMB, max_file_size=%lluMB, cache_ttl=%llus\n",
                 (unsigned long long)(cache_config.max_cache_size / 1024 / 1024),
                 (unsigned long long)(cache_config.max_file_size / 1024 / 1024),
                 (unsigned long long)cache_config.cache_ttl_seconds);
    
    auto file_cache = std::make_shared<FileCache>(cache_config);

    // ------------------------------------------------------------
    // 初始化 Chunk 缓存
    // ------------------------------------------------------------
    ChunkCacheConfig chunk_cache_config;
    
    // 从配置文件读取 chunk 缓存参数（可选）
    if (root.map.count("chunk_cache")) {
        const auto &chunk_cache_node = root.map.at("chunk_cache");
        
        // max_cache_size: 最大缓存大小（MB），默认 256MB
        if (chunk_cache_node.map.count("max_cache_size")) {
            chunk_cache_config.max_cache_size = 
                std::stoull(chunk_cache_node.map.at("max_cache_size").value) * 1024 * 1024;
        }
        
        // cache_ttl: 缓存过期时间（秒），默认 60 秒
        if (chunk_cache_node.map.count("cache_ttl")) {
            chunk_cache_config.cache_ttl_seconds = 
                std::stoull(chunk_cache_node.map.at("cache_ttl").value);
        }
    }
    
    std::fprintf(stderr, "Chunk缓存配置: max_cache_size=%lluMB, cache_ttl=%llus\n",
                 (unsigned long long)(chunk_cache_config.max_cache_size / 1024 / 1024),
                 (unsigned long long)chunk_cache_config.cache_ttl_seconds);
    
    auto chunk_cache = std::make_shared<ChunkCache>(chunk_cache_config);

    // ------------------------------------------------------------
    // 初始化异步上传器
    // ------------------------------------------------------------
    AsyncUploadConfig async_config;
    
    // 从配置文件读取异步上传参数（可选）
    if (root.map.count("async_upload")) {
        const auto &async_node = root.map.at("async_upload");
        
        // cache_dir: 本地缓存目录
        if (async_node.map.count("cache_dir")) {
            async_config.cache_dir = async_node.map.at("cache_dir").value;
        }
        
        // worker_threads: 后台上传线程数，默认 2
        if (async_node.map.count("worker_threads")) {
            async_config.worker_threads = 
                std::stoi(async_node.map.at("worker_threads").value);
        }
        
        // max_retries: 最大重试次数，默认 3
        if (async_node.map.count("max_retries")) {
            async_config.max_retries = 
                std::stoi(async_node.map.at("max_retries").value);
        }
        
        // retry_delay_ms: 重试间隔（毫秒），默认 1000
        if (async_node.map.count("retry_delay_ms")) {
            async_config.retry_delay_ms = 
                std::stoi(async_node.map.at("retry_delay_ms").value);
        }
        
        // max_queue_size: 最大队列长度，默认 1000
        if (async_node.map.count("max_queue_size")) {
            async_config.max_queue_size = 
                std::stoull(async_node.map.at("max_queue_size").value);
        }
    }
    
    std::fprintf(stderr, "异步上传配置: cache_dir=%s, worker_threads=%d, max_retries=%d, max_queue_size=%llu\n",
                 async_config.cache_dir.c_str(),
                 async_config.worker_threads,
                 async_config.max_retries,
                 (unsigned long long)async_config.max_queue_size);
    
    // 异步上传器需要直接访问后端和编码器，以便进行分块缓存
    g_async_uploader = std::make_shared<AsyncUploader>(backends, coder, k, m, async_config);
    
    // 恢复上次未完成的上传
    g_async_uploader->recover_pending_uploads();
    
    // 启动后台上传线程
    g_async_uploader->start();

    // ------------------------------------------------------------
    // 初始化元数据与文件管理器
    // ------------------------------------------------------------
    g_meta = std::make_shared<MetadataManager>();
    g_fm   = std::make_shared<FileManager>(raid, g_meta, file_cache, chunk_cache, g_async_uploader);

    // 元数据存储在 CloudRaidFS 内部文件中
    if (!g_meta->load_from_backend(g_fm.get())) {
        // 首次启动或元数据损坏，初始化空的元数据文件
        std::fprintf(stderr, "初始化新的元数据文件...\n");
        g_meta->save_to_backend(g_fm.get());
    }

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

    // 注意：raidfs_destroy 会在 fuse_main 返回前被调用
    // 这里不需要再次保存元数据

    fuse_opt_free_args(&args);

    return ret;
}
