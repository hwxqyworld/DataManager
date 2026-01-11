#include "local_chunk_store.h"
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <cinttypes>

// 构造函数
LocalChunkStore::LocalChunkStore(const std::string &root_dir)
    : root(root_dir)
{
    if (!root.empty() && root.back() != '/')
        root += '/';
}

// 生成路径：root/stripes/<stripe_id>/<chunk_id>.chunk
std::string LocalChunkStore::make_path(uint64_t stripe_id,
                                       uint32_t chunk_id) const
{
    char buf[256];
    snprintf(buf, sizeof(buf),
             "%sstripes/%08lu/%02u.chunk",
             root.c_str(), stripe_id, chunk_id);
    return std::string(buf);
}

// 确保目录存在：root/stripes/<stripe_id>/
bool LocalChunkStore::ensure_dir(uint64_t stripe_id) const
{
    char dirbuf[256];
    snprintf(dirbuf, sizeof(dirbuf),
             "%sstripes/%08lu",
             root.c_str(), stripe_id);

    // 逐级创建目录
    std::string path = root + "stripes";
    mkdir(path.c_str(), 0755);

    path = std::string(dirbuf);
    mkdir(path.c_str(), 0755);

    return true;
}

// 读取 chunk
bool LocalChunkStore::read_chunk(uint64_t stripe_id,
                                 uint32_t chunk_id,
                                 std::string &out)
{
    auto path = make_path(stripe_id, chunk_id);
    FILE *fp = fopen(path.c_str(), "rb");
    if (!fp)
        return false;

    out.clear();
    char buf[4096];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), fp)) > 0) {
        out.append(buf, n);
    }

    fclose(fp);
    return true;
}

// 写入 chunk
bool LocalChunkStore::write_chunk(uint64_t stripe_id,
                                  uint32_t chunk_id,
                                  const std::string &data)
{
    ensure_dir(stripe_id);

    auto path = make_path(stripe_id, chunk_id);
    fprintf(stderr, "LOCAL_WRITE stripe=%" PRIu64 " chunk=%u path=%s size=%zu\n", stripe_id, chunk_id, path.c_str(), data.size());
    FILE *fp = fopen(path.c_str(), "wb");
    if (!fp)
        return false;

    fwrite(data.data(), 1, data.size(), fp);
    fclose(fp);
    return true;
}

// 删除 chunk
bool LocalChunkStore::delete_chunk(uint64_t stripe_id,
                                   uint32_t chunk_id)
{
    auto path = make_path(stripe_id, chunk_id);
    return unlink(path.c_str()) == 0;
}
