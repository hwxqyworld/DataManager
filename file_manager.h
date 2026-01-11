#ifndef FILE_MANAGER_H
#define FILE_MANAGER_H

#include "raid_chunk_store.h"
#include "metadata_manager.h"
#include <cstdint>
#include <memory>
#include <string>

class FileManager {
public:
    static const uint64_t STRIPE_SIZE = 4ULL * 1024 * 1024; // 4 MiB

    FileManager(std::shared_ptr<RAIDChunkStore> raid_store,
                std::shared_ptr<MetadataManager> meta_mgr);

    // 读取 [offset, offset+size)
    bool read(const std::string &path,
              uint64_t offset,
              size_t size,
              std::string &out);

    // 写入 [offset, offset+size)
    bool write(const std::string &path,
               uint64_t offset,
               const char *data,
               size_t size);

    // 获取文件大小
    uint64_t get_size(const std::string &path);

    // 截断文件
    bool truncate(const std::string &path, uint64_t new_size);

private:
    std::shared_ptr<RAIDChunkStore> raid;
    std::shared_ptr<MetadataManager> meta;

    // 根据 offset 找到 stripe_id（不存在则自动扩展）
    uint64_t ensure_stripe(const std::string &path, uint64_t stripe_index);

    // 读取单个 stripe
    bool read_stripe(uint64_t stripe_id, std::string &out);

    // 写入单个 stripe
    bool write_stripe(uint64_t stripe_id, const std::string &data);
};

#endif // FILE_MANAGER_H
