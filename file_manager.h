#ifndef FILE_MANAGER_H
#define FILE_MANAGER_H

#include "raid_chunk_store.h"
#include "metadata_manager.h"
#include "file_cache.h"
#include "chunk_cache.h"
#include "async_uploader.h"
#include <cstdint>
#include <memory>
#include <string>

class FileManager {
public:
    static const uint64_t STRIPE_SIZE = 4ULL * 1024 * 1024; // 4 MiB

    FileManager(std::shared_ptr<RAIDChunkStore> raid_store,
                std::shared_ptr<MetadataManager> meta_mgr,
                std::shared_ptr<FileCache> file_cache = nullptr,
                std::shared_ptr<ChunkCache> chunk_cache = nullptr,
                std::shared_ptr<AsyncUploader> async_uploader = nullptr);

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

    // 刷新所有待上传数据（同步等待）
    void flush();

    // 同步写入（绕过异步上传，直接写入后端）
    bool sync_write(const std::string &path,
                    uint64_t offset,
                    const char *data,
                    size_t size);

    // 获取异步上传器（用于外部访问）
    std::shared_ptr<AsyncUploader> get_async_uploader() { return async_uploader_; }

private:
    std::shared_ptr<RAIDChunkStore> raid;
    std::shared_ptr<MetadataManager> meta;
    std::shared_ptr<FileCache> file_cache_;
    std::shared_ptr<ChunkCache> chunk_cache_;
    std::shared_ptr<AsyncUploader> async_uploader_;

    // 根据 offset 找到 stripe_id（不存在则自动扩展）
    uint64_t ensure_stripe(const std::string &path, uint64_t stripe_index);

    // 读取单个 stripe（带 chunk 缓存，支持从异步缓存读取）
    bool read_stripe(uint64_t stripe_id, std::string &out);

    // 写入单个 stripe（同时更新 chunk 缓存）
    bool write_stripe(uint64_t stripe_id, const std::string &data);

    // 同步写入单个 stripe（直接写入后端）
    bool sync_write_stripe(uint64_t stripe_id, const std::string &data);

    // 从后端读取完整文件（用于文件缓存）
    bool read_full_file(const std::string &path, std::string &out);
};

#endif // FILE_MANAGER_H
