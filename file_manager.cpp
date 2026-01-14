#include "file_manager.h"
#include <algorithm>
#include <cstring>
#include <cinttypes>
#include <iostream>

FileManager::FileManager(std::shared_ptr<RAIDChunkStore> raid_store,
                         std::shared_ptr<MetadataManager> meta_mgr,
                         std::shared_ptr<FileCache> file_cache,
                         std::shared_ptr<ChunkCache> chunk_cache)
    : raid(std::move(raid_store)),
      meta(std::move(meta_mgr)),
      file_cache_(std::move(file_cache)),
      chunk_cache_(std::move(chunk_cache))
{
}

// ------------------------------------------------------------
// 获取文件大小
// ------------------------------------------------------------
uint64_t FileManager::get_size(const std::string &path) {
    return meta->get_size(path);
}

// ------------------------------------------------------------
// 截断文件
// ------------------------------------------------------------
bool FileManager::truncate(const std::string &path, uint64_t new_size) {
    // 使文件缓存失效
    if (file_cache_) {
        file_cache_->invalidate(path);
    }
    
    // 使相关 chunk 缓存失效
    if (chunk_cache_) {
        const auto &stripes = meta->get_stripes(path);
        for (uint64_t stripe_id : stripes) {
            chunk_cache_->invalidate(stripe_id);
        }
    }
    
    meta->set_size(path, new_size);
    return true;
}

// ------------------------------------------------------------
// 读取单个 stripe（带 chunk 缓存）
// ------------------------------------------------------------
bool FileManager::read_stripe(uint64_t stripe_id, std::string &out) {
    // 先尝试从 chunk 缓存读取
    if (chunk_cache_) {
        if (chunk_cache_->get(stripe_id, out)) {
            // chunk 缓存命中
            if (out.size() < STRIPE_SIZE) {
                out.resize((size_t)STRIPE_SIZE, 0);
            }
            return true;
        }
    }

    // chunk 缓存未命中，从 RAID 读取
    if (!raid->read_chunk(stripe_id, 0, out)) {
        // stripe 不存在或解码失败 → 视为全 0
        out.assign((size_t)STRIPE_SIZE, 0);
        return true;
    }

    // 确保长度至少为一个 stripe
    if (out.size() < STRIPE_SIZE) {
        out.resize((size_t)STRIPE_SIZE, 0);
    }

    // 放入 chunk 缓存
    if (chunk_cache_) {
        chunk_cache_->put(stripe_id, out);
    }

    return true;
}

// ------------------------------------------------------------
// 写入单个 stripe（同时更新 chunk 缓存）
// ------------------------------------------------------------
bool FileManager::write_stripe(uint64_t stripe_id, const std::string &data) {
    // 写入时使 chunk 缓存失效
    if (chunk_cache_) {
        chunk_cache_->invalidate(stripe_id);
    }

    bool result = raid->write_chunk(stripe_id, 0, data);

    // 写入成功后，更新 chunk 缓存
    if (result && chunk_cache_) {
        chunk_cache_->put(stripe_id, data);
    }

    return result;
}

// ------------------------------------------------------------
// 确保 stripe 存在，不存在则分配
// ------------------------------------------------------------
uint64_t FileManager::ensure_stripe(const std::string &path, uint64_t stripe_index) {
    const auto &stripes = meta->get_stripes(path);

    // stripe 已存在
    if (stripe_index < stripes.size()) {
        return stripes[stripe_index];
    }

    // stripe 不存在 → 分配新 stripe_id 并添加到文件
    // 注意：可能需要填充中间的 stripe
    while (meta->get_stripes(path).size() <= stripe_index) {
        uint64_t new_id = raid->allocate_new_stripe();
        meta->add_stripe(path, new_id);
    }
    return meta->get_stripes(path)[stripe_index];
}

// ------------------------------------------------------------
// 从后端读取完整文件（用于文件缓存）
// ------------------------------------------------------------
bool FileManager::read_full_file(const std::string &path, std::string &out) {
    uint64_t file_size = meta->get_size(path);
    
    if (file_size == 0) {
        out.clear();
        return true;
    }
    
    out.clear();
    out.reserve(file_size);
    
    const auto &stripes = meta->get_stripes(path);
    uint64_t remaining = file_size;
    
    for (size_t i = 0; i < stripes.size() && remaining > 0; ++i) {
        std::string stripe_data;
        read_stripe(stripes[i], stripe_data);
        
        size_t to_copy = std::min<uint64_t>(remaining, STRIPE_SIZE);
        out.append(stripe_data.data(), to_copy);
        remaining -= to_copy;
    }
    
    return true;
}

// ------------------------------------------------------------
// 读取文件
// ------------------------------------------------------------
bool FileManager::read(const std::string &path,
                       uint64_t offset,
                       size_t size,
                       std::string &out)
{
    uint64_t file_size = meta->get_size(path);

    if (offset >= file_size) {
        out.clear();
        return true; // EOF
    }

    if (offset + size > file_size) {
        size = (size_t)(file_size - offset);
    }

    // 尝试从文件缓存读取（仅当读取整个文件或从头开始读取时）
    if (file_cache_ && offset == 0 && size == file_size) {
        std::string cached_data;
        if (file_cache_->get(path, cached_data)) {
            // 文件缓存命中
            out = cached_data;
            return true;
        }
        
        // 文件缓存未命中，从后端读取完整文件
        if (read_full_file(path, out)) {
            // 放入文件缓存
            file_cache_->put(path, out);
            return true;
        }
        return false;
    }
    
    // 尝试从文件缓存读取部分数据
    if (file_cache_) {
        std::string cached_data;
        if (file_cache_->get(path, cached_data)) {
            // 文件缓存命中，提取需要的部分
            if (offset + size <= cached_data.size()) {
                out = cached_data.substr(offset, size);
                return true;
            }
        }
    }

    // 文件缓存未命中，逐 stripe 读取（会使用 chunk 缓存）
    out.clear();
    out.reserve(size);

    uint64_t pos = offset;
    size_t remaining = size;

    while (remaining > 0) {
        uint64_t stripe_index = pos / STRIPE_SIZE;
        uint64_t stripe_offset = pos % STRIPE_SIZE;
        size_t to_read = std::min<uint64_t>(remaining, STRIPE_SIZE - stripe_offset);

        const auto &stripes = meta->get_stripes(path);

        std::string stripe_data;

        if (stripe_index < stripes.size()) {
            uint64_t stripe_id = stripes[stripe_index];
            read_stripe(stripe_id, stripe_data);
        } else {
            // stripe 不存在 → 全 0
            stripe_data.assign((size_t)STRIPE_SIZE, 0);
        }

        out.append(stripe_data.data() + stripe_offset, to_read);

        pos       += to_read;
        remaining -= to_read;
    }

    // 如果读取的是完整文件，放入文件缓存
    if (file_cache_ && offset == 0 && size == file_size) {
        file_cache_->put(path, out);
    }

    return true;
}

// ------------------------------------------------------------
// 写入文件
// ------------------------------------------------------------
bool FileManager::write(const std::string &path,
                        uint64_t offset,
                        const char *data,
                        size_t size)
{
    // 写入时使文件缓存失效
    if (file_cache_) {
        file_cache_->invalidate(path);
    }

    uint64_t pos = offset;
    size_t remaining = size;

    while (remaining > 0) {
        uint64_t stripe_index = pos / STRIPE_SIZE;
        uint64_t stripe_offset = pos % STRIPE_SIZE;
        size_t to_write = std::min<uint64_t>(remaining, STRIPE_SIZE - stripe_offset);

        // 确保 stripe 存在
        uint64_t stripe_id = ensure_stripe(path, stripe_index);

        // 读取旧 stripe
        std::string stripe_data;
        read_stripe(stripe_id, stripe_data);

        // 覆盖写入
        std::memcpy(&stripe_data[(size_t)stripe_offset], data, to_write);

        // 写回 RAID（同时更新 chunk 缓存）
        if (!write_stripe(stripe_id, stripe_data)) {
            fprintf(stderr,
                    "FileManager::write: write_chunk 失败, stripe_id=%" PRIu64 "\n",
                    stripe_id);
            return false;
        }

        pos       += to_write;
        data      += to_write;
        remaining -= to_write;
    }

    // 更新文件大小
    uint64_t end_pos = offset + size;
    if (end_pos > meta->get_size(path)) {
        meta->set_size(path, end_pos);
    }

    return true;
}
