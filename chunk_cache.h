#ifndef CHUNK_CACHE_H
#define CHUNK_CACHE_H

#include <string>
#include <unordered_map>
#include <list>
#include <mutex>
#include <chrono>
#include <cstdint>

// Chunk 内存缓存
// - 缓存 stripe/chunk 数据（通常为 4MB）
// - 使用 LRU + 过期时间策略
// - 访问时自动延长过期时间
// - 缓存区满时移除热度最低的 chunk

struct ChunkCacheEntry {
    uint64_t stripe_id;
    std::string data;
    std::chrono::steady_clock::time_point expire_time;
    uint64_t access_count;  // 访问次数，用于热度计算
};

struct ChunkCacheConfig {
    uint64_t max_cache_size = 256ULL * 1024 * 1024;  // 最大缓存大小，默认 256MB
    uint64_t cache_ttl_seconds = 60;                  // 缓存过期时间，默认 60 秒
};

class ChunkCache {
public:
    explicit ChunkCache(const ChunkCacheConfig& config = ChunkCacheConfig());

    // 尝试从缓存获取 chunk 内容
    // 如果命中，自动延长过期时间
    // 返回 true 表示命中，out 填充数据
    bool get(uint64_t stripe_id, std::string& out);

    // 将 chunk 放入缓存
    // 如果缓存已满（且无法腾出空间），则不缓存
    void put(uint64_t stripe_id, const std::string& data);

    // 使缓存失效（chunk 被修改或删除时调用）
    void invalidate(uint64_t stripe_id);

    // 清理过期条目
    void cleanup_expired();

    // 获取当前缓存使用量
    uint64_t current_size() const;

    // 获取缓存命中统计
    uint64_t hit_count() const { return hits_; }
    uint64_t miss_count() const { return misses_; }

private:
    ChunkCacheConfig config_;
    
    // stripe_id -> ChunkCacheEntry
    std::unordered_map<uint64_t, ChunkCacheEntry> cache_;
    
    // LRU 列表：最近访问的在前面
    std::list<uint64_t> lru_list_;
    
    // stripe_id -> lru_list_ 中的迭代器（用于快速移动到前面）
    std::unordered_map<uint64_t, std::list<uint64_t>::iterator> lru_map_;
    
    uint64_t current_size_ = 0;
    uint64_t hits_ = 0;
    uint64_t misses_ = 0;
    
    mutable std::mutex mutex_;

    // 移动到 LRU 列表前面
    void touch_lru(uint64_t stripe_id);

    // 腾出空间以容纳 needed_size 字节
    // 返回 true 表示成功腾出空间
    bool make_room(uint64_t needed_size);

    // 移除一个条目
    void remove_entry(uint64_t stripe_id);

    // 计算条目热度分数（越低越容易被移除）
    double calc_heat_score(const ChunkCacheEntry& entry) const;
};

#endif // CHUNK_CACHE_H
