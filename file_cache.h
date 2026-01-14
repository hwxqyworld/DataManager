#ifndef FILE_CACHE_H
#define FILE_CACHE_H

#include <string>
#include <unordered_map>
#include <list>
#include <mutex>
#include <chrono>
#include <cstdint>

// 文件内存缓存
// - 缓存小于 max_file_size 的文件
// - 使用 LRU + 过期时间策略
// - 访问时自动延长过期时间
// - 缓存区满时移除热度最低的文件

struct CacheEntry {
    std::string path;
    std::string data;
    uint64_t file_size;
    std::chrono::steady_clock::time_point expire_time;
    uint64_t access_count;  // 访问次数，用于热度计算
};

struct CacheConfig {
    uint64_t max_cache_size = 256ULL * 1024 * 1024;  // 最大缓存大小，默认 256MB
    uint64_t max_file_size = 32ULL * 1024 * 1024;    // 最大可缓存文件大小，默认 32MB
    uint64_t cache_ttl_seconds = 60;                  // 缓存过期时间，默认 60 秒
};

class FileCache {
public:
    explicit FileCache(const CacheConfig& config = CacheConfig());

    // 尝试从缓存获取文件内容
    // 如果命中，自动延长过期时间
    // 返回 true 表示命中，out 填充数据
    bool get(const std::string& path, std::string& out);

    // 将文件放入缓存
    // 如果文件太大或缓存已满（且无法腾出空间），则不缓存
    void put(const std::string& path, const std::string& data);

    // 使缓存失效（文件被修改或删除时调用）
    void invalidate(const std::string& path);

    // 清理过期条目
    void cleanup_expired();

    // 获取当前缓存使用量
    uint64_t current_size() const;

    // 获取缓存命中统计
    uint64_t hit_count() const { return hits_; }
    uint64_t miss_count() const { return misses_; }

private:
    CacheConfig config_;
    
    // path -> CacheEntry
    std::unordered_map<std::string, CacheEntry> cache_;
    
    // LRU 列表：最近访问的在前面
    std::list<std::string> lru_list_;
    
    // path -> lru_list_ 中的迭代器（用于快速移动到前面）
    std::unordered_map<std::string, std::list<std::string>::iterator> lru_map_;
    
    uint64_t current_size_ = 0;
    uint64_t hits_ = 0;
    uint64_t misses_ = 0;
    
    mutable std::mutex mutex_;

    // 移动到 LRU 列表前面
    void touch_lru(const std::string& path);

    // 腾出空间以容纳 needed_size 字节
    // 返回 true 表示成功腾出空间
    bool make_room(uint64_t needed_size);

    // 移除一个条目
    void remove_entry(const std::string& path);

    // 计算条目热度分数（越低越容易被移除）
    double calc_heat_score(const CacheEntry& entry) const;
};

#endif // FILE_CACHE_H
