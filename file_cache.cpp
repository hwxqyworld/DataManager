#include "file_cache.h"
#include <algorithm>
#include <iostream>
#include <chrono>
#include <vector>

FileCache::FileCache(const CacheConfig& config)
    : config_(config)
{
}

bool FileCache::get(const std::string& path, std::string& out) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = cache_.find(path);
    if (it == cache_.end()) {
        misses_++;
        return false;
    }

    CacheEntry& entry = it->second;

    // 检查是否过期
    auto now = std::chrono::steady_clock::now();
    if (now > entry.expire_time) {
        // 过期，移除
        remove_entry(path);
        misses_++;
        return false;
    }

    // 命中：延长过期时间
    entry.expire_time = now + std::chrono::seconds(config_.cache_ttl_seconds);
    entry.access_count++;

    // 更新 LRU
    touch_lru(path);

    out = entry.data;
    hits_++;
    return true;
}

void FileCache::put(const std::string& path, const std::string& data) {
    std::lock_guard<std::mutex> lock(mutex_);

    uint64_t data_size = data.size();

    // 文件太大，不缓存
    if (data_size > config_.max_file_size) {
        return;
    }

    // 如果已存在，先移除旧条目
    auto existing = cache_.find(path);
    if (existing != cache_.end()) {
        current_size_ -= existing->second.data.size();
        lru_list_.erase(lru_map_[path]);
        lru_map_.erase(path);
        cache_.erase(existing);
    }

    // 尝试腾出空间
    if (!make_room(data_size)) {
        // 无法腾出足够空间，放弃缓存
        return;
    }

    // 创建新条目
    CacheEntry entry;
    entry.path = path;
    entry.data = data;
    entry.file_size = data_size;
    entry.expire_time = std::chrono::steady_clock::now() + 
                        std::chrono::seconds(config_.cache_ttl_seconds);
    entry.access_count = 1;

    cache_[path] = std::move(entry);
    current_size_ += data_size;

    // 添加到 LRU 列表前面
    lru_list_.push_front(path);
    lru_map_[path] = lru_list_.begin();
}

void FileCache::invalidate(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    remove_entry(path);
}

void FileCache::cleanup_expired() {
    std::lock_guard<std::mutex> lock(mutex_);

    auto now = std::chrono::steady_clock::now();
    std::vector<std::string> expired_paths;

    for (const auto& kv : cache_) {
        if (now > kv.second.expire_time) {
            expired_paths.push_back(kv.first);
        }
    }

    for (const auto& path : expired_paths) {
        remove_entry(path);
    }
}

uint64_t FileCache::current_size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_size_;
}

void FileCache::touch_lru(const std::string& path) {
    // 假设已持有锁
    auto it = lru_map_.find(path);
    if (it != lru_map_.end()) {
        lru_list_.erase(it->second);
        lru_list_.push_front(path);
        lru_map_[path] = lru_list_.begin();
    }
}

bool FileCache::make_room(uint64_t needed_size) {
    // 假设已持有锁

    // 如果需要的空间超过最大缓存大小，无法缓存
    if (needed_size > config_.max_cache_size) {
        return false;
    }

    // 清理过期条目
    auto now = std::chrono::steady_clock::now();
    std::vector<std::string> expired_paths;
    for (const auto& kv : cache_) {
        if (now > kv.second.expire_time) {
            expired_paths.push_back(kv.first);
        }
    }
    for (const auto& path : expired_paths) {
        remove_entry(path);
    }

    // 如果空间足够，返回
    if (current_size_ + needed_size <= config_.max_cache_size) {
        return true;
    }

    // 按热度排序，移除热度最低的条目
    // 热度 = access_count / (time_since_last_access + 1)
    // 简化：使用 access_count 和 LRU 位置综合考虑

    // 收集所有条目并计算热度分数
    std::vector<std::pair<std::string, double>> scored_entries;
    for (const auto& kv : cache_) {
        double score = calc_heat_score(kv.second);
        scored_entries.push_back({kv.first, score});
    }

    // 按热度升序排序（热度低的在前面）
    std::sort(scored_entries.begin(), scored_entries.end(),
              [](const auto& a, const auto& b) {
                  return a.second < b.second;
              });

    // 移除热度最低的条目，直到空间足够
    for (const auto& entry : scored_entries) {
        if (current_size_ + needed_size <= config_.max_cache_size) {
            break;
        }
        remove_entry(entry.first);
    }

    return current_size_ + needed_size <= config_.max_cache_size;
}

void FileCache::remove_entry(const std::string& path) {
    // 假设已持有锁
    auto it = cache_.find(path);
    if (it == cache_.end()) {
        return;
    }

    current_size_ -= it->second.data.size();
    cache_.erase(it);

    auto lru_it = lru_map_.find(path);
    if (lru_it != lru_map_.end()) {
        lru_list_.erase(lru_it->second);
        lru_map_.erase(lru_it);
    }
}

double FileCache::calc_heat_score(const CacheEntry& entry) const {
    // 热度分数计算：
    // - 访问次数越多，热度越高
    // - 距离过期时间越近，热度越低（即将过期的优先移除）
    // - 文件越小，热度越高（保留小文件更划算）

    auto now = std::chrono::steady_clock::now();
    auto time_to_expire = std::chrono::duration_cast<std::chrono::seconds>(
        entry.expire_time - now).count();
    
    if (time_to_expire < 0) {
        return -1.0;  // 已过期，最低热度
    }

    // 热度 = access_count * (time_to_expire + 1) / (file_size + 1)
    double score = (double)entry.access_count * 
                   (double)(time_to_expire + 1) / 
                   (double)(entry.file_size / 1024 + 1);  // 以 KB 为单位

    return score;
}
