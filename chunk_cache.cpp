#include "chunk_cache.h"
#include <algorithm>
#include <vector>

ChunkCache::ChunkCache(const ChunkCacheConfig& config)
    : config_(config)
{
}

bool ChunkCache::get(uint64_t stripe_id, std::string& out) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = cache_.find(stripe_id);
    if (it == cache_.end()) {
        misses_++;
        return false;
    }

    ChunkCacheEntry& entry = it->second;

    // 检查是否过期
    auto now = std::chrono::steady_clock::now();
    if (now > entry.expire_time) {
        // 过期，移除
        remove_entry(stripe_id);
        misses_++;
        return false;
    }

    // 命中：延长过期时间
    entry.expire_time = now + std::chrono::seconds(config_.cache_ttl_seconds);
    entry.access_count++;

    // 更新 LRU
    touch_lru(stripe_id);

    out = entry.data;
    hits_++;
    return true;
}

void ChunkCache::put(uint64_t stripe_id, const std::string& data) {
    std::lock_guard<std::mutex> lock(mutex_);

    uint64_t data_size = data.size();

    // 如果已存在，先移除旧条目
    auto existing = cache_.find(stripe_id);
    if (existing != cache_.end()) {
        current_size_ -= existing->second.data.size();
        lru_list_.erase(lru_map_[stripe_id]);
        lru_map_.erase(stripe_id);
        cache_.erase(existing);
    }

    // 尝试腾出空间
    if (!make_room(data_size)) {
        // 无法腾出足够空间，放弃缓存
        return;
    }

    // 创建新条目
    ChunkCacheEntry entry;
    entry.stripe_id = stripe_id;
    entry.data = data;
    entry.expire_time = std::chrono::steady_clock::now() + 
                        std::chrono::seconds(config_.cache_ttl_seconds);
    entry.access_count = 1;

    cache_[stripe_id] = std::move(entry);
    current_size_ += data_size;

    // 添加到 LRU 列表前面
    lru_list_.push_front(stripe_id);
    lru_map_[stripe_id] = lru_list_.begin();
}

void ChunkCache::invalidate(uint64_t stripe_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    remove_entry(stripe_id);
}

void ChunkCache::cleanup_expired() {
    std::lock_guard<std::mutex> lock(mutex_);

    auto now = std::chrono::steady_clock::now();
    std::vector<uint64_t> expired_ids;

    for (const auto& kv : cache_) {
        if (now > kv.second.expire_time) {
            expired_ids.push_back(kv.first);
        }
    }

    for (uint64_t id : expired_ids) {
        remove_entry(id);
    }
}

uint64_t ChunkCache::current_size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_size_;
}

void ChunkCache::touch_lru(uint64_t stripe_id) {
    // 假设已持有锁
    auto it = lru_map_.find(stripe_id);
    if (it != lru_map_.end()) {
        lru_list_.erase(it->second);
        lru_list_.push_front(stripe_id);
        lru_map_[stripe_id] = lru_list_.begin();
    }
}

bool ChunkCache::make_room(uint64_t needed_size) {
    // 假设已持有锁

    // 如果需要的空间超过最大缓存大小，无法缓存
    if (needed_size > config_.max_cache_size) {
        return false;
    }

    // 清理过期条目
    auto now = std::chrono::steady_clock::now();
    std::vector<uint64_t> expired_ids;
    for (const auto& kv : cache_) {
        if (now > kv.second.expire_time) {
            expired_ids.push_back(kv.first);
        }
    }
    for (uint64_t id : expired_ids) {
        remove_entry(id);
    }

    // 如果空间足够，返回
    if (current_size_ + needed_size <= config_.max_cache_size) {
        return true;
    }

    // 按热度排序，移除热度最低的条目
    std::vector<std::pair<uint64_t, double>> scored_entries;
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

void ChunkCache::remove_entry(uint64_t stripe_id) {
    // 假设已持有锁
    auto it = cache_.find(stripe_id);
    if (it == cache_.end()) {
        return;
    }

    current_size_ -= it->second.data.size();
    cache_.erase(it);

    auto lru_it = lru_map_.find(stripe_id);
    if (lru_it != lru_map_.end()) {
        lru_list_.erase(lru_it->second);
        lru_map_.erase(lru_it);
    }
}

double ChunkCache::calc_heat_score(const ChunkCacheEntry& entry) const {
    // 热度分数计算：
    // - 访问次数越多，热度越高
    // - 距离过期时间越近，热度越低（即将过期的优先移除）

    auto now = std::chrono::steady_clock::now();
    auto time_to_expire = std::chrono::duration_cast<std::chrono::seconds>(
        entry.expire_time - now).count();
    
    if (time_to_expire < 0) {
        return -1.0;  // 已过期，最低热度
    }

    // 热度 = access_count * (time_to_expire + 1)
    double score = (double)entry.access_count * (double)(time_to_expire + 1);

    return score;
}
