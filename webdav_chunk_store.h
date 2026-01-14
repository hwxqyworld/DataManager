#ifndef WEBDAV_CHUNK_STORE_H
#define WEBDAV_CHUNK_STORE_H

#include "chunk_store.h"
#include <curl/curl.h>
#include <string>
#include <mutex>
#include <queue>
#include <unordered_set>
#include <memory>

// CURL 句柄池，避免频繁创建/销毁连接
class CurlPool {
public:
    explicit CurlPool(size_t max_size = 16) : max_size_(max_size) {}
    
    ~CurlPool() {
        std::lock_guard<std::mutex> lock(mu_);
        while (!pool_.empty()) {
            curl_easy_cleanup(pool_.front());
            pool_.pop();
        }
    }
    
    CURL* acquire() {
        std::lock_guard<std::mutex> lock(mu_);
        if (!pool_.empty()) {
            CURL* curl = pool_.front();
            pool_.pop();
            curl_easy_reset(curl);
            return curl;
        }
        return curl_easy_init();
    }
    
    void release(CURL* curl) {
        if (!curl) return;
        std::lock_guard<std::mutex> lock(mu_);
        if (pool_.size() < max_size_) {
            pool_.push(curl);
        } else {
            curl_easy_cleanup(curl);
        }
    }

private:
    std::queue<CURL*> pool_;
    std::mutex mu_;
    size_t max_size_;
};

// RAII 包装器，自动归还 CURL 句柄
class CurlHandle {
public:
    CurlHandle(CurlPool& pool) : pool_(pool), curl_(pool.acquire()) {}
    ~CurlHandle() { pool_.release(curl_); }
    
    CURL* get() { return curl_; }
    operator CURL*() { return curl_; }
    
    // 禁止拷贝
    CurlHandle(const CurlHandle&) = delete;
    CurlHandle& operator=(const CurlHandle&) = delete;

private:
    CurlPool& pool_;
    CURL* curl_;
};

class WebDavChunkStore : public ChunkStore {
public:
    WebDavChunkStore(const std::string& base_url,
                     const std::string& username = "",
                     const std::string& password = "");

    ~WebDavChunkStore();

    bool read_chunk(uint64_t stripe_id,
                    uint32_t chunk_index,
                    std::string& out) override;

    bool write_chunk(uint64_t stripe_id,
                     uint32_t chunk_index,
                     const std::string& data) override;

    bool delete_chunk(uint64_t stripe_id,
                      uint32_t chunk_id) override;

private:
    std::string base_url;
    std::string username;
    std::string password;
    
    // 连接池（替代单个互斥锁）
    mutable CurlPool curl_pool_;
    
    // 已创建目录缓存（避免重复 MKCOL）
    std::unordered_set<uint64_t> created_stripe_dirs_;
    std::mutex dir_cache_mu_;
    bool stripes_dir_created_ = false;

    std::string make_url(uint64_t stripe_id, uint32_t chunk_index) const;
    std::string make_dir_url(const std::string& rel_path) const;
    
    // WebDAV MKCOL 创建目录
    bool mkcol(CURL* curl, const std::string& url);
    
    // 确保 stripe 目录存在（带缓存）
    void ensure_stripe_dir(uint64_t stripe_id);
    
    // 设置 CURL 通用选项（认证等）
    void setup_curl_auth(CURL* curl);
};

#endif
