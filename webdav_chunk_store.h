#ifndef WEBDAV_CHUNK_STORE_H
#define WEBDAV_CHUNK_STORE_H

#include "chunk_store.h"
#include <neon/ne_session.h>
#include <neon/ne_request.h>
#include <neon/ne_basic.h>
#include <neon/ne_auth.h>
#include <neon/ne_uri.h>
#include <string>
#include <mutex>
#include <queue>
#include <unordered_set>
#include <memory>

// Neon 会话池，避免频繁创建/销毁连接
class NeonPool {
public:
    explicit NeonPool(const std::string& scheme,
                      const std::string& host,
                      int port,
                      const std::string& username,
                      const std::string& password,
                      size_t max_size = 16);
    
    ~NeonPool();
    
    ne_session* acquire();
    void release(ne_session* sess);

private:
    std::string scheme_;
    std::string host_;
    int port_;
    std::string username_;
    std::string password_;
    
    std::queue<ne_session*> pool_;
    std::mutex mu_;
    size_t max_size_;
    
    ne_session* create_session();
    static int auth_callback(void* userdata, const char* realm, int attempt,
                             char* username, char* password);
};

// RAII 包装器，自动归还 Neon 会话
class NeonHandle {
public:
    NeonHandle(NeonPool& pool) : pool_(pool), sess_(pool.acquire()) {}
    ~NeonHandle() { pool_.release(sess_); }
    
    ne_session* get() { return sess_; }
    operator ne_session*() { return sess_; }
    
    // 禁止拷贝
    NeonHandle(const NeonHandle&) = delete;
    NeonHandle& operator=(const NeonHandle&) = delete;

private:
    NeonPool& pool_;
    ne_session* sess_;
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
    std::string base_url_;
    std::string base_path_;
    std::string username_;
    std::string password_;
    
    // 连接池
    std::unique_ptr<NeonPool> neon_pool_;
    
    // 已创建目录缓存（避免重复 MKCOL）
    std::unordered_set<uint64_t> created_stripe_dirs_;
    std::mutex dir_cache_mu_;
    bool stripes_dir_created_ = false;

    std::string make_path(uint64_t stripe_id, uint32_t chunk_index) const;
    std::string make_dir_path(const std::string& rel_path) const;
    
    // WebDAV MKCOL 创建目录
    bool mkcol(ne_session* sess, const std::string& path);
    
    // 确保 stripe 目录存在（带缓存）
    void ensure_stripe_dir(uint64_t stripe_id);
};

#endif
