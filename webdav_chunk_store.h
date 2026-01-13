#ifndef WEBDAV_CHUNK_STORE_H
#define WEBDAV_CHUNK_STORE_H

#include "chunk_store.h"
#include <curl/curl.h>
#include <string>
#include <mutex>

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
    std::mutex mu;

    std::string make_url(uint64_t stripe_id, uint32_t chunk_index) const;
    std::string make_dir_url(const std::string& rel_path) const;
    
    // WebDAV MKCOL 创建目录
    bool mkcol(const std::string& url);
    
    // 设置 CURL 通用选项（认证等）
    void setup_curl_auth(CURL* curl);
};

#endif
