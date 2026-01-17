#ifndef S3_CHUNK_STORE_H
#define S3_CHUNK_STORE_H

#include "chunk_store.h"
#include <string>
#include <mutex>
#include <memory>

// 前向声明 minio-cpp 类型
namespace minio {
namespace s3 {
    class Client;
    class BaseUrl;
}
namespace creds {
    class StaticProvider;
}
}

// S3 实现的 ChunkStore（使用 minio-cpp SDK）
// 每个 chunk 对应一个对象：
//   <bucket>/stripes/<stripe_id>/<chunk_id>.chunk

class S3ChunkStore : public ChunkStore {
public:
    S3ChunkStore(const std::string& endpoint,
                 const std::string& access_key,
                 const std::string& secret_key,
                 const std::string& bucket,
                 bool use_ssl = true,
                 const std::string& region = "");

    ~S3ChunkStore();

    bool read_chunk(uint64_t stripe_id,
                    uint32_t chunk_id,
                    std::string& out) override;

    bool write_chunk(uint64_t stripe_id,
                     uint32_t chunk_id,
                     const std::string& data) override;

    bool delete_chunk(uint64_t stripe_id,
                      uint32_t chunk_id) override;

private:
    std::string endpoint_;
    std::string access_key_;
    std::string secret_key_;
    std::string bucket_;
    std::string region_;
    bool use_ssl_;

    // minio-cpp 凭证提供者（必须在 client_ 之前声明，确保生命周期）
    std::unique_ptr<minio::creds::StaticProvider> creds_provider_;

    // minio-cpp 客户端
    std::unique_ptr<minio::s3::Client> client_;
    std::mutex client_mu_;

    // 确保 bucket 存在
    bool bucket_exists_checked_ = false;
    std::mutex bucket_mu_;
    void ensure_bucket();

    // 生成对象 key
    std::string make_object_key(uint64_t stripe_id, uint32_t chunk_id) const;
};

#endif // S3_CHUNK_STORE_H
