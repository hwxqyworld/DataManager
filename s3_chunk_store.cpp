#include "s3_chunk_store.h"

#include <cstdio>
#include <sstream>

// minio-cpp headers
#include <miniocpp/client.h>

// 重试次数
static const int S3_MAX_RETRIES = 3;

// 构造函数
S3ChunkStore::S3ChunkStore(const std::string& endpoint,
                           const std::string& access_key,
                           const std::string& secret_key,
                           const std::string& bucket,
                           bool use_ssl,
                           const std::string& region)
    : endpoint_(endpoint),
      access_key_(access_key),
      secret_key_(secret_key),
      bucket_(bucket),
      region_(region.empty() ? "us-east-1" : region),
      use_ssl_(use_ssl)
{
    // 构建 endpoint URL
    std::string full_endpoint = endpoint_;
    if (full_endpoint.find("://") == std::string::npos) {
        full_endpoint = (use_ssl_ ? "https://" : "http://") + full_endpoint;
    }
    
    // 创建 minio-cpp 客户端
    minio::s3::BaseUrl base_url(full_endpoint);
    base_url.https = use_ssl_;
    
    // 创建凭证提供者（保存为成员变量，确保生命周期）
    creds_provider_ = std::make_unique<minio::creds::StaticProvider>(access_key_, secret_key_);
    
    // 创建客户端（使用成员变量的指针）
    client_ = std::make_unique<minio::s3::Client>(base_url, creds_provider_.get());
    
    std::fprintf(stderr, "S3ChunkStore: initialized with endpoint=%s, bucket=%s, ssl=%s\n",
                 endpoint_.c_str(), bucket_.c_str(), use_ssl_ ? "true" : "false");
}

// 析构函数
S3ChunkStore::~S3ChunkStore() = default;

// 确保 bucket 存在
void S3ChunkStore::ensure_bucket() {
    std::lock_guard<std::mutex> lock(bucket_mu_);

    if (bucket_exists_checked_) {
        return;
    }

    // 检查 bucket 是否存在
    minio::s3::BucketExistsArgs exists_args;
    exists_args.bucket = bucket_;

    minio::s3::BucketExistsResponse exists_resp;
    {
        std::lock_guard<std::mutex> client_lock(client_mu_);
        exists_resp = client_->BucketExists(exists_args);
    }

    if (!exists_resp) {
        std::fprintf(stderr,
                     "S3ChunkStore::ensure_bucket: BucketExists check failed: %s\n",
                     exists_resp.Error().String().c_str());
        return;  // 下次重试
    }

    if (!exists_resp.exist) {
        std::fprintf(stderr,
                     "S3ChunkStore::ensure_bucket: bucket %s does not exist, creating...\n",
                     bucket_.c_str());

        minio::s3::MakeBucketArgs make_args;
        make_args.bucket = bucket_;
        if (!region_.empty() && region_ != "us-east-1") {
            make_args.region = region_;
        }

        minio::s3::MakeBucketResponse make_resp;
        {
            std::lock_guard<std::mutex> client_lock(client_mu_);
            make_resp = client_->MakeBucket(make_args);
        }

        if (!make_resp) {
            std::fprintf(stderr,
                         "S3ChunkStore::ensure_bucket: CreateBucket failed: %s\n",
                         make_resp.Error().String().c_str());
            return;  // 下次重试
        }

        std::fprintf(stderr,
                     "S3ChunkStore::ensure_bucket: created bucket %s\n",
                     bucket_.c_str());
    }

    bucket_exists_checked_ = true;
}


// 生成对象 key
std::string S3ChunkStore::make_object_key(uint64_t stripe_id, uint32_t chunk_id) const
{
    char buf[256];
    std::snprintf(buf, sizeof(buf), "stripes/%08llu/%02u.chunk",
                  (unsigned long long)stripe_id,
                  (unsigned int)chunk_id);
    return std::string(buf);
}

// 读取 chunk
bool S3ChunkStore::read_chunk(uint64_t stripe_id,
                              uint32_t chunk_id,
                              std::string& out)
{
    ensure_bucket();
    
    std::string object_key = make_object_key(stripe_id, chunk_id);
    
    out.clear();
    
    minio::s3::GetObjectArgs args;
    args.bucket = bucket_;
    args.object = object_key;
    
    // 使用回调函数收集数据
    std::ostringstream data_stream;
    args.datafunc = [&data_stream](minio::http::DataFunctionArgs args) -> bool {
        data_stream.write(args.datachunk.data(), args.datachunk.size());
        return true;
    };
    
    std::lock_guard<std::mutex> lock(client_mu_);
    minio::s3::GetObjectResponse resp = client_->GetObject(args);
    
    if (resp) {
        out = data_stream.str();
        return true;
    }
    
    // 检查是否是 404 Not Found（对象不存在是正常情况，不需要重试或打印错误）
    // minio-cpp 返回的错误信息可能包含 "404"、"NoSuchKey"、"ResourceNotFound" 等
    std::string error_code = resp.code;
    std::string error_msg = resp.Error().String();
    if (error_code == "NoSuchKey" || error_code == "ResourceNotFound" ||
        error_msg.find("404") != std::string::npos ||
        error_msg.find("NoSuchKey") != std::string::npos ||
        error_msg.find("Not Found") != std::string::npos) {
        // 对象不存在，静默返回 false（首次启动时这是正常情况）
        out.clear();
        return false;
    }
    
    // 其他错误才需要重试
    for (int attempt = 1; attempt < S3_MAX_RETRIES; ++attempt) {
        std::fprintf(stderr, "S3ChunkStore::read_chunk: %s attempt %d failed: %s\n",
                     object_key.c_str(), attempt, error_msg.c_str());
        
        out.clear();
        data_stream.str("");
        data_stream.clear();
        
        minio::s3::GetObjectResponse retry_resp = client_->GetObject(args);
        
        if (retry_resp) {
            out = data_stream.str();
            return true;
        }
        
        error_code = retry_resp.code;
        error_msg = retry_resp.Error().String();
        if (error_code == "NoSuchKey" || error_code == "ResourceNotFound" ||
            error_msg.find("404") != std::string::npos ||
            error_msg.find("NoSuchKey") != std::string::npos ||
            error_msg.find("Not Found") != std::string::npos) {
            out.clear();
            return false;
        }
    }
    
    std::fprintf(stderr, "S3ChunkStore::read_chunk: %s failed after %d retries: %s\n",
                 object_key.c_str(), S3_MAX_RETRIES, error_msg.c_str());
    return false;
}

// 写入 chunk
bool S3ChunkStore::write_chunk(uint64_t stripe_id,
                               uint32_t chunk_id,
                               const std::string& data)
{
    ensure_bucket();
    
    std::string object_key = make_object_key(stripe_id, chunk_id);
    
    for (int attempt = 0; attempt < S3_MAX_RETRIES; ++attempt) {
        // 创建输入流
        std::istringstream data_stream(data);
        
        minio::s3::PutObjectArgs args(data_stream, static_cast<long>(data.size()), 0);
        args.bucket = bucket_;
        args.object = object_key;
        args.content_type = "application/octet-stream";
        
        std::lock_guard<std::mutex> lock(client_mu_);
        minio::s3::PutObjectResponse resp = client_->PutObject(args);
        
        if (resp) {
            return true;
        }
        
        std::fprintf(stderr, "S3ChunkStore::write_chunk: %s attempt %d failed: %s\n",
                     object_key.c_str(), attempt + 1, resp.Error().String().c_str());
    }
    
    std::fprintf(stderr, "S3ChunkStore::write_chunk: %s failed after %d retries\n",
                 object_key.c_str(), S3_MAX_RETRIES);
    return false;
}

// 删除 chunk
bool S3ChunkStore::delete_chunk(uint64_t stripe_id, uint32_t chunk_id)
{
    ensure_bucket();
    
    std::string object_key = make_object_key(stripe_id, chunk_id);
    
    for (int attempt = 0; attempt < S3_MAX_RETRIES; ++attempt) {
        minio::s3::RemoveObjectArgs args;
        args.bucket = bucket_;
        args.object = object_key;
        
        std::lock_guard<std::mutex> lock(client_mu_);
        minio::s3::RemoveObjectResponse resp = client_->RemoveObject(args);
        
        if (resp) {
            return true;
        }
        
        // S3 删除不存在的对象通常不报错，但检查一下
        std::string error_code = resp.code;
        if (error_code == "NoSuchKey" || error_code == "ResourceNotFound") {
            return true;  // 对象本来就不存在，视为成功
        }
        
        std::fprintf(stderr, "S3ChunkStore::delete_chunk: %s attempt %d failed: %s\n",
                     object_key.c_str(), attempt + 1, resp.Error().String().c_str());
    }
    
    std::fprintf(stderr, "S3ChunkStore::delete_chunk: %s failed after %d retries\n",
                 object_key.c_str(), S3_MAX_RETRIES);
    return false;
}
