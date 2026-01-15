#include "s3_chunk_store.h"

#include <cstdio>
#include <sstream>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>

// 重试次数
static const int S3_MAX_RETRIES = 3;

// AWS SDK 初始化管理（全局只初始化一次）
static struct AwsSdkInitializer {
    AwsSdkInitializer() {
        Aws::SDKOptions options;
        Aws::InitAPI(options);
    }
    ~AwsSdkInitializer() {
        Aws::SDKOptions options;
        Aws::ShutdownAPI(options);
    }
} s_aws_sdk_initializer;

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
    // 创建 AWS S3 客户端配置
    Aws::Client::ClientConfiguration config;
    config.region = region_;
    config.scheme = use_ssl_ ? Aws::Http::Scheme::HTTPS : Aws::Http::Scheme::HTTP;
    
    // 设置自定义 endpoint（用于 MinIO 等兼容 S3 的服务）
    if (!endpoint_.empty()) {
        // 确保 endpoint 格式正确
        std::string full_endpoint = endpoint_;
        if (full_endpoint.find("://") == std::string::npos) {
            full_endpoint = (use_ssl_ ? "https://" : "http://") + full_endpoint;
        }
        config.endpointOverride = full_endpoint;
    }
    
    // 创建凭证
    Aws::Auth::AWSCredentials credentials(access_key_, secret_key_);
    
    // 创建 S3 客户端
    // 使用 path-style 访问（对 MinIO 等兼容服务更友好）
    client_ = std::make_shared<Aws::S3::S3Client>(
        credentials,
        config,
        Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
        false  // useVirtualAddressing = false，使用 path-style
    );
    
    std::fprintf(stderr, "S3ChunkStore: initialized with endpoint=%s, bucket=%s, ssl=%s\n",
                 endpoint_.c_str(), bucket_.c_str(), use_ssl_ ? "true" : "false");
}

// 析构函数
S3ChunkStore::~S3ChunkStore()
{
    // client_ 会自动销毁
}

// 确保 bucket 存在
void S3ChunkStore::ensure_bucket()
{
    std::lock_guard<std::mutex> lock(bucket_mu_);
    
    if (bucket_exists_checked_) {
        return;
    }
    
    // 检查 bucket 是否存在
    Aws::S3::Model::HeadBucketRequest head_request;
    head_request.SetBucket(bucket_);
    
    auto head_outcome = client_->HeadBucket(head_request);
    
    if (!head_outcome.IsSuccess()) {
        auto error = head_outcome.GetError();
        std::fprintf(stderr, "S3ChunkStore::ensure_bucket: HeadBucket failed: %s\n",
                     error.GetMessage().c_str());
        
        // 尝试创建 bucket
        Aws::S3::Model::CreateBucketRequest create_request;
        create_request.SetBucket(bucket_);
        
        // 如果不是 us-east-1，需要设置 LocationConstraint
        if (region_ != "us-east-1") {
            Aws::S3::Model::CreateBucketConfiguration bucket_config;
            bucket_config.SetLocationConstraint(
                Aws::S3::Model::BucketLocationConstraintMapper::GetBucketLocationConstraintForName(region_));
            create_request.SetCreateBucketConfiguration(bucket_config);
        }
        
        auto create_outcome = client_->CreateBucket(create_request);
        if (!create_outcome.IsSuccess()) {
            std::fprintf(stderr, "S3ChunkStore::ensure_bucket: CreateBucket failed: %s\n",
                         create_outcome.GetError().GetMessage().c_str());
        } else {
            std::fprintf(stderr, "S3ChunkStore::ensure_bucket: created bucket %s\n",
                         bucket_.c_str());
        }
    }
    
    bucket_exists_checked_ = true;
}

// 生成对象 key
std::string S3ChunkStore::make_object_key(uint64_t stripe_id, uint32_t chunk_id) const
{
    char buf[256];
    std::snprintf(buf, sizeof(buf), "stripes/%08lu/%02u.chunk",
                  (unsigned long)stripe_id,
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
    
    for (int attempt = 0; attempt < S3_MAX_RETRIES; ++attempt) {
        out.clear();
        
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(bucket_);
        request.SetKey(object_key);
        
        std::lock_guard<std::mutex> lock(client_mu_);
        auto outcome = client_->GetObject(request);
        
        if (outcome.IsSuccess()) {
            // 读取响应体
            auto& body = outcome.GetResult().GetBody();
            std::ostringstream ss;
            ss << body.rdbuf();
            out = ss.str();
            return true;
        }
        
        // 检查是否是 404 Not Found
        auto error = outcome.GetError();
        auto error_type = error.GetErrorType();
        if (error_type == Aws::S3::S3Errors::NO_SUCH_KEY ||
            error_type == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
            out.clear();
            return false;
        }
        
        std::fprintf(stderr, "S3ChunkStore::read_chunk: %s attempt %d failed: %s\n",
                     object_key.c_str(), attempt + 1, error.GetMessage().c_str());
    }
    
    std::fprintf(stderr, "S3ChunkStore::read_chunk: %s failed after %d retries\n",
                 object_key.c_str(), S3_MAX_RETRIES);
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
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(bucket_);
        request.SetKey(object_key);
        request.SetContentType("application/octet-stream");
        
        // 创建输入流
        auto input_data = Aws::MakeShared<Aws::StringStream>("S3ChunkStore");
        input_data->write(data.data(), data.size());
        request.SetBody(input_data);
        request.SetContentLength(data.size());
        
        std::lock_guard<std::mutex> lock(client_mu_);
        auto outcome = client_->PutObject(request);
        
        if (outcome.IsSuccess()) {
            return true;
        }
        
        std::fprintf(stderr, "S3ChunkStore::write_chunk: %s attempt %d failed: %s\n",
                     object_key.c_str(), attempt + 1, outcome.GetError().GetMessage().c_str());
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
        Aws::S3::Model::DeleteObjectRequest request;
        request.SetBucket(bucket_);
        request.SetKey(object_key);
        
        std::lock_guard<std::mutex> lock(client_mu_);
        auto outcome = client_->DeleteObject(request);
        
        if (outcome.IsSuccess()) {
            return true;
        }
        
        // S3 删除不存在的对象通常不报错，但检查一下
        auto error = outcome.GetError();
        auto error_type = error.GetErrorType();
        if (error_type == Aws::S3::S3Errors::NO_SUCH_KEY ||
            error_type == Aws::S3::S3Errors::RESOURCE_NOT_FOUND) {
            return true;  // 对象本来就不存在，视为成功
        }
        
        std::fprintf(stderr, "S3ChunkStore::delete_chunk: %s attempt %d failed: %s\n",
                     object_key.c_str(), attempt + 1, error.GetMessage().c_str());
    }
    
    std::fprintf(stderr, "S3ChunkStore::delete_chunk: %s failed after %d retries\n",
                 object_key.c_str(), S3_MAX_RETRIES);
    return false;
}
