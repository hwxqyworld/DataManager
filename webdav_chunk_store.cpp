#include "webdav_chunk_store.h"

#include <cstring>
#include <cstdio>

// 重试次数
static const int WEBDAV_MAX_RETRIES = 3;

// CURL 写回调 - 将响应数据写入 std::string
static size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata)
{
    size_t total = size * nmemb;
    auto* str = static_cast<std::string*>(userdata);
    str->append(ptr, total);
    return total;
}

// CURL 读回调 - 从 std::string 读取数据用于 PUT
struct ReadContext {
    const char* ptr;
    size_t left;
};

static size_t read_callback(char* buffer, size_t size, size_t nmemb, void* userdata)
{
    auto* ctx = static_cast<ReadContext*>(userdata);
    size_t max_copy = size * nmemb;
    size_t to_copy = (ctx->left < max_copy) ? ctx->left : max_copy;
    
    if (to_copy == 0) return 0;
    
    std::memcpy(buffer, ctx->ptr, to_copy);
    ctx->ptr += to_copy;
    ctx->left -= to_copy;
    
    return to_copy;
}

// 构造函数
WebDavChunkStore::WebDavChunkStore(const std::string& base_url_,
                                   const std::string& user,
                                   const std::string& pass)
    : base_url(base_url_),
      username(user),
      password(pass)
{
    // 确保 base_url 以 / 结尾
    if (!base_url.empty() && base_url.back() != '/') {
        base_url += '/';
    }
    
    // 全局初始化 curl（只需一次，但多次调用也安全）
    curl_global_init(CURL_GLOBAL_DEFAULT);
    
    std::fprintf(stderr, "WebDavChunkStore: initialized with URL=%s, user=%s\n",
                 base_url.c_str(), username.c_str());
}

// 析构函数
WebDavChunkStore::~WebDavChunkStore()
{
    // 注意：curl_global_cleanup() 应该在程序退出时调用一次
    // 这里不调用，因为可能有多个实例
}

// 设置 CURL 认证选项
void WebDavChunkStore::setup_curl_auth(CURL* curl)
{
    if (!username.empty()) {
        // 设置用户名和密码
        curl_easy_setopt(curl, CURLOPT_USERNAME, username.c_str());
        curl_easy_setopt(curl, CURLOPT_PASSWORD, password.c_str());
        
        // 使用 Basic 和 Digest 认证（自动选择）
        curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BASIC | CURLAUTH_DIGEST);
    }
    
    // 通用设置
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 60L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
    
    // 禁用 SSL 验证（生产环境应该启用）
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);
}

// 构建完整 URL
std::string WebDavChunkStore::make_url(uint64_t stripe_id, uint32_t chunk_index) const
{
    char buf[512];
    std::snprintf(buf, sizeof(buf), "%sstripes/%08lu/%02u.chunk",
                  base_url.c_str(),
                  (unsigned long)stripe_id,
                  (unsigned int)chunk_index);
    return std::string(buf);
}

// 构建目录 URL
std::string WebDavChunkStore::make_dir_url(const std::string& rel_path) const
{
    std::string url = base_url + rel_path;
    if (!url.empty() && url.back() != '/') {
        url += '/';
    }
    return url;
}

// WebDAV MKCOL 创建目录
bool WebDavChunkStore::mkcol(const std::string& url)
{
    CURL* curl = curl_easy_init();
    if (!curl) return false;
    
    setup_curl_auth(curl);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "MKCOL");
    
    // 忽略响应体
    curl_easy_setopt(curl, CURLOPT_NOBODY, 0L);
    std::string response;
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    
    CURLcode res = curl_easy_perform(curl);
    long http_code = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
    
    curl_easy_cleanup(curl);
    
    // 201 Created, 200 OK, 405 Method Not Allowed (已存在), 409 Conflict (父目录问题但可能已存在)
    if (res == CURLE_OK && (http_code == 201 || http_code == 200 || 
                            http_code == 405 || http_code == 409)) {
        return true;
    }
    
    std::fprintf(stderr, "WebDavChunkStore::mkcol: %s failed, HTTP %ld, curl %d\n",
                 url.c_str(), http_code, (int)res);
    return false;
}

// 读取 chunk
bool WebDavChunkStore::read_chunk(uint64_t stripe_id,
                                  uint32_t chunk_index,
                                  std::string& out)
{
    std::string url = make_url(stripe_id, chunk_index);
    
    for (int attempt = 0; attempt < WEBDAV_MAX_RETRIES; ++attempt) {
        std::lock_guard<std::mutex> lock(mu);
        
        CURL* curl = curl_easy_init();
        if (!curl) return false;
        
        out.clear();
        
        setup_curl_auth(curl);
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPGET, 1L);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);
        
        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        
        curl_easy_cleanup(curl);
        
        if (res == CURLE_OK && http_code >= 200 && http_code < 300) {
            return true;
        }
        
        if (http_code == 404) {
            out.clear();
            return false;
        }
        
        std::fprintf(stderr, "WebDavChunkStore::read_chunk: %s attempt %d failed, HTTP %ld\n",
                     url.c_str(), attempt + 1, http_code);
    }
    
    std::fprintf(stderr, "WebDavChunkStore::read_chunk: %s failed after %d retries\n",
                 url.c_str(), WEBDAV_MAX_RETRIES);
    return false;
}

// 写入 chunk
bool WebDavChunkStore::write_chunk(uint64_t stripe_id,
                                   uint32_t chunk_index,
                                   const std::string& data)
{
    // 先创建目录
    {
        std::lock_guard<std::mutex> lock(mu);
        
        std::string stripes_url = make_dir_url("stripes");
        mkcol(stripes_url);
        
        char dirbuf[256];
        std::snprintf(dirbuf, sizeof(dirbuf), "stripes/%08lu", (unsigned long)stripe_id);
        std::string stripe_url = make_dir_url(dirbuf);
        mkcol(stripe_url);
    }
    
    std::string url = make_url(stripe_id, chunk_index);
    
    for (int attempt = 0; attempt < WEBDAV_MAX_RETRIES; ++attempt) {
        std::lock_guard<std::mutex> lock(mu);
        
        CURL* curl = curl_easy_init();
        if (!curl) return false;
        
        ReadContext ctx{data.data(), data.size()};
        
        setup_curl_auth(curl);
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_UPLOAD, 1L);
        curl_easy_setopt(curl, CURLOPT_READFUNCTION, read_callback);
        curl_easy_setopt(curl, CURLOPT_READDATA, &ctx);
        curl_easy_setopt(curl, CURLOPT_INFILESIZE_LARGE, (curl_off_t)data.size());
        
        // 设置 Content-Type
        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/octet-stream");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        
        // 忽略响应体
        std::string response;
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        
        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        
        curl_slist_free_all(headers);
        curl_easy_cleanup(curl);
        
        if (res == CURLE_OK && http_code >= 200 && http_code < 300) {
            return true;
        }
        
        std::fprintf(stderr, "WebDavChunkStore::write_chunk: %s attempt %d failed, HTTP %ld, curl %d\n",
                     url.c_str(), attempt + 1, http_code, (int)res);
    }
    
    std::fprintf(stderr, "WebDavChunkStore::write_chunk: %s failed after %d retries\n",
                 url.c_str(), WEBDAV_MAX_RETRIES);
    return false;
}

// 删除 chunk
bool WebDavChunkStore::delete_chunk(uint64_t stripe_id, uint32_t chunk_id)
{
    std::string url = make_url(stripe_id, chunk_id);
    
    for (int attempt = 0; attempt < WEBDAV_MAX_RETRIES; ++attempt) {
        std::lock_guard<std::mutex> lock(mu);
        
        CURL* curl = curl_easy_init();
        if (!curl) return false;
        
        setup_curl_auth(curl);
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
        
        // 忽略响应体
        std::string response;
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
        
        CURLcode res = curl_easy_perform(curl);
        long http_code = 0;
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);
        
        curl_easy_cleanup(curl);
        
        // 200 OK, 204 No Content, 404 Not Found (已不存在)
        if (res == CURLE_OK && (http_code == 200 || http_code == 204 || http_code == 404)) {
            return true;
        }
        
        std::fprintf(stderr, "WebDavChunkStore::delete_chunk: %s attempt %d failed, HTTP %ld\n",
                     url.c_str(), attempt + 1, http_code);
    }
    
    return false;
}
