#include "webdav_chunk_store.h"

#include <cstring>
#include <cstdio>
#include <cstdlib>

// 重试次数
static const int WEBDAV_MAX_RETRIES = 3;

// 认证回调的用户数据结构
struct AuthData {
    std::string username;
    std::string password;
};

// ==================== NeonPool 实现 ====================

NeonPool::NeonPool(const std::string& scheme,
                   const std::string& host,
                   int port,
                   const std::string& username,
                   const std::string& password,
                   size_t max_size)
    : scheme_(scheme),
      host_(host),
      port_(port),
      username_(username),
      password_(password),
      max_size_(max_size)
{
    // 初始化 neon 库（只需一次，多次调用也安全）
    ne_sock_init();
}

NeonPool::~NeonPool()
{
    std::lock_guard<std::mutex> lock(mu_);
    while (!pool_.empty()) {
        ne_session_destroy(pool_.front());
        pool_.pop();
    }
    // 注意：ne_sock_exit() 应该在程序退出时调用一次
}

int NeonPool::auth_callback(void* userdata, const char* realm, int attempt,
                            char* username, char* password)
{
    (void)realm;
    
    // 只尝试一次认证
    if (attempt > 0) {
        return -1;
    }
    
    auto* auth = static_cast<AuthData*>(userdata);
    std::strncpy(username, auth->username.c_str(), NE_ABUFSIZ - 1);
    username[NE_ABUFSIZ - 1] = '\0';
    std::strncpy(password, auth->password.c_str(), NE_ABUFSIZ - 1);
    password[NE_ABUFSIZ - 1] = '\0';
    
    return 0;
}

ne_session* NeonPool::create_session()
{
    ne_session* sess = ne_session_create(scheme_.c_str(), host_.c_str(), port_);
    if (!sess) {
        return nullptr;
    }
    
    // 设置超时
    ne_set_connect_timeout(sess, 10);
    ne_set_read_timeout(sess, 30);
    
    // 设置认证回调
    if (!username_.empty()) {
        // 创建认证数据（存储在会话的私有数据中）
        auto* auth = new AuthData{username_, password_};
        ne_set_server_auth(sess, auth_callback, auth);
        // 注意：auth 的生命周期需要与 session 一致
        // 这里简化处理，实际应该在 session 销毁时清理
        ne_hook_destroy_session(sess, [](void* userdata) {
            delete static_cast<AuthData*>(userdata);
        }, auth);
    }
    
    // 禁用 SSL 验证（生产环境应该启用）
    ne_ssl_set_verify(sess, [](void*, int, const ne_ssl_certificate*) -> int {
        return 0;  // 接受所有证书
    }, nullptr);
    
    return sess;
}

ne_session* NeonPool::acquire()
{
    std::lock_guard<std::mutex> lock(mu_);
    if (!pool_.empty()) {
        ne_session* sess = pool_.front();
        pool_.pop();
        return sess;
    }
    return create_session();
}

void NeonPool::release(ne_session* sess)
{
    if (!sess) return;
    std::lock_guard<std::mutex> lock(mu_);
    if (pool_.size() < max_size_) {
        pool_.push(sess);
    } else {
        ne_session_destroy(sess);
    }
}

// ==================== WebDavChunkStore 实现 ====================

// 构造函数
WebDavChunkStore::WebDavChunkStore(const std::string& base_url,
                                   const std::string& user,
                                   const std::string& pass)
    : base_url_(base_url),
      username_(user),
      password_(pass)
{
    // 解析 URL
    ne_uri uri;
    if (ne_uri_parse(base_url.c_str(), &uri) != 0) {
        std::fprintf(stderr, "WebDavChunkStore: failed to parse URL: %s\n", base_url.c_str());
        return;
    }
    
    // 提取各部分
    std::string scheme = uri.scheme ? uri.scheme : "http";
    std::string host = uri.host ? uri.host : "localhost";
    int port = uri.port ? uri.port : (scheme == "https" ? 443 : 80);
    base_path_ = uri.path ? uri.path : "/";
    
    // 确保 base_path_ 以 / 结尾
    if (!base_path_.empty() && base_path_.back() != '/') {
        base_path_ += '/';
    }
    
    ne_uri_free(&uri);
    
    // 创建连接池
    neon_pool_ = std::make_unique<NeonPool>(scheme, host, port, username_, password_, 16);
    
    std::fprintf(stderr, "WebDavChunkStore: initialized with URL=%s, user=%s\n",
                 base_url.c_str(), username_.c_str());
}

// 析构函数
WebDavChunkStore::~WebDavChunkStore()
{
    // neon_pool_ 会自动清理
}

// 构建完整路径
std::string WebDavChunkStore::make_path(uint64_t stripe_id, uint32_t chunk_index) const
{
    char buf[512];
    std::snprintf(buf, sizeof(buf), "%sstripes/%08lu/%02u.chunk",
                  base_path_.c_str(),
                  (unsigned long)stripe_id,
                  (unsigned int)chunk_index);
    return std::string(buf);
}

// 构建目录路径
std::string WebDavChunkStore::make_dir_path(const std::string& rel_path) const
{
    return base_path_ + rel_path;
}

// WebDAV MKCOL 创建目录
bool WebDavChunkStore::mkcol(ne_session* sess, const std::string& path)
{
    // 使用 ne_request_create 发送 MKCOL 请求
    ne_request* req = ne_request_create(sess, "MKCOL", path.c_str());
    int ret = ne_request_dispatch(req);

    // NE_OK 成功，或者目录已存在（405 Method Not Allowed）
    if (ret == NE_OK) {
        ne_request_destroy(req);
        return true;
    }

    // 检查 HTTP 状态码
    const ne_status* status = ne_get_status(req);
    if (status && (status->code == 405 || status->code == 409 || status->code == 301)) {
        // 405: 目录已存在
        // 409: 父目录问题但可能已存在
        // 301: 重定向（通常表示目录已存在）
        ne_request_destroy(req);
        return true;
    }

    std::fprintf(stderr, "WebDavChunkStore::mkcol: %s failed, error: %s\n",
                 path.c_str(), ne_get_error(sess));
    ne_request_destroy(req);
    return false;
}

// 确保 stripe 目录存在（带缓存）
void WebDavChunkStore::ensure_stripe_dir(uint64_t stripe_id)
{
    // 快速检查是否已创建
    {
        std::lock_guard<std::mutex> lock(dir_cache_mu_);
        if (created_stripe_dirs_.count(stripe_id) > 0) {
            return;  // 已创建，直接返回
        }
    }
    
    // 需要创建目录
    NeonHandle sess(*neon_pool_);
    
    // 确保 stripes 根目录存在
    {
        std::lock_guard<std::mutex> lock(dir_cache_mu_);
        if (!stripes_dir_created_) {
            std::string stripes_path = make_dir_path("stripes");
            mkcol(sess, stripes_path);
            stripes_dir_created_ = true;
        }
    }
    
    // 创建 stripe 目录
    char dirbuf[256];
    std::snprintf(dirbuf, sizeof(dirbuf), "stripes/%08lu", (unsigned long)stripe_id);
    std::string stripe_path = make_dir_path(dirbuf);
    
    mkcol(sess, stripe_path);
    
    // 记录已创建
    {
        std::lock_guard<std::mutex> lock(dir_cache_mu_);
        created_stripe_dirs_.insert(stripe_id);
    }
}

// 读取 chunk - 响应体接收器
struct ReadBodyContext {
    std::string* out;
};

static int read_body_reader(void* userdata, const char* buf, size_t len)
{
    auto* ctx = static_cast<ReadBodyContext*>(userdata);
    ctx->out->append(buf, len);
    return 0;
}

bool WebDavChunkStore::read_chunk(uint64_t stripe_id,
                                  uint32_t chunk_index,
                                  std::string& out)
{
    std::string path = make_path(stripe_id, chunk_index);
    
    for (int attempt = 0; attempt < WEBDAV_MAX_RETRIES; ++attempt) {
        NeonHandle sess(*neon_pool_);
        
        out.clear();
        
        // 创建 GET 请求
        ne_request* req = ne_request_create(sess, "GET", path.c_str());
        
        // 设置响应体处理
        ReadBodyContext ctx{&out};
        ne_add_response_body_reader(req, ne_accept_2xx, read_body_reader, &ctx);
        
        int ret = ne_request_dispatch(req);
        const ne_status* status = ne_get_status(req);
        int http_code = status ? status->code : 0;
        
        ne_request_destroy(req);
        
        if (ret == NE_OK && http_code >= 200 && http_code < 300) {
            return true;
        }
        
        if (http_code == 404) {
            out.clear();
            return false;
        }
        
        std::fprintf(stderr, "WebDavChunkStore::read_chunk: %s attempt %d failed, HTTP %d, error: %s\n",
                     path.c_str(), attempt + 1, http_code, ne_get_error(sess));
    }
    
    std::fprintf(stderr, "WebDavChunkStore::read_chunk: %s failed after %d retries\n",
                 path.c_str(), WEBDAV_MAX_RETRIES);
    return false;
}

// 写入 chunk - 请求体提供器
struct WriteBodyContext {
    const char* ptr;
    size_t left;
};

static ssize_t write_body_provider(void* userdata, char* buffer, size_t buflen)
{
    auto* ctx = static_cast<WriteBodyContext*>(userdata);
    
    if (ctx->left == 0) {
        return 0;  // 数据已全部发送
    }
    
    size_t to_copy = (ctx->left < buflen) ? ctx->left : buflen;
    std::memcpy(buffer, ctx->ptr, to_copy);
    ctx->ptr += to_copy;
    ctx->left -= to_copy;
    
    return static_cast<ssize_t>(to_copy);
}

bool WebDavChunkStore::write_chunk(uint64_t stripe_id,
                                   uint32_t chunk_index,
                                   const std::string& data)
{
    // 确保目录存在（带缓存）
    ensure_stripe_dir(stripe_id);
    
    std::string path = make_path(stripe_id, chunk_index);
    
    for (int attempt = 0; attempt < WEBDAV_MAX_RETRIES; ++attempt) {
        NeonHandle sess(*neon_pool_);
        
        // 创建 PUT 请求
        ne_request* req = ne_request_create(sess, "PUT", path.c_str());
        
        // 设置请求体
        WriteBodyContext ctx{data.data(), data.size()};
        ne_set_request_body_provider(req, data.size(), write_body_provider, &ctx);
        
        // 设置 Content-Type
        ne_add_request_header(req, "Content-Type", "application/octet-stream");
        
        int ret = ne_request_dispatch(req);
        const ne_status* status = ne_get_status(req);
        int http_code = status ? status->code : 0;
        
        ne_request_destroy(req);
        
        if (ret == NE_OK && http_code >= 200 && http_code < 300) {
            return true;
        }
        
        std::fprintf(stderr, "WebDavChunkStore::write_chunk: %s attempt %d failed, HTTP %d, error: %s\n",
                     path.c_str(), attempt + 1, http_code, ne_get_error(sess));
    }
    
    std::fprintf(stderr, "WebDavChunkStore::write_chunk: %s failed after %d retries\n",
                 path.c_str(), WEBDAV_MAX_RETRIES);
    return false;
}

// 删除 chunk
bool WebDavChunkStore::delete_chunk(uint64_t stripe_id, uint32_t chunk_id)
{
    std::string path = make_path(stripe_id, chunk_id);

    for (int attempt = 0; attempt < WEBDAV_MAX_RETRIES; ++attempt) {
        NeonHandle sess(*neon_pool_);

        // 创建 DELETE 请求
        ne_request* req = ne_request_create(sess, "DELETE", path.c_str());

        int ret = ne_request_dispatch(req);
        const ne_status* status = ne_get_status(req);
        int http_code = status ? status->code : 0;

        ne_request_destroy(req);

        // 200 OK, 204 No Content, 404 Not Found (已不存在)
        if (ret == NE_OK || http_code == 200 || http_code == 204 || http_code == 404) {
            return true;
        }

        std::fprintf(stderr, "WebDavChunkStore::delete_chunk: %s attempt %d failed, HTTP %d, error: %s\n",
                     path.c_str(), attempt + 1, http_code, ne_get_error(sess));
    }

    return false;
}
