#include "webdav_chunk_store.h"

#include <neon/ne_auth.h>
#include <neon/ne_uri.h>
#include <neon/ne_utils.h>
#include <cstring>
#include <cstdio>
#include <unistd.h> // for ssize_t

// 重试次数
static const int WEBDAV_MAX_RETRIES = 3;

// 把相对路径挂到 root_path 下，比如 root="/dav" 时：
//   rel="stripes"              → "/dav/stripes"
//   rel="stripes/00000000"     → "/dav/stripes/00000000"
//   rel="/already/has/slash"   → "/dav/already/has/slash"
// 如果 root 为空，则保证返回以 "/" 开头的路径：
//   rel="stripes"              → "/stripes"
//   rel="/stripes"             → "/stripes"
static std::string with_root(const std::string& root, const std::string& rel)
{
    if (root.empty()) {
        if (!rel.empty() && rel[0] == '/')
            return rel;
        return "/" + rel;
    } else {
        if (!rel.empty() && rel[0] == '/')
            return root + rel;
        return root + "/" + rel;
    }
}

// 认证回调，userdata 直接传 this
static int webdav_auth_callback(void *userdata,
                                const char *realm,
                                int attempts,
                                char *username,
                                char *password)
{
    (void)realm;
    (void)attempts;

    if (!userdata) return 1;

    auto *self = static_cast<WebDavChunkStore *>(userdata);

    std::snprintf(username, NE_ABUFSIZ, "%s", self->username.c_str());
    std::snprintf(password, NE_ABUFSIZ, "%s", self->password.c_str());

    return 0;
}

// 构造函数
WebDavChunkStore::WebDavChunkStore(const std::string &base_url_,
                                   const std::string &user,
                                   const std::string &pass)
    : base_url(base_url_),
      username(user),
      password(pass),
      session(nullptr)
{
    if (!base_url.empty() && base_url.back() != '/')
        base_url += '/';

    ne_uri uri;
    if (ne_uri_parse(base_url.c_str(), &uri) != 0) {
        std::fprintf(stderr, "WebDavChunkStore: invalid base_url: %s\n",
                     base_url.c_str());
        std::memset(&uri, 0, sizeof(uri));
        return;
    }

    const char *scheme = uri.scheme ? uri.scheme : "http";
    bool use_ssl = std::strcmp(scheme, "https") == 0;

    const char *host = uri.host ? uri.host : "localhost";
    int port = uri.port ? uri.port : (use_ssl ? 443 : 80);

    session = ne_session_create(scheme, host, port);
    if (!session) {
        std::fprintf(stderr, "WebDavChunkStore: failed to create session\n");
        ne_uri_free(&uri);
        return;
    }

    if (uri.path && uri.path[0] != '\0') {
        root_path = uri.path;
        // 去掉末尾的 '/', 但保留单独一个 "/"
        if (root_path.size() > 1 && root_path.back() == '/') {
            root_path.pop_back();
        }
    } else {
        root_path.clear();
    }
    // base_url 只保留 scheme://host:port
    char urlbuf[1024];
    std::snprintf(urlbuf, sizeof(urlbuf), "%s://%s:%d", scheme, host, port);
    base_url = urlbuf;

    if (!username.empty()) {
        ne_set_server_auth(session, webdav_auth_callback, this);
    }

    ne_uri_free(&uri);
}

// 析构函数
WebDavChunkStore::~WebDavChunkStore()
{
    std::lock_guard<std::mutex> lock(mu);
    if (session) {
        ne_session_destroy(session);
        session = nullptr;
    }
}

// 路径格式：stripes/%08lu/%02u.chunk，加上 base_url 作为前缀
std::string WebDavChunkStore::make_path(uint64_t stripe_id,
                                        uint32_t chunk_index) const
{
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "stripes/%08lu/%02u.chunk",
                  (unsigned long)stripe_id,
                  (unsigned int)chunk_index);
    return with_root(root_path, buf);
}

// MKCOL 辅助函数
static bool webdav_mkcol(ne_session *sess, const std::string &path)
{
    ne_request *req = ne_request_create(sess, "MKCOL", path.c_str());
    if (!req) return false;

    int ret  = ne_request_dispatch(req);
    int code = ne_get_status(req)->code;
    ne_request_destroy(req);

    // 201 / 200 / 405 / 409 都视为成功
    if (ret == NE_OK &&
        (code == 201 || code == 200 || code == 405 || code == 409))
        return true;

    return false;
}

// 读取 chunk
bool WebDavChunkStore::read_chunk(uint64_t stripe_id,
                                  uint32_t chunk_index,
                                  std::string &out)
{
    std::string path = make_path(stripe_id, chunk_index);

    for (int attempt = 0; attempt < WEBDAV_MAX_RETRIES; ++attempt) {
        std::lock_guard<std::mutex> lock(mu);
        if (!session) return false;

        ne_request *req = ne_request_create(session, "GET", path.c_str());
        if (!req) return false;

        out.clear();
        auto write_cb = [](void *userdata, const char *buf, size_t len) -> int {
            auto *str = static_cast<std::string *>(userdata);
            str->append(buf, len);
            return 0;
        };

        ne_add_response_body_reader(req, ne_accept_2xx, write_cb, &out);

        int ret  = ne_request_dispatch(req);
        int code = ne_get_status(req)->code;

        ne_request_destroy(req);

        if (ret == NE_OK && code >= 200 && code < 300)
            return true;

        if (code == 404) {
            out.clear();
            return false;
        }
        // 其他错误继续重试
    }

    return false;
}

// 写入 chunk
bool WebDavChunkStore::write_chunk(uint64_t stripe_id,
                                   uint32_t chunk_index,
                                   const std::string &data)
{
    // 先创建目录：<root>/stripes/ 和 <root>/stripes/<stripe_id>/
    {
        std::lock_guard<std::mutex> lock(mu);
        if (!session) return false;

        std::string stripes_dir = with_root(root_path, "stripes");
        webdav_mkcol(session, stripes_dir);

        char dirbuf[256];
        std::snprintf(dirbuf, sizeof(dirbuf),
                      "stripes/%08lu",
                      (unsigned long)stripe_id);
        std::string stripe_dir = with_root(root_path, dirbuf);
        webdav_mkcol(session, stripe_dir);
    }

    std::string path = make_path(stripe_id, chunk_index);

    for (int attempt = 0; attempt < WEBDAV_MAX_RETRIES; ++attempt) {
        std::lock_guard<std::mutex> lock(mu);
        if (!session) return false;

        ne_request *req = ne_request_create(session, "PUT", path.c_str());
        if (!req) return false;

        struct BodyCtx {
            const char *ptr;
            size_t left;
        } ctx{data.data(), data.size()};

        auto reader_cb = [](void *userdata,
                            char *buf,
                            size_t buflen) -> ssize_t {
            auto *c = static_cast<BodyCtx *>(userdata);
            size_t n = (c->left < buflen) ? c->left : buflen;
            if (n == 0) return 0;
            std::memcpy(buf, c->ptr, n);
            c->ptr += n;
            c->left -= n;
            return (ssize_t)n;
        };

        ne_add_request_header(req, "Content-Type", "application/octet-stream");
        ne_set_request_body_provider(req,
                                     (ne_off_t)data.size(),
                                     reader_cb,
                                     &ctx);

        int ret  = ne_request_dispatch(req);
        int code = ne_get_status(req)->code;

        ne_request_destroy(req);

        if (ret == NE_OK && code >= 200 && code < 300)
            return true;
        // 其他错误重试
    }

    return false;
}

// 删除 chunk（不管目录）
bool WebDavChunkStore::delete_chunk(uint64_t stripe_id,
                                    uint32_t chunk_id)
{
    std::string path = make_path(stripe_id, chunk_id);

    for (int attempt = 0; attempt < WEBDAV_MAX_RETRIES; ++attempt) {
        std::lock_guard<std::mutex> lock(mu);
        if (!session) return false;

        ne_request *req = ne_request_create(session, "DELETE", path.c_str());
        if (!req) return false;

        int ret  = ne_request_dispatch(req);
        int code = ne_get_status(req)->code;

        ne_request_destroy(req);

        // 200 / 204 正常删除，404 当作已经不存在也算成功
        if (ret == NE_OK && (code == 200 || code == 204 || code == 404))
            return true;
    }

    return false;
}
