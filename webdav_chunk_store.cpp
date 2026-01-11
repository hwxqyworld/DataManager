#include "webdav_chunk_store.h"

#include <neon/ne_auth.h>
#include <neon/ne_uri.h>
#include <neon/ne_utils.h>
#include <cstring>
#include <cstdio>

// 重试次数
static const int WEBDAV_MAX_RETRIES = 3;

// 认证回调
static int webdav_auth_callback(void *userdata,
                                const char *realm,
                                int attempts,
                                char *username,
                                char *password)
{
    if (!userdata) return 1;

    auto *creds = static_cast<std::pair<std::string, std::string> *>(userdata);

    std::snprintf(username, NE_ABUFSIZ, "%s", creds->first.c_str());
    std::snprintf(password, NE_ABUFSIZ, "%s", creds->second.c_str());

    return 0;
}

// 构造函数
WebDavChunkStore::WebDavChunkStore(const std::string &base_url_,
                                   const std::string &username,
                                   const std::string &password)
    : base_url(base_url_),
      session(nullptr)
{
    if (!base_url.empty() && base_url.back() != '/')
        base_url += '/';

    ne_uri uri;
    if (ne_uri_parse(base_url.c_str(), &uri) != 0) {
        fprintf(stderr, "WebDavChunkStore: invalid base_url: %s\n", base_url.c_str());
        memset(&uri, 0, sizeof(uri));
        return;
    }

    const char *scheme = uri.scheme ? uri.scheme : "http";
    bool use_ssl = strcmp(scheme, "https") == 0;

    const char *host = uri.host ? uri.host : "localhost";
    int port = uri.port ? uri.port : (use_ssl ? 443 : 80);

    session = ne_session_create(scheme, host, port);
    if (!session) {
        fprintf(stderr, "WebDavChunkStore: failed to create session\n");
        ne_uri_free(&uri);
        return;
    }

    if (uri.path && strlen(uri.path) > 0)
        ne_set_server_root(session, uri.path);

    if (!username.empty()) {
        auto *creds = new std::pair<std::string, std::string>(username, password);
        ne_set_server_auth(session, webdav_auth_callback, creds);

        ne_set_session_private(session, creds, [](void *data) {
            delete static_cast<std::pair<std::string, std::string> *>(data);
        });
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

// 路径格式：stripes/%08lu/%02u.chunk
std::string WebDavChunkStore::make_path(uint64_t stripe_id,
                                        int chunk_index) const
{
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "stripes/%08lu/%02u.chunk",
                  (unsigned long)stripe_id,
                  (unsigned int)chunk_index);
    return std::string(buf);
}

// MKCOL 辅助函数
static bool webdav_mkcol(ne_session *sess, const std::string &path)
{
    ne_request *req = ne_request_create(sess, "MKCOL", path.c_str());
    if (!req) return false;

    int ret = ne_request_dispatch(req);
    int code = ne_get_status(req)->code;
    ne_request_destroy(req);

    // 201 Created / 405 Already exists / 409 Conflict 都视为成功
    if (ret == NE_OK &&
        (code == 201 || code == 200 || code == 405 || code == 409))
        return true;

    return false;
}

// 读取 chunk
bool WebDavChunkStore::read_chunk(uint64_t stripe_id,
                                  int chunk_index,
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

        ne_set_request_body_reader(req, ne_accept_2xx, write_cb, &out);

        int ret = ne_request_dispatch(req);
        int code = ne_get_status(req)->code;

        ne_request_destroy(req);

        if (ret == NE_OK && code >= 200 && code < 300)
            return true;

        if (code == 404) {
            out.clear();
            return false;
        }
    }

    return false;
}

// 写入 chunk
bool WebDavChunkStore::write_chunk(uint64_t stripe_id,
                                   int chunk_index,
                                   const std::string &data)
{
    // 先创建目录：stripes/ 和 stripes/<stripe_id>/
    {
        std::lock_guard<std::mutex> lock(mu);
        if (!session) return false;

        webdav_mkcol(session, "stripes");

        char dirbuf[256];
        std::snprintf(dirbuf, sizeof(dirbuf),
                      "stripes/%08lu",
                      (unsigned long)stripe_id);
        webdav_mkcol(session, dirbuf);
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

        auto reader_cb = [](void *userdata, char *buf, size_t buflen) -> ssize_t {
            auto *c = static_cast<BodyCtx *>(userdata);
            size_t n = (c->left < buflen) ? c->left : buflen;
            if (n == 0) return 0;
            memcpy(buf, c->ptr, n);
            c->ptr += n;
            c->left -= n;
            return (ssize_t)n;
        };

        ne_set_request_body_provider(req,
                                     "application/octet-stream",
                                     data.size(),
                                     reader_cb,
                                     &ctx);

        int ret = ne_request_dispatch(req);
        int code = ne_get_status(req)->code;

        ne_request_destroy(req);

        if (ret == NE_OK && code >= 200 && code < 300)
            return true;
    }

    return false;
}
