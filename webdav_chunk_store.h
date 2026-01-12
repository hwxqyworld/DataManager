#ifndef WEBDAV_CHUNK_STORE_H
#define WEBDAV_CHUNK_STORE_H

#include "chunk_store.h"
#include <neon/ne_request.h>
#include <neon/ne_session.h>
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
    ne_session* session;
    std::mutex mu;

    std::string make_path(uint64_t stripe_id, int chunk_index) const;
};

#endif
