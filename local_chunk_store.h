#ifndef LOCAL_CHUNK_STORE_H
#define LOCAL_CHUNK_STORE_H

#include "chunk_store.h"
#include <string>
#include <cstdint>

// 本地目录实现的 ChunkStore
// 每个 chunk 对应一个文件：
//   <root>/stripes/<stripe_id>/<chunk_id>.chunk

class LocalChunkStore : public ChunkStore {
public:
    explicit LocalChunkStore(const std::string &root_dir);

    bool read_chunk(uint64_t stripe_id,
                    uint32_t chunk_id,
                    std::string &out) override;

    bool write_chunk(uint64_t stripe_id,
                     uint32_t chunk_id,
                     const std::string &data) override;

    bool delete_chunk(uint64_t stripe_id,
                      uint32_t chunk_id) override;

private:
    std::string root;

    // 生成 chunk 文件路径
    std::string make_path(uint64_t stripe_id,
                          uint32_t chunk_id) const;

    // 确保目录存在
    bool ensure_dir(uint64_t stripe_id) const;
};

#endif // LOCAL_CHUNK_STORE_H
