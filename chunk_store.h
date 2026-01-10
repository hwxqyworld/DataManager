#ifndef CHUNK_STORE_H
#define CHUNK_STORE_H

#include <string>
#include <cstdint>

// 统一的 4MB 块访问接口
// 给出 stripe_id + chunk_id 就能读写一个完整的数据块
// 屏蔽底层实现（本地盘 / S3 / WebDAV / SMB / 多云纠删码）

class ChunkStore {
public:
    // 读取一个 chunk（通常为 4MB）
    // 返回 true 表示成功，out 填充数据
    virtual bool read_chunk(uint64_t stripe_id,
                            uint32_t chunk_id,
                            std::string &out) = 0;

    // 写入一个 chunk（通常为 4MB）
    virtual bool write_chunk(uint64_t stripe_id,
                             uint32_t chunk_id,
                             const std::string &data) = 0;

    // 删除一个 chunk
    virtual bool delete_chunk(uint64_t stripe_id,
                              uint32_t chunk_id) = 0;

    virtual ~ChunkStore() = default;
};

#endif // CHUNK_STORE_H
