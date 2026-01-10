#ifndef RAID_CHUNK_STORE_H
#define RAID_CHUNK_STORE_H

#include "chunk_store.h"
#include "erasure_coder.h"
#include <vector>
#include <memory>
#include <string>
#include <cstdint>

// RAID (k+m) 纠删码层
// 将一个条带编码成 k+m 个 chunk
// 分发到多个后端 ChunkStore
//
// 读取时从任意 k 个有效 chunk 恢复原始数据
// 并自动修复缺失的 chunk

class RAIDChunkStore : public ChunkStore {
public:
    // backends: 多个后端，每个后端存一个 chunk
    // k: 数据块数量
    // m: 校验块数量
    // coder: 通用 (k+m) 纠删码实现（如 RSCoder）
    RAIDChunkStore(std::vector<std::shared_ptr<ChunkStore>> backends,
                   int k, int m,
                   std::shared_ptr<ErasureCoder> coder);

    // 这里忽略 chunk_id，因为对上层暴露的是“整条带”
    bool read_chunk(uint64_t stripe_id,
                    uint32_t chunk_id,
                    std::string &out) override;

    bool write_chunk(uint64_t stripe_id,
                     uint32_t chunk_id,
                     const std::string &data) override;

    bool delete_chunk(uint64_t stripe_id,
                      uint32_t chunk_id) override;

private:
    std::vector<std::shared_ptr<ChunkStore>> backends;
    int k;
    int m;
    std::shared_ptr<ErasureCoder> coder;

    // 自动修复：根据完整 data 重新编码，并把缺失 chunk 补写回去
    void repair_missing_chunks(uint64_t stripe_id,
                               const std::vector<bool> &present,
                               const std::string &data);
};

#endif // RAID_CHUNK_STORE_H
