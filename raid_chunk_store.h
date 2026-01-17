#ifndef RAID_CHUNK_STORE_H
#define RAID_CHUNK_STORE_H

#include "chunk_store.h"
#include "erasure_coder.h"
#include <vector>
#include <memory>
#include <string>
#include <cstdint>
#include <chrono>
#include <mutex>

// 后端操作统计信息
struct BackendStats {
    int backend_id;
    double elapsed_ms;   // 该后端耗时(毫秒)
    bool success;
};

// 一次批量操作的统计结果
struct OperationStats {
    double total_elapsed_ms;           // 总耗时（最慢后端）
    std::vector<BackendStats> backends; // 每个后端的统计
};

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

    // 这里忽略 chunk_id，因为对上层暴露的是"整条带"
    bool read_chunk(uint64_t stripe_id,
                    uint32_t chunk_id,
                    std::string &out) override;

    bool write_chunk(uint64_t stripe_id,
                     uint32_t chunk_id,
                     const std::string &data) override;

    bool delete_chunk(uint64_t stripe_id,
                      uint32_t chunk_id) override;
    
    uint64_t allocate_new_stripe() { return next_stripe_id++; }
    
    // 设置下一个 stripe ID（用于加载元数据后更新）
    void set_next_stripe_id(uint64_t id) { 
        if (id > next_stripe_id) next_stripe_id = id; 
    }

    // 获取最近一次操作的统计信息
    OperationStats get_last_read_stats() const { return last_read_stats_; }
    OperationStats get_last_write_stats() const { return last_write_stats_; }

private:
    std::vector<std::shared_ptr<ChunkStore>> backends;
    int k;
    int m;
    std::shared_ptr<ErasureCoder> coder;
    uint64_t next_stripe_id = 100;  // 保留 0-99 给元数据文件

    // 最近一次操作统计
    mutable std::mutex stats_mutex_;
    OperationStats last_read_stats_;
    OperationStats last_write_stats_;

    // 自动修复：根据完整 data 重新编码，并把缺失 chunk 补写回去
    void repair_missing_chunks(uint64_t stripe_id,
                               const std::vector<bool> &present,
                               const std::string &data);
};

#endif // RAID_CHUNK_STORE_H
