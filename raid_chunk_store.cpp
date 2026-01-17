#include "raid_chunk_store.h"
#include <cstdio>
#include <cinttypes>
#include <atomic>
#include <thread>
#include <chrono>

// 构造函数
RAIDChunkStore::RAIDChunkStore(std::vector<std::shared_ptr<ChunkStore>> backends,
                               int k, int m,
                               std::shared_ptr<ErasureCoder> coder)
    : backends(std::move(backends)), k(k), m(m), coder(std::move(coder))
{
    if ((int)this->backends.size() != k + m) {
        fprintf(stderr, "RAIDChunkStore: backend 数量必须等于 k+m\n");
    }
    if (!this->coder) {
        fprintf(stderr, "RAIDChunkStore: coder 不能为空\n");
    }
}

// 写入条带：编码 → 并发写入多个后端（真正并行，返回时间为最慢后端耗时）
bool RAIDChunkStore::write_chunk(uint64_t stripe_id,
                                 uint32_t chunk_id,
                                 const std::string &data)
{
    (void)chunk_id; // 暂不使用，保留接口一致性

    if (!coder) return false;

    // 1. 编码成 k+m 个 chunk
    std::vector<std::string> chunks;
    if (!coder->encode(data, k, m, chunks)) {
        fprintf(stderr, "RAIDChunkStore::write_chunk: encode 失败, stripe=%" PRIu64 "\n", stripe_id);
        return false;
    }
    if ((int)chunks.size() != k + m) {
        fprintf(stderr, "RAIDChunkStore::write_chunk: chunks 数量 != k+m\n");
        return false;
    }

    // 2. 准备统计
    std::vector<BackendStats> backend_stats(k + m);
    std::vector<bool> results(k + m, false);
    
    auto overall_start = std::chrono::steady_clock::now();

    // 3. 并发写入（使用 std::thread 真正并行）
    std::vector<std::thread> threads;
    threads.reserve(k + m);

    for (int i = 0; i < k + m; i++) {
        threads.emplace_back([this, &chunks, &backend_stats, &results, stripe_id, i]() {
            auto start = std::chrono::steady_clock::now();
            bool ok = backends[i]->write_chunk(stripe_id, (uint32_t)i, chunks[i]);
            auto end = std::chrono::steady_clock::now();
            
            double elapsed = std::chrono::duration<double, std::milli>(end - start).count();
            
            backend_stats[i].backend_id = i;
            backend_stats[i].elapsed_ms = elapsed;
            backend_stats[i].success = ok;
            results[i] = ok;
        });
    }

    // 4. 等待所有线程完成
    for (auto &t : threads) {
        t.join();
    }

    auto overall_end = std::chrono::steady_clock::now();
    double total_elapsed = std::chrono::duration<double, std::milli>(overall_end - overall_start).count();

    // 5. 更新统计
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        last_write_stats_.total_elapsed_ms = total_elapsed;
        last_write_stats_.backends = std::move(backend_stats);
    }

    // 6. 打印统计信息
    fprintf(stderr, "RAIDChunkStore::write_chunk stripe=%" PRIu64 " 总耗时=%.2fms (各后端并行)\n", 
            stripe_id, total_elapsed);
    for (const auto &s : last_write_stats_.backends) {
        fprintf(stderr, "  后端[%d]: %.2fms %s\n", 
                s.backend_id, s.elapsed_ms, s.success ? "成功" : "失败");
    }

    // 7. 检查结果
    bool ok = true;
    for (int i = 0; i < k + m; i++) {
        if (!results[i]) ok = false;
    }

    if (!ok) {
        fprintf(stderr, "RAIDChunkStore::write_chunk: 部分后端写入失败\n");
    }

    return ok;
}

// 自动修复缺失 chunk：用完整 data 重新编码，然后补写缺失位置
void RAIDChunkStore::repair_missing_chunks(uint64_t stripe_id,
                                           const std::vector<bool> &present,
                                           const std::string &data)
{
    if (!coder) return;

    std::vector<std::string> chunks;
    if (!coder->encode(data, k, m, chunks)) {
        fprintf(stderr, "RAIDChunkStore::repair_missing_chunks: encode 失败\n");
        return;
    }
    if ((int)chunks.size() != k + m) return;

    std::vector<std::thread> threads;

    for (int i = 0; i < k + m; i++) {
        if (!present[i]) {
            threads.emplace_back([this, &chunks, stripe_id, i]() {
                fprintf(stderr, "RAIDChunkStore: 修复 stripe %lu 的 chunk %d\n",
                        (unsigned long)stripe_id, i);
                backends[i]->write_chunk(stripe_id, (uint32_t)i, chunks[i]);
            });
        }
    }

    for (auto &t : threads) {
        t.join();
    }
}

// 读取条带：从任意 k 个有效 chunk 恢复 + 自动修复缺失 chunk
bool RAIDChunkStore::read_chunk(uint64_t stripe_id,
                                uint32_t chunk_id,
                                std::string &out)
{
    (void)chunk_id; // 暂不使用

    if (!coder) return false;

    std::vector<std::string> chunks(k + m);
    std::vector<bool> present(k + m, false);
    std::atomic<int> ok_count{0};

    // 准备统计
    std::vector<BackendStats> backend_stats(k + m);
    
    auto overall_start = std::chrono::steady_clock::now();

    // 并发读取所有 chunk（使用 std::thread 真正并行）
    std::vector<std::thread> threads;
    threads.reserve(k + m);

    for (int i = 0; i < k + m; i++) {
        threads.emplace_back([this, &chunks, &present, &ok_count, &backend_stats, stripe_id, i]() {
            auto start = std::chrono::steady_clock::now();
            std::string buf;
            bool ok = backends[i]->read_chunk(stripe_id, (uint32_t)i, buf) && !buf.empty();
            auto end = std::chrono::steady_clock::now();
            
            double elapsed = std::chrono::duration<double, std::milli>(end - start).count();
            
            backend_stats[i].backend_id = i;
            backend_stats[i].elapsed_ms = elapsed;
            backend_stats[i].success = ok;
            
            if (ok) {
                chunks[i] = std::move(buf);
                present[i] = true;
                ok_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // 等待所有读取完成
    for (auto &t : threads) {
        t.join();
    }

    auto overall_end = std::chrono::steady_clock::now();
    double total_elapsed = std::chrono::duration<double, std::milli>(overall_end - overall_start).count();

    // 更新统计
    {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        last_read_stats_.total_elapsed_ms = total_elapsed;
        last_read_stats_.backends = std::move(backend_stats);
    }

    // 打印统计信息
    fprintf(stderr, "RAIDChunkStore::read_chunk stripe=%" PRIu64 " 总耗时=%.2fms (各后端并行)\n", 
            stripe_id, total_elapsed);
    for (const auto &s : last_read_stats_.backends) {
        fprintf(stderr, "  后端[%d]: %.2fms %s\n", 
                s.backend_id, s.elapsed_ms, s.success ? "成功" : "失败");
    }

    if (ok_count.load() < k) {
        // stripe 不存在或损坏，静默返回失败（首次启动时这是正常情况）
        return false;
    }

    // 解码：ErasureCoder 负责从部分非空 chunks 中恢复原始 data
    if (!coder->decode(chunks, k, m, out)) {
        fprintf(stderr, "RAIDChunkStore::read_chunk: decode 失败\n");
        return false;
    }

    // 自动修复缺失 chunk（后台异步执行，不阻塞返回）
    std::thread([this, stripe_id, present, out]() {
        repair_missing_chunks(stripe_id, present, out);
    }).detach();

    return true;
}

// 删除条带：并发删除所有 chunk
bool RAIDChunkStore::delete_chunk(uint64_t stripe_id,
                                  uint32_t chunk_id)
{
    (void)chunk_id; // 暂不使用

    std::vector<bool> results(k + m, false);
    std::vector<std::thread> threads;
    threads.reserve(k + m);

    for (int i = 0; i < k + m; i++) {
        threads.emplace_back([this, &results, stripe_id, i]() {
            results[i] = backends[i]->delete_chunk(stripe_id, (uint32_t)i);
        });
    }

    for (auto &t : threads) {
        t.join();
    }

    bool ok = true;
    for (int i = 0; i < k + m; i++) {
        if (!results[i]) ok = false;
    }
    return ok;
}
