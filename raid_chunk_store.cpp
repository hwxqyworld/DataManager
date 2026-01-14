#include "raid_chunk_store.h"
#include <cstdio>
#include <future>
#include <cinttypes>
#include <atomic>
#include <thread>

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

// 写入条带：编码 → 并发写入多个后端
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

    // 2. 并发写入
    std::vector<std::future<bool>> tasks;
    tasks.reserve(k + m);

    for (int i = 0; i < k + m; i++) {
        tasks.push_back(std::async(std::launch::async, [&, i]() {
            return backends[i]->write_chunk(stripe_id, (uint32_t)i, chunks[i]);
        }));
    }

    // 3. 等待结果
    bool ok = true;
    for (auto &t : tasks) {
        if (!t.get()) ok = false;
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

    std::vector<std::future<bool>> tasks;

    for (int i = 0; i < k + m; i++) {
        if (!present[i]) {
            tasks.push_back(std::async(std::launch::async, [&, i]() {
                fprintf(stderr, "RAIDChunkStore: 修复 stripe %lu 的 chunk %d\n",
                        (unsigned long)stripe_id, i);
                return backends[i]->write_chunk(stripe_id, (uint32_t)i, chunks[i]);
            }));
        }
    }

    for (auto &t : tasks) {
        (void)t.get(); // 这里修复失败也不影响 read 返回的数据
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

    // 并发读取所有 chunk
    std::vector<std::future<void>> tasks;
    tasks.reserve(k + m);

    for (int i = 0; i < k + m; i++) {
        tasks.push_back(std::async(std::launch::async, [this, &chunks, &present, &ok_count, stripe_id, i]() {
            std::string buf;
            if (backends[i]->read_chunk(stripe_id, (uint32_t)i, buf) && !buf.empty()) {
                chunks[i] = std::move(buf);
                present[i] = true;
                ok_count.fetch_add(1, std::memory_order_relaxed);
            }
        }));
    }

    // 等待所有读取完成
    for (auto &t : tasks) {
        t.get();
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

    std::vector<std::future<bool>> tasks;
    tasks.reserve(k + m);

    for (int i = 0; i < k + m; i++) {
        tasks.push_back(std::async(std::launch::async, [this, stripe_id, i]() {
            return backends[i]->delete_chunk(stripe_id, (uint32_t)i);
        }));
    }

    bool ok = true;
    for (auto &t : tasks) {
        if (!t.get()) ok = false;
    }
    return ok;
}
