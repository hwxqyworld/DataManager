#ifndef ASYNC_UPLOADER_H
#define ASYNC_UPLOADER_H

#include "chunk_store.h"
#include "erasure_coder.h"
#include <string>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <memory>
#include <functional>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// 单个 chunk 上传任务
struct ChunkUploadTask {
    uint64_t stripe_id;
    int chunk_index;           // chunk 在 stripe 中的索引 (0 ~ k+m-1)
    std::string local_cache_path;  // 本地缓存文件路径
    int retry_count = 0;
    
    ChunkUploadTask() = default;
    ChunkUploadTask(uint64_t sid, int idx, std::string path)
        : stripe_id(sid), chunk_index(idx), local_cache_path(std::move(path)) {}
};

// 异步上传配置
struct AsyncUploadConfig {
    std::string cache_dir = "/tmp/cloudraidfs_cache";  // 本地缓存目录
    int worker_threads = 4;                             // 后台上传线程数
    int max_retries = 3;                                // 最大重试次数
    int retry_delay_ms = 1000;                          // 重试间隔（毫秒）
    uint64_t max_queue_size = 10000;                    // 最大队列长度（chunk 数量）
};

// 异步上传器
// - 写入时先进行 RAID 编码，将 chunks 保存到本地磁盘缓存，立即返回
// - 后台线程将 chunks 上传到对应的后端存储
// - 上传成功后删除本地缓存
// - 支持程序重启后恢复未完成的上传
class AsyncUploader {
public:
    // backends: 多个后端，每个后端存一个 chunk
    // coder: 纠删码编码器
    // k: 数据块数量
    // m: 校验块数量
    AsyncUploader(std::vector<std::shared_ptr<ChunkStore>> backends,
                  std::shared_ptr<ErasureCoder> coder,
                  int k, int m,
                  const AsyncUploadConfig& config = AsyncUploadConfig());
    
    ~AsyncUploader();
    
    // 启动后台上传线程
    void start();
    
    // 停止后台上传（等待队列完成）
    void stop();
    
    // 异步写入条带
    // 先进行 RAID 编码，将 chunks 保存到本地，然后加入上传队列
    // 返回 true 表示已成功加入队列
    bool async_write_stripe(uint64_t stripe_id, const std::string& data);
    
    // 从本地缓存读取条带（如果后端还没有同步完成）
    // 需要从本地缓存加载所有 chunks 并解码
    bool read_from_cache(uint64_t stripe_id, std::string& out);
    
    // 检查条带是否在上传队列中（任一 chunk 还未上传完成）
    bool is_pending(uint64_t stripe_id);
    
    // 等待特定条带上传完成
    void wait_for_stripe(uint64_t stripe_id);
    
    // 等待所有上传完成
    void flush();
    
    // 获取队列长度（chunk 数量）
    size_t queue_size() const;
    
    // 获取待上传条带数
    size_t pending_stripe_count() const;
    
    // 恢复上次未完成的上传（程序重启时调用）
    void recover_pending_uploads();

private:
    std::vector<std::shared_ptr<ChunkStore>> backends_;
    std::shared_ptr<ErasureCoder> coder_;
    int k_;
    int m_;
    AsyncUploadConfig config_;
    
    // 上传队列
    std::queue<ChunkUploadTask> queue_;
    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::condition_variable drain_cv_;  // 用于等待队列清空
    
    // 正在处理的 stripe（stripe_id -> 待上传的 chunk 数量）
    std::unordered_map<uint64_t, int> pending_stripes_;
    mutable std::mutex pending_mutex_;
    std::condition_variable pending_cv_;
    
    // 工作线程
    std::vector<std::thread> workers_;
    std::atomic<bool> running_{false};
    
    // 统计
    std::atomic<uint64_t> total_chunks_uploaded_{0};
    std::atomic<uint64_t> total_chunks_failed_{0};
    
    // 工作线程函数
    void worker_func();
    
    // 处理单个 chunk 上传任务
    bool process_task(ChunkUploadTask& task);
    
    // 保存 chunk 到本地缓存
    bool save_chunk_to_cache(uint64_t stripe_id, int chunk_index, 
                             const std::string& data, std::string& cache_path);
    
    // 从本地缓存加载 chunk
    bool load_chunk_from_cache(const std::string& cache_path, std::string& data);
    
    // 删除本地缓存
    void remove_cache(const std::string& cache_path);
    
    // 生成 chunk 缓存文件路径
    std::string get_chunk_cache_path(uint64_t stripe_id, int chunk_index) const;
    
    // 确保缓存目录存在
    void ensure_cache_dir();
    
    // 标记一个 chunk 上传完成，返回该 stripe 是否全部完成
    bool mark_chunk_done(uint64_t stripe_id);
};

#endif // ASYNC_UPLOADER_H
