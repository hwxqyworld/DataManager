#ifndef ASYNC_UPLOADER_H
#define ASYNC_UPLOADER_H

#include "raid_chunk_store.h"
#include <string>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <atomic>
#include <memory>
#include <functional>
#include <cstdint>

// 上传任务
struct UploadTask {
    uint64_t stripe_id;
    std::string data;
    std::string local_cache_path;  // 本地缓存文件路径
    int retry_count = 0;
    
    UploadTask() = default;
    UploadTask(uint64_t id, std::string d, std::string path)
        : stripe_id(id), data(std::move(d)), local_cache_path(std::move(path)) {}
};

// 异步上传配置
struct AsyncUploadConfig {
    std::string cache_dir = "/tmp/cloudraidfs_cache";  // 本地缓存目录
    int worker_threads = 2;                             // 后台上传线程数
    int max_retries = 3;                                // 最大重试次数
    int retry_delay_ms = 1000;                          // 重试间隔（毫秒）
    uint64_t max_queue_size = 1000;                     // 最大队列长度
};

// 异步上传器
// - 写入时先保存到本地磁盘缓存，立即返回
// - 后台线程将数据上传到后端存储
// - 上传成功后删除本地缓存
// - 支持程序重启后恢复未完成的上传
class AsyncUploader {
public:
    AsyncUploader(std::shared_ptr<RAIDChunkStore> raid_store,
                  const AsyncUploadConfig& config = AsyncUploadConfig());
    
    ~AsyncUploader();
    
    // 启动后台上传线程
    void start();
    
    // 停止后台上传（等待队列完成）
    void stop();
    
    // 异步写入条带
    // 返回 true 表示已成功加入队列
    bool async_write_stripe(uint64_t stripe_id, const std::string& data);
    
    // 同步写入条带（绕过异步队列，直接写入）
    bool sync_write_stripe(uint64_t stripe_id, const std::string& data);
    
    // 从本地缓存读取（如果后端还没有同步完成）
    bool read_from_cache(uint64_t stripe_id, std::string& out);
    
    // 检查条带是否在上传队列中
    bool is_pending(uint64_t stripe_id);
    
    // 等待特定条带上传完成
    void wait_for_stripe(uint64_t stripe_id);
    
    // 等待所有上传完成
    void flush();
    
    // 获取队列长度
    size_t queue_size() const;
    
    // 获取待上传任务数
    size_t pending_count() const;
    
    // 恢复上次未完成的上传（程序重启时调用）
    void recover_pending_uploads();

private:
    std::shared_ptr<RAIDChunkStore> raid_;
    AsyncUploadConfig config_;
    
    // 上传队列
    std::queue<UploadTask> queue_;
    mutable std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::condition_variable drain_cv_;  // 用于等待队列清空
    
    // 正在处理的任务（stripe_id -> 是否完成）
    std::unordered_map<uint64_t, bool> pending_stripes_;
    mutable std::mutex pending_mutex_;
    std::condition_variable pending_cv_;
    
    // 工作线程
    std::vector<std::thread> workers_;
    std::atomic<bool> running_{false};
    
    // 统计
    std::atomic<uint64_t> total_uploaded_{0};
    std::atomic<uint64_t> total_failed_{0};
    
    // 工作线程函数
    void worker_func();
    
    // 处理单个上传任务
    bool process_task(UploadTask& task);
    
    // 保存到本地缓存
    bool save_to_cache(uint64_t stripe_id, const std::string& data, std::string& cache_path);
    
    // 从本地缓存加载
    bool load_from_cache(const std::string& cache_path, std::string& data);
    
    // 删除本地缓存
    void remove_cache(const std::string& cache_path);
    
    // 生成缓存文件路径
    std::string get_cache_path(uint64_t stripe_id) const;
    
    // 确保缓存目录存在
    void ensure_cache_dir();
};

#endif // ASYNC_UPLOADER_H
