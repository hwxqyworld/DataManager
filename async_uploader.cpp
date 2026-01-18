#include "async_uploader.h"
#include <fstream>
#include <filesystem>
#include <cstdio>
#include <cinttypes>
#include <chrono>

namespace fs = std::filesystem;

AsyncUploader::AsyncUploader(std::shared_ptr<RAIDChunkStore> raid_store,
                             const AsyncUploadConfig& config)
    : raid_(std::move(raid_store)), config_(config)
{
    ensure_cache_dir();
}

AsyncUploader::~AsyncUploader() {
    stop();
}

void AsyncUploader::ensure_cache_dir() {
    try {
        if (!fs::exists(config_.cache_dir)) {
            fs::create_directories(config_.cache_dir);
        }
    } catch (const std::exception& e) {
        fprintf(stderr, "AsyncUploader: 创建缓存目录失败: %s\n", e.what());
    }
}

std::string AsyncUploader::get_cache_path(uint64_t stripe_id) const {
    char buf[256];
    snprintf(buf, sizeof(buf), "%s/stripe_%020" PRIu64 ".dat", 
             config_.cache_dir.c_str(), stripe_id);
    return std::string(buf);
}

void AsyncUploader::start() {
    if (running_.exchange(true)) {
        return;  // 已经在运行
    }
    
    fprintf(stderr, "AsyncUploader: 启动 %d 个后台上传线程\n", config_.worker_threads);
    
    for (int i = 0; i < config_.worker_threads; ++i) {
        workers_.emplace_back(&AsyncUploader::worker_func, this);
    }
}

void AsyncUploader::stop() {
    if (!running_.exchange(false)) {
        return;  // 已经停止
    }
    
    fprintf(stderr, "AsyncUploader: 正在停止，等待队列完成...\n");
    
    // 唤醒所有等待的线程
    queue_cv_.notify_all();
    
    // 等待所有工作线程结束
    for (auto& t : workers_) {
        if (t.joinable()) {
            t.join();
        }
    }
    workers_.clear();
    
    fprintf(stderr, "AsyncUploader: 已停止，共上传 %" PRIu64 " 个条带，失败 %" PRIu64 " 个\n",
            total_uploaded_.load(), total_failed_.load());
}

void AsyncUploader::worker_func() {
    while (running_.load()) {
        UploadTask task;
        
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            
            // 等待任务或停止信号
            queue_cv_.wait(lock, [this] {
                return !queue_.empty() || !running_.load();
            });
            
            if (!running_.load() && queue_.empty()) {
                break;
            }
            
            if (queue_.empty()) {
                continue;
            }
            
            task = std::move(queue_.front());
            queue_.pop();
        }
        
        // 处理任务
        bool success = process_task(task);
        
        if (success) {
            total_uploaded_.fetch_add(1);
            
            // 从 pending 列表中移除
            {
                std::lock_guard<std::mutex> lock(pending_mutex_);
                pending_stripes_.erase(task.stripe_id);
            }
            pending_cv_.notify_all();
            
            // 删除本地缓存
            remove_cache(task.local_cache_path);
        } else {
            // 重试
            task.retry_count++;
            if (task.retry_count < config_.max_retries) {
                fprintf(stderr, "AsyncUploader: 条带 %" PRIu64 " 上传失败，将重试 (%d/%d)\n",
                        task.stripe_id, task.retry_count, config_.max_retries);
                
                // 等待一段时间后重试
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(config_.retry_delay_ms * task.retry_count));
                
                {
                    std::lock_guard<std::mutex> lock(queue_mutex_);
                    queue_.push(std::move(task));
                }
                queue_cv_.notify_one();
            } else {
                fprintf(stderr, "AsyncUploader: 条带 %" PRIu64 " 上传失败，已达最大重试次数\n",
                        task.stripe_id);
                total_failed_.fetch_add(1);
                
                // 从 pending 列表中移除（标记为失败）
                {
                    std::lock_guard<std::mutex> lock(pending_mutex_);
                    pending_stripes_.erase(task.stripe_id);
                }
                pending_cv_.notify_all();
                
                // 保留本地缓存，以便后续手动恢复
            }
        }
        
        // 通知等待队列清空的线程
        drain_cv_.notify_all();
    }
}

bool AsyncUploader::process_task(UploadTask& task) {
    // 如果数据为空，从本地缓存加载
    if (task.data.empty() && !task.local_cache_path.empty()) {
        if (!load_from_cache(task.local_cache_path, task.data)) {
            fprintf(stderr, "AsyncUploader: 无法从缓存加载条带 %" PRIu64 "\n", task.stripe_id);
            return false;
        }
    }
    
    // 上传到后端
    return raid_->write_chunk(task.stripe_id, 0, task.data);
}

bool AsyncUploader::save_to_cache(uint64_t stripe_id, const std::string& data, 
                                   std::string& cache_path) {
    cache_path = get_cache_path(stripe_id);
    
    std::ofstream file(cache_path, std::ios::binary);
    if (!file) {
        fprintf(stderr, "AsyncUploader: 无法创建缓存文件: %s\n", cache_path.c_str());
        return false;
    }
    
    file.write(data.data(), data.size());
    if (!file) {
        fprintf(stderr, "AsyncUploader: 写入缓存文件失败: %s\n", cache_path.c_str());
        return false;
    }
    
    return true;
}

bool AsyncUploader::load_from_cache(const std::string& cache_path, std::string& data) {
    std::ifstream file(cache_path, std::ios::binary | std::ios::ate);
    if (!file) {
        return false;
    }
    
    auto size = file.tellg();
    file.seekg(0, std::ios::beg);
    
    data.resize(size);
    file.read(&data[0], size);
    
    return file.good();
}

void AsyncUploader::remove_cache(const std::string& cache_path) {
    try {
        if (!cache_path.empty() && fs::exists(cache_path)) {
            fs::remove(cache_path);
        }
    } catch (const std::exception& e) {
        fprintf(stderr, "AsyncUploader: 删除缓存文件失败: %s\n", e.what());
    }
}

bool AsyncUploader::async_write_stripe(uint64_t stripe_id, const std::string& data) {
    // 检查队列是否已满
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (queue_.size() >= config_.max_queue_size) {
            fprintf(stderr, "AsyncUploader: 队列已满，拒绝新任务\n");
            return false;
        }
    }
    
    // 保存到本地缓存
    std::string cache_path;
    if (!save_to_cache(stripe_id, data, cache_path)) {
        return false;
    }
    
    // 标记为 pending
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_stripes_[stripe_id] = false;
    }
    
    // 加入队列
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queue_.emplace(stripe_id, data, cache_path);
    }
    queue_cv_.notify_one();
    
    return true;
}

bool AsyncUploader::sync_write_stripe(uint64_t stripe_id, const std::string& data) {
    return raid_->write_chunk(stripe_id, 0, data);
}

bool AsyncUploader::read_from_cache(uint64_t stripe_id, std::string& out) {
    std::string cache_path = get_cache_path(stripe_id);
    return load_from_cache(cache_path, out);
}

bool AsyncUploader::is_pending(uint64_t stripe_id) {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    return pending_stripes_.find(stripe_id) != pending_stripes_.end();
}

void AsyncUploader::wait_for_stripe(uint64_t stripe_id) {
    std::unique_lock<std::mutex> lock(pending_mutex_);
    pending_cv_.wait(lock, [this, stripe_id] {
        return pending_stripes_.find(stripe_id) == pending_stripes_.end();
    });
}

void AsyncUploader::flush() {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    drain_cv_.wait(lock, [this] {
        return queue_.empty();
    });
    
    // 还需要等待所有 pending 任务完成
    std::unique_lock<std::mutex> pending_lock(pending_mutex_);
    pending_cv_.wait(pending_lock, [this] {
        return pending_stripes_.empty();
    });
}

size_t AsyncUploader::queue_size() const {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    return queue_.size();
}

size_t AsyncUploader::pending_count() const {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    return pending_stripes_.size();
}

void AsyncUploader::recover_pending_uploads() {
    fprintf(stderr, "AsyncUploader: 扫描未完成的上传任务...\n");
    
    try {
        if (!fs::exists(config_.cache_dir)) {
            return;
        }
        
        int recovered = 0;
        for (const auto& entry : fs::directory_iterator(config_.cache_dir)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                
                // 解析 stripe_id: stripe_XXXX.dat
                if (filename.rfind("stripe_", 0) == 0 && filename.size() > 11) {
                    std::string id_str = filename.substr(7, filename.size() - 11);
                    try {
                        uint64_t stripe_id = std::stoull(id_str);
                        std::string cache_path = entry.path().string();
                        
                        // 标记为 pending
                        {
                            std::lock_guard<std::mutex> lock(pending_mutex_);
                            pending_stripes_[stripe_id] = false;
                        }
                        
                        // 加入队列（数据稍后从缓存加载）
                        {
                            std::lock_guard<std::mutex> lock(queue_mutex_);
                            queue_.emplace(stripe_id, std::string(), cache_path);
                        }
                        
                        recovered++;
                    } catch (...) {
                        // 忽略无法解析的文件
                    }
                }
            }
        }
        
        if (recovered > 0) {
            fprintf(stderr, "AsyncUploader: 恢复了 %d 个未完成的上传任务\n", recovered);
            queue_cv_.notify_all();
        }
    } catch (const std::exception& e) {
        fprintf(stderr, "AsyncUploader: 恢复失败: %s\n", e.what());
    }
}
