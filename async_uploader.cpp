#include "async_uploader.h"
#include <fstream>
#include <filesystem>
#include <cstdio>
#include <cinttypes>
#include <chrono>

namespace fs = std::filesystem;

AsyncUploader::AsyncUploader(std::vector<std::shared_ptr<ChunkStore>> backends,
                             std::shared_ptr<ErasureCoder> coder,
                             int k, int m,
                             const AsyncUploadConfig& config)
    : backends_(std::move(backends)), coder_(std::move(coder)),
      k_(k), m_(m), config_(config)
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

std::string AsyncUploader::get_chunk_cache_path(uint64_t stripe_id, int chunk_index) const {
    char buf[256];
    snprintf(buf, sizeof(buf), "%s/stripe_%020" PRIu64 "_chunk_%02d.dat", 
             config_.cache_dir.c_str(), stripe_id, chunk_index);
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
    
    fprintf(stderr, "AsyncUploader: 已停止，共上传 %" PRIu64 " 个 chunks，失败 %" PRIu64 " 个\n",
            total_chunks_uploaded_.load(), total_chunks_failed_.load());
}

void AsyncUploader::worker_func() {
    while (running_.load()) {
        ChunkUploadTask task;
        
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
            total_chunks_uploaded_.fetch_add(1);
            
            // 标记 chunk 完成，检查 stripe 是否全部完成
            bool stripe_done = mark_chunk_done(task.stripe_id);
            if (stripe_done) {
                pending_cv_.notify_all();
            }
            
            // 删除本地缓存
            remove_cache(task.local_cache_path);
        } else {
            // 重试
            task.retry_count++;
            if (task.retry_count < config_.max_retries) {
                fprintf(stderr, "AsyncUploader: stripe %" PRIu64 " chunk %d 上传失败，将重试 (%d/%d)\n",
                        task.stripe_id, task.chunk_index, task.retry_count, config_.max_retries);
                
                // 等待一段时间后重试
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(config_.retry_delay_ms * task.retry_count));
                
                {
                    std::lock_guard<std::mutex> lock(queue_mutex_);
                    queue_.push(std::move(task));
                }
                queue_cv_.notify_one();
            } else {
                fprintf(stderr, "AsyncUploader: stripe %" PRIu64 " chunk %d 上传失败，已达最大重试次数\n",
                        task.stripe_id, task.chunk_index);
                total_chunks_failed_.fetch_add(1);
                
                // 标记 chunk 完成（虽然失败了，但不再重试）
                bool stripe_done = mark_chunk_done(task.stripe_id);
                if (stripe_done) {
                    pending_cv_.notify_all();
                }
                
                // 保留本地缓存，以便后续手动恢复
            }
        }
        
        // 通知等待队列清空的线程
        drain_cv_.notify_all();
    }
}

bool AsyncUploader::process_task(ChunkUploadTask& task) {
    // 从本地缓存加载 chunk 数据
    std::string chunk_data;
    if (!load_chunk_from_cache(task.local_cache_path, chunk_data)) {
        fprintf(stderr, "AsyncUploader: 无法从缓存加载 stripe %" PRIu64 " chunk %d\n", 
                task.stripe_id, task.chunk_index);
        return false;
    }
    
    // 上传到对应的后端
    if (task.chunk_index < 0 || task.chunk_index >= (int)backends_.size()) {
        fprintf(stderr, "AsyncUploader: chunk_index %d 超出范围\n", task.chunk_index);
        return false;
    }
    
    return backends_[task.chunk_index]->write_chunk(task.stripe_id, (uint32_t)task.chunk_index, chunk_data);
}

bool AsyncUploader::save_chunk_to_cache(uint64_t stripe_id, int chunk_index,
                                        const std::string& data, std::string& cache_path) {
    cache_path = get_chunk_cache_path(stripe_id, chunk_index);
    
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

bool AsyncUploader::load_chunk_from_cache(const std::string& cache_path, std::string& data) {
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
    if (!coder_) {
        fprintf(stderr, "AsyncUploader: coder 为空\n");
        return false;
    }
    
    // 检查队列是否已满
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (queue_.size() >= config_.max_queue_size) {
            fprintf(stderr, "AsyncUploader: 队列已满，拒绝新任务\n");
            return false;
        }
    }
    
    // 1. 进行 RAID 编码
    std::vector<std::string> chunks;
    if (!coder_->encode(data, k_, m_, chunks)) {
        fprintf(stderr, "AsyncUploader: encode 失败, stripe=%" PRIu64 "\n", stripe_id);
        return false;
    }
    
    if ((int)chunks.size() != k_ + m_) {
        fprintf(stderr, "AsyncUploader: chunks 数量 != k+m\n");
        return false;
    }
    
    // 2. 将所有 chunks 保存到本地缓存
    std::vector<std::string> cache_paths(k_ + m_);
    for (int i = 0; i < k_ + m_; ++i) {
        if (!save_chunk_to_cache(stripe_id, i, chunks[i], cache_paths[i])) {
            // 保存失败，清理已保存的缓存
            for (int j = 0; j < i; ++j) {
                remove_cache(cache_paths[j]);
            }
            return false;
        }
    }
    
    // 3. 标记为 pending
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_stripes_[stripe_id] = k_ + m_;  // 待上传的 chunk 数量
    }
    
    // 4. 将所有 chunks 加入上传队列
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        for (int i = 0; i < k_ + m_; ++i) {
            queue_.emplace(stripe_id, i, cache_paths[i]);
        }
    }
    queue_cv_.notify_all();
    
    return true;
}

bool AsyncUploader::mark_chunk_done(uint64_t stripe_id) {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_stripes_.find(stripe_id);
    if (it == pending_stripes_.end()) {
        return true;  // 已经不在 pending 列表中
    }
    
    it->second--;
    if (it->second <= 0) {
        pending_stripes_.erase(it);
        return true;  // stripe 全部完成
    }
    return false;
}

bool AsyncUploader::read_from_cache(uint64_t stripe_id, std::string& out) {
    if (!coder_) {
        return false;
    }
    
    // 尝试从本地缓存加载所有 chunks
    std::vector<std::string> chunks(k_ + m_);
    int loaded_count = 0;
    
    for (int i = 0; i < k_ + m_; ++i) {
        std::string cache_path = get_chunk_cache_path(stripe_id, i);
        if (load_chunk_from_cache(cache_path, chunks[i])) {
            loaded_count++;
        }
    }
    
    // 需要至少 k 个 chunks 才能解码
    if (loaded_count < k_) {
        return false;
    }
    
    // 解码
    return coder_->decode(chunks, k_, m_, out);
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

size_t AsyncUploader::pending_stripe_count() const {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    return pending_stripes_.size();
}

void AsyncUploader::recover_pending_uploads() {
    fprintf(stderr, "AsyncUploader: 扫描未完成的上传任务...\n");
    
    try {
        if (!fs::exists(config_.cache_dir)) {
            return;
        }
        
        // 收集所有缓存的 chunks，按 stripe_id 分组
        std::unordered_map<uint64_t, std::vector<std::pair<int, std::string>>> stripe_chunks;
        
        for (const auto& entry : fs::directory_iterator(config_.cache_dir)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().filename().string();
                
                // 解析: stripe_XXXX_chunk_YY.dat
                if (filename.rfind("stripe_", 0) == 0) {
                    size_t chunk_pos = filename.find("_chunk_");
                    if (chunk_pos != std::string::npos) {
                        try {
                            std::string stripe_id_str = filename.substr(7, chunk_pos - 7);
                            std::string chunk_idx_str = filename.substr(chunk_pos + 7, 2);
                            
                            uint64_t stripe_id = std::stoull(stripe_id_str);
                            int chunk_index = std::stoi(chunk_idx_str);
                            std::string cache_path = entry.path().string();
                            
                            stripe_chunks[stripe_id].emplace_back(chunk_index, cache_path);
                        } catch (...) {
                            // 忽略无法解析的文件
                        }
                    }
                }
            }
        }
        
        int recovered_stripes = 0;
        int recovered_chunks = 0;
        
        for (auto& [stripe_id, chunks] : stripe_chunks) {
            // 标记为 pending
            {
                std::lock_guard<std::mutex> lock(pending_mutex_);
                pending_stripes_[stripe_id] = (int)chunks.size();
            }
            
            // 加入队列
            {
                std::lock_guard<std::mutex> lock(queue_mutex_);
                for (auto& [chunk_index, cache_path] : chunks) {
                    queue_.emplace(stripe_id, chunk_index, cache_path);
                    recovered_chunks++;
                }
            }
            
            recovered_stripes++;
        }
        
        if (recovered_stripes > 0) {
            fprintf(stderr, "AsyncUploader: 恢复了 %d 个 stripes 的 %d 个未完成 chunks\n", 
                    recovered_stripes, recovered_chunks);
            queue_cv_.notify_all();
        }
    } catch (const std::exception& e) {
        fprintf(stderr, "AsyncUploader: 恢复失败: %s\n", e.what());
    }
}
