#include "file_manager.h"
#include <algorithm>
#include <vector>
#include <cstring>

FileManager::FileManager(std::shared_ptr<RAIDChunkStore> raid_store)
    : raid(std::move(raid_store)), file_size(0)
{
}

uint64_t FileManager::get_size(const std::string &path) const
{
    (void)path; // 只支持一个文件
    return file_size;
}

bool FileManager::truncate(const std::string &path, uint64_t new_size)
{
    (void)path;
    // 极简实现：只更新内存中的 file_size
    // 不立即清理多余的 stripe（懒删除）
    file_size = new_size;
    return true;
}

bool FileManager::read(const std::string &path,
                       uint64_t offset,
                       size_t size,
                       std::string &out)
{
    (void)path;

    if (offset >= file_size) {
        out.clear();
        return true; // 读 EOF
    }

    if (offset + size > file_size) {
        size = (size_t)(file_size - offset);
    }

    out.clear();
    out.reserve(size);

    uint64_t pos = offset;
    size_t remaining = size;

    while (remaining > 0) {
        uint64_t stripe_id = pos / STRIPE_SIZE;
        uint64_t stripe_offset = pos % STRIPE_SIZE;
        size_t to_read = (size_t)std::min<uint64_t>(
            remaining, STRIPE_SIZE - stripe_offset);

        std::string stripe_data;
        if (!raid->read_chunk(stripe_id, 0, stripe_data)) {
            // 如果条带不存在，按 0 填充
            stripe_data.assign((size_t)STRIPE_SIZE, 0);
        }

        if (stripe_offset >= stripe_data.size()) {
            // 条带数据太短，补齐
            stripe_data.resize((size_t)stripe_offset + to_read, 0);
        }

        out.append(stripe_data.data() + stripe_offset, to_read);

        pos += to_read;
        remaining -= to_read;
    }

    return true;
}

bool FileManager::write(const std::string &path,
                        uint64_t offset,
                        const char *data,
                        size_t size)
{
    (void)path;

    uint64_t pos = offset;
    size_t remaining = size;

    while (remaining > 0) {
        uint64_t stripe_id = pos / STRIPE_SIZE;
        uint64_t stripe_offset = pos % STRIPE_SIZE;
        size_t to_write = (size_t)std::min<uint64_t>(
            remaining, STRIPE_SIZE - stripe_offset);

        std::string stripe_data;
        if (!raid->read_chunk(stripe_id, 0, stripe_data)) {
            stripe_data.assign((size_t)STRIPE_SIZE, 0);
        }

        if (stripe_data.size() < STRIPE_SIZE) {
            stripe_data.resize((size_t)STRIPE_SIZE, 0);
        }

        std::memcpy(&stripe_data[(size_t)stripe_offset], data, to_write);

        if (!raid->write_chunk(stripe_id, 0, stripe_data)) {
            return false;
        }

        pos += to_write;
        data += to_write;
        remaining -= to_write;
    }

    // 更新文件大小
    uint64_t end_pos = offset + size;
    if (end_pos > file_size) {
        file_size = end_pos;
    }

    return true;
}
