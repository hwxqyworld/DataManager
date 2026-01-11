#include "file_manager.h"
#include <algorithm>
#include <vector>
#include <cstring>
#include <cinttypes>

FileManager::FileManager(std::shared_ptr<RAIDChunkStore> raid_store)
    : raid(std::move(raid_store)), file_size(0)
{
}

uint64_t FileManager::get_size(const std::string &path) const
{
    (void)path; // 目前只管理一个文件
    return file_size;
}

bool FileManager::truncate(const std::string &path, uint64_t new_size)
{
    (void)path;
    // 极简实现：只更新内存中的 file_size
    file_size = new_size;
    return true;
}

bool FileManager::read(const std::string &path,
                       uint64_t offset,
                       size_t size,
                       std::string &out)
{
    (void)path;

    fprintf(stderr,
            "[FM_READ] offset=%" PRIu64 " size=%zu\n",
            (uint64_t)offset, size);

    if (offset >= file_size) {
        out.clear();
        return true; // EOF
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

        fprintf(stderr,
                " [FM_READ_CHUNK] stripe_id=%" PRIu64
                " stripe_offset=%" PRIu64
                " to_read=%zu\n",
                (uint64_t)stripe_id,
                (uint64_t)stripe_offset,
                to_read);

        std::string stripe_data;

        // 尝试从 RAID 读取整个条带
        if (!raid->read_chunk(stripe_id, 0, stripe_data)) {
            // 条带不存在或解码失败：当作全 0 条带
            stripe_data.assign((size_t)STRIPE_SIZE, 0);
        }

        // 保证条带长度覆盖到我们要读取的范围
        if (stripe_offset + to_read > stripe_data.size()) {
            stripe_data.resize(stripe_offset + to_read, 0);
        }

        out.append(stripe_data.data() + stripe_offset, to_read);

        pos       += to_read;
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

    fprintf(stderr,
            "[FM_WRITE] offset=%" PRIu64 " size=%zu\n",
            (uint64_t)offset, size);

    uint64_t pos = offset;
    size_t remaining = size;

    while (remaining > 0) {
        uint64_t stripe_id = pos / STRIPE_SIZE;
        uint64_t stripe_offset = pos % STRIPE_SIZE;
        size_t to_write = (size_t)std::min<uint64_t>(
            remaining, STRIPE_SIZE - stripe_offset);

        fprintf(stderr,
                " [FM_WRITE_CHUNK] stripe_id=%" PRIu64
                " stripe_offset=%" PRIu64
                " to_write=%zu\n",
                (uint64_t)stripe_id,
                (uint64_t)stripe_offset,
                to_write);

        std::string stripe_data;

        // 尝试读取已有条带数据
        if (!raid->read_chunk(stripe_id, 0, stripe_data)) {
            // 条带完全不存在：构造一条全 0 条带
            stripe_data.assign((size_t)STRIPE_SIZE, 0);
        } else {
            // 条带存在：保证长度至少一个条带大小
            if (stripe_data.size() < STRIPE_SIZE) {
                stripe_data.resize((size_t)STRIPE_SIZE, 0);
            }
        }

        // 把本次写入覆盖到条带的对应位置
        std::memcpy(&stripe_data[(size_t)stripe_offset], data, to_write);

        // 整条带写回 RAID
        if (!raid->write_chunk(stripe_id, 0, stripe_data)) {
            fprintf(stderr,
                    "FileManager::write: write_chunk 失败, stripe_id=%" PRIu64 "\n",
                    (uint64_t)stripe_id);
            return false;
        }

        pos       += to_write;
        data      += to_write;
        remaining -= to_write;
    }

    // 更新文件大小
    uint64_t end_pos = offset + size;
    if (end_pos > file_size) {
        file_size = end_pos;
    }

    return true;
}
