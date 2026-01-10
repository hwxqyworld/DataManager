#ifndef FILE_MANAGER_H
#define FILE_MANAGER_H

#include "raid_chunk_store.h"
#include <cstdint>
#include <memory>
#include <string>

// 极简 FileManager：
// 只管理一个逻辑文件 "/file"
// 把文件视为一串连续的条带（stripe）
// stripe_id = offset / STRIPE_SIZE

class FileManager {
public:
    static const uint64_t STRIPE_SIZE = 4ULL * 1024 * 1024; // 4 MiB

    explicit FileManager(std::shared_ptr<RAIDChunkStore> raid_store);

    // 读取 [offset, offset+size) 范围的数据
    bool read(const std::string &path,
              uint64_t offset,
              size_t size,
              std::string &out);

    // 写入 [offset, offset+size) 范围的数据（覆盖写）
    bool write(const std::string &path,
               uint64_t offset,
               const char *data,
               size_t size);

    // 获取逻辑文件大小
    uint64_t get_size(const std::string &path) const;

    // 截断文件
    bool truncate(const std::string &path, uint64_t new_size);

private:
    std::shared_ptr<RAIDChunkStore> raid;
    // 目前只管理一个文件，直接用一个成员变量记录大小
    uint64_t file_size;
};

#endif // FILE_MANAGER_H
