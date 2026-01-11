#ifndef CLOUDRAIDFS_METADATA_MANAGER_H
#define CLOUDRAIDFS_METADATA_MANAGER_H

#include <string>
#include <unordered_map>
#include <vector>
#include <stdint.h>
#include "path_trie.h"

// 单个文件的元数据
struct FileMeta {
    uint64_t size = 0;                 // 文件大小
    std::vector<uint64_t> stripes;     // 文件对应的 stripe_id 列表
};

class MetadataManager {
public:
    MetadataManager();

    // 文件是否存在
    bool exists(const std::string& path);

    // 获取文件元数据（不存在返回 nullptr）
    FileMeta* get(const std::string& path);

    // 创建文件（如果已存在则覆盖）
    void create_file(const std::string& path);

    // 删除文件
    void remove_file(const std::string& path);

    // 列出目录内容
    std::vector<std::string> list_dir(const std::string& path);

    // 设置文件大小
    void set_size(const std::string& path, uint64_t size);

    // 获取文件大小
    uint64_t get_size(const std::string& path);

    // stripe 管理
    void add_stripe(const std::string& path, uint64_t stripe_id);
    const std::vector<uint64_t>& get_stripes(const std::string& path);

    // TODO: 后续加入 load/save metadata.json

private:
    std::unordered_map<std::string, FileMeta> files;
    PathTrie trie;
};

#endif
