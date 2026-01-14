#ifndef METADATA_MANAGER_H
#define METADATA_MANAGER_H

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <cstdint>
#include "path_trie.h"

class FileManager; // 前置声明

struct FileMeta {
    uint64_t size = 0;
    std::vector<uint64_t> stripes;
};

class MetadataManager {
public:
    MetadataManager();

    // 从 CloudRaidFS 内部文件加载元数据
    bool load_from_backend(FileManager* fm);

    // 保存元数据到 CloudRaidFS 内部文件
    bool save_to_backend(FileManager* fm);

    // 基本操作
    bool exists(const std::string& path);
    FileMeta* get(const std::string& path);

    void create_file(const std::string& path);
    void remove_file(const std::string& path);

    std::vector<std::string> list_dir(const std::string& path);

    void set_size(const std::string& path, uint64_t size);
    uint64_t get_size(const std::string& path);

    void add_stripe(const std::string& path, uint64_t stripe_id);
    const std::vector<uint64_t>& get_stripes(const std::string& path);

    // 目录操作
    bool create_dir(const std::string& path);
    bool remove_dir(const std::string& path);
    bool is_dir(const std::string& path);
    bool is_empty_dir(const std::string& path);

    // 重命名操作
    bool rename(const std::string& old_path, const std::string& new_path);

private:
    std::unordered_map<std::string, FileMeta> files;
    std::unordered_set<std::string> directories;  // 显式创建的目录
    PathTrie trie;

    static constexpr const char* META_PATH = "/.__cloudraidfs_meta";
};

#endif
