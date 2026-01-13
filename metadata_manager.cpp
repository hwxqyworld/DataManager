#include "metadata_manager.h"
#include "file_manager.h"

#include <cstring>
#include <iostream>

MetadataManager::MetadataManager() {}

// ------------------------------------------------------------
// 从 CloudRaidFS 内部文件加载元数据
// ------------------------------------------------------------
bool MetadataManager::load_from_backend(FileManager* fm) {
    // 首先注册元数据文件自身（使用保留的 stripe 0）
    // 这样 FileManager::read 才能找到正确的 stripe
    if (!exists(META_PATH)) {
        create_file(META_PATH);
    }
    files[META_PATH].stripes.clear();
    files[META_PATH].stripes.push_back(0);  // 保留 stripe 0 给元数据文件
    files[META_PATH].size = 16ULL * 1024 * 1024;  // 先设置一个足够大的值

    // 尝试读取一段上限数据，如果读不到就认为没有元数据
    const uint64_t MAX_META_SIZE = 16ull * 1024 * 1024;

    std::string data;
    if (!fm->read(META_PATH, 0, MAX_META_SIZE, data)) {
        std::cerr << "MetadataManager: no metadata file, starting empty\n";
        // 清空元数据文件的注册（保留 stripe 0）
        files.clear();
        trie = PathTrie();
        return false;
    }

    if (data.empty()) {
        std::cerr << "MetadataManager: metadata file empty\n";
        files.clear();
        trie = PathTrie();
        return false;
    }

    const char* p   = data.data();
    const char* end = p + data.size();

    auto read_u32 = [&](uint32_t& v) {
        if (p + 4 > end) return false;
        std::memcpy(&v, p, 4);
        p += 4;
        return true;
    };

    auto read_u64 = [&](uint64_t& v) {
        if (p + 8 > end) return false;
        std::memcpy(&v, p, 8);
        p += 8;
        return true;
    };

    files.clear();
    trie = PathTrie();

    uint32_t file_count = 0;
    if (!read_u32(file_count)) return false;

    for (uint32_t i = 0; i < file_count; i++) {
        uint32_t path_len = 0;
        if (!read_u32(path_len)) return false;

        if (p + path_len > end) return false;
        std::string path(p, path_len);
        p += path_len;

        FileMeta meta;

        if (!read_u64(meta.size)) return false;

        uint32_t stripe_count = 0;
        if (!read_u32(stripe_count)) return false;

        meta.stripes.resize(stripe_count);
        for (uint32_t j = 0; j < stripe_count; j++) {
            if (!read_u64(meta.stripes[j])) return false;
        }

        files[path] = meta;
        trie.insert(path);
    }

    return true;
}

// ------------------------------------------------------------
// 保存元数据到 CloudRaidFS 内部文件
// ------------------------------------------------------------
bool MetadataManager::save_to_backend(FileManager* fm) {
    std::string data;

    auto write_u32 = [&](uint32_t v) {
        data.append(reinterpret_cast<const char*>(&v), 4);
    };

    auto write_u64 = [&](uint64_t v) {
        data.append(reinterpret_cast<const char*>(&v), 8);
    };

    // 排除元数据文件自身，只序列化用户文件
    uint32_t file_count = 0;
    for (auto it = files.begin(); it != files.end(); ++it) {
        if (it->first != META_PATH) {
            file_count++;
        }
    }
    write_u32(file_count);

    for (auto it = files.begin(); it != files.end(); ++it) {
        const std::string& path = it->first;
        const FileMeta& meta    = it->second;

        // 跳过元数据文件自身
        if (path == META_PATH) {
            continue;
        }

        uint32_t path_len = static_cast<uint32_t>(path.size());
        write_u32(path_len);
        data.append(path.data(), path_len);

        write_u64(meta.size);

        uint32_t stripe_count = static_cast<uint32_t>(meta.stripes.size());
        write_u32(stripe_count);

        for (size_t i = 0; i < meta.stripes.size(); ++i) {
            write_u64(meta.stripes[i]);
        }
    }

    // 确保元数据文件已注册（使用保留的 stripe ID）
    if (!exists(META_PATH)) {
        create_file(META_PATH);
    }
    // 清空旧的 stripe 列表，重新分配保留的 stripe
    files[META_PATH].stripes.clear();
    
    // 计算需要多少个 stripe（每个 stripe 4MB）
    const uint64_t STRIPE_SIZE = 4ULL * 1024 * 1024;
    uint64_t num_stripes = (data.size() + STRIPE_SIZE - 1) / STRIPE_SIZE;
    if (num_stripes == 0) num_stripes = 1;
    
    // 使用保留的 stripe ID（从 0 开始）
    for (uint64_t i = 0; i < num_stripes; ++i) {
        files[META_PATH].stripes.push_back(i);  // 保留 stripe 0, 1, 2, ...
    }
    files[META_PATH].size = data.size();

    // 写入 CloudRaidFS 内部文件
    return fm->write(META_PATH, 0, data.data(), data.size());
}

// ------------------------------------------------------------
// 基本操作
// ------------------------------------------------------------
bool MetadataManager::exists(const std::string& path) {
    return files.count(path) > 0;
}

FileMeta* MetadataManager::get(const std::string& path) {
    if (!exists(path)) return nullptr;
    return &files[path];
}

void MetadataManager::create_file(const std::string& path) {
    if (!exists(path)) {
        files[path] = FileMeta();
    }
    trie.insert(path);
}

void MetadataManager::remove_file(const std::string& path) {
    if (!exists(path)) return;
    files.erase(path);
    trie.remove(path);
}

std::vector<std::string> MetadataManager::list_dir(const std::string& path) {
    return trie.list_dir(path);
}

void MetadataManager::set_size(const std::string& path, uint64_t size) {
    if (!exists(path)) create_file(path);
    files[path].size = size;
}

uint64_t MetadataManager::get_size(const std::string& path) {
    if (!exists(path)) return 0;
    return files[path].size;
}

void MetadataManager::add_stripe(const std::string& path, uint64_t stripe_id) {
    if (!exists(path)) create_file(path);
    files[path].stripes.push_back(stripe_id);
}

const std::vector<uint64_t>& MetadataManager::get_stripes(const std::string& path) {
    return files[path].stripes;
}
