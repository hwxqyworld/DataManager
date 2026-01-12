#include "metadata_manager.h"
#include "file_manager.h"

#include <cstring>
#include <iostream>

MetadataManager::MetadataManager() {}

// ------------------------------------------------------------
// 从 CloudRaidFS 内部文件加载元数据
// ------------------------------------------------------------
bool MetadataManager::load_from_backend(FileManager* fm) {
    // 尝试读取一段上限数据，如果读不到就认为没有元数据
    const uint64_t MAX_META_SIZE = 16ull * 1024 * 1024;

    std::string data;
    if (!fm->read(META_PATH, 0, MAX_META_SIZE, data)) {
        std::cerr << "MetadataManager: no metadata file, starting empty\n";
        return false;
    }

    if (data.empty()) {
        std::cerr << "MetadataManager: metadata file empty\n";
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

        FileMeta fm;

        if (!read_u64(fm.size)) return false;

        uint32_t stripe_count = 0;
        if (!read_u32(stripe_count)) return false;

        fm.stripes.resize(stripe_count);
        for (uint32_t j = 0; j < stripe_count; j++) {
            if (!read_u64(fm.stripes[j])) return false;
        }

        files[path] = fm;
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

    uint32_t file_count = static_cast<uint32_t>(files.size());
    write_u32(file_count);

    for (auto it = files.begin(); it != files.end(); ++it) {
        const std::string& path = it->first;
        const FileMeta& meta    = it->second;

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
