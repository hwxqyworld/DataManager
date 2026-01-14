#include "metadata_manager.h"
#include "file_manager.h"

#include <cstring>
#include <iostream>
#include <algorithm>

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
    if (!fm->read(META_PATH, 0, MAX_META_SIZE, data) || data.empty()) {
        // 首次启动或读取失败，保持元数据文件的注册状态
        // 清空其他文件，只保留元数据文件自身
        files.clear();
        directories.clear();
        trie.clear();
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
    directories.clear();
    trie.clear();

    // 读取文件数量
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

    // 读取目录数量
    uint32_t dir_count = 0;
    if (!read_u32(dir_count)) return false;

    for (uint32_t i = 0; i < dir_count; i++) {
        uint32_t path_len = 0;
        if (!read_u32(path_len)) return false;

        if (p + path_len > end) return false;
        std::string path(p, path_len);
        p += path_len;

        directories.insert(path);
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

    // 序列化目录
    uint32_t dir_count = static_cast<uint32_t>(directories.size());
    write_u32(dir_count);

    for (const auto& dir_path : directories) {
        uint32_t path_len = static_cast<uint32_t>(dir_path.size());
        write_u32(path_len);
        data.append(dir_path.data(), path_len);
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
    static const std::vector<uint64_t> empty_stripes;
    auto it = files.find(path);
    if (it == files.end()) {
        return empty_stripes;
    }
    return it->second.stripes;
}

// ------------------------------------------------------------
// 目录操作
// ------------------------------------------------------------
bool MetadataManager::create_dir(const std::string& path) {
    if (path.empty() || path == "/") {
        return false;  // 根目录已存在
    }
    
    // 检查是否已存在同名文件
    if (exists(path)) {
        return false;
    }
    
    // 检查是否已存在同名目录
    if (directories.count(path) > 0) {
        return false;
    }
    
    // 检查父目录是否存在（根目录或已存在的目录）
    size_t last_slash = path.rfind('/');
    if (last_slash != std::string::npos && last_slash > 0) {
        std::string parent = path.substr(0, last_slash);
        if (!is_dir(parent) && parent != "/") {
            return false;  // 父目录不存在
        }
    }
    
    directories.insert(path);
    trie.insert(path);
    return true;
}

bool MetadataManager::remove_dir(const std::string& path) {
    if (path.empty() || path == "/") {
        return false;  // 不能删除根目录
    }
    
    // 检查目录是否存在
    if (directories.count(path) == 0) {
        // 可能是隐式目录（由文件路径创建）
        auto children = list_dir(path);
        if (children.empty()) {
            return false;  // 目录不存在
        }
        return false;  // 隐式目录非空，不能删除
    }
    
    // 检查目录是否为空
    auto children = list_dir(path);
    if (!children.empty()) {
        return false;  // 目录非空
    }
    
    directories.erase(path);
    trie.remove(path);
    return true;
}

bool MetadataManager::is_dir(const std::string& path) {
    if (path == "/") {
        return true;  // 根目录
    }
    
    // 显式创建的目录
    if (directories.count(path) > 0) {
        return true;
    }
    
    // 隐式目录（有子项但不是文件）
    if (!exists(path)) {
        auto children = list_dir(path);
        if (!children.empty()) {
            return true;
        }
    }
    
    return false;
}

bool MetadataManager::is_empty_dir(const std::string& path) {
    if (!is_dir(path)) {
        return false;
    }
    auto children = list_dir(path);
    return children.empty();
}

// ------------------------------------------------------------
// 重命名操作
// ------------------------------------------------------------
bool MetadataManager::rename(const std::string& old_path, const std::string& new_path) {
    if (old_path.empty() || new_path.empty() || 
        old_path == "/" || new_path == "/") {
        return false;
    }
    
    // 检查新路径是否已存在
    if (exists(new_path) || directories.count(new_path) > 0) {
        return false;
    }
    
    // 检查新路径的父目录是否存在
    size_t last_slash = new_path.rfind('/');
    if (last_slash != std::string::npos && last_slash > 0) {
        std::string parent = new_path.substr(0, last_slash);
        if (!is_dir(parent) && parent != "/") {
            return false;  // 父目录不存在
        }
    }
    
    // 重命名文件
    if (exists(old_path)) {
        FileMeta meta = files[old_path];
        files.erase(old_path);
        trie.remove(old_path);
        
        files[new_path] = meta;
        trie.insert(new_path);
        return true;
    }
    
    // 重命名目录
    if (directories.count(old_path) > 0) {
        // 收集所有需要移动的文件和子目录
        std::vector<std::pair<std::string, FileMeta>> files_to_move;
        std::vector<std::string> dirs_to_move;
        
        // 查找所有以 old_path 开头的文件
        for (auto it = files.begin(); it != files.end(); ++it) {
            const std::string& file_path = it->first;
            if (file_path.size() > old_path.size() &&
                file_path.substr(0, old_path.size()) == old_path &&
                file_path[old_path.size()] == '/') {
                std::string new_file_path = new_path + file_path.substr(old_path.size());
                files_to_move.push_back({file_path, it->second});
            }
        }
        
        // 查找所有以 old_path 开头的子目录
        for (const auto& dir : directories) {
            if (dir.size() > old_path.size() &&
                dir.substr(0, old_path.size()) == old_path &&
                dir[old_path.size()] == '/') {
                dirs_to_move.push_back(dir);
            }
        }
        
        // 移动文件
        for (const auto& pair : files_to_move) {
            std::string new_file_path = new_path + pair.first.substr(old_path.size());
            files.erase(pair.first);
            trie.remove(pair.first);
            files[new_file_path] = pair.second;
            trie.insert(new_file_path);
        }
        
        // 移动子目录
        for (const auto& dir : dirs_to_move) {
            std::string new_dir_path = new_path + dir.substr(old_path.size());
            directories.erase(dir);
            trie.remove(dir);
            directories.insert(new_dir_path);
            trie.insert(new_dir_path);
        }
        
        // 移动目录本身
        directories.erase(old_path);
        trie.remove(old_path);
        directories.insert(new_path);
        trie.insert(new_path);
        
        return true;
    }
    
    // 隐式目录的重命名（有子项但未显式创建）
    auto children = list_dir(old_path);
    if (!children.empty()) {
        // 收集所有需要移动的文件和子目录
        std::vector<std::pair<std::string, FileMeta>> files_to_move;
        std::vector<std::string> dirs_to_move;
        
        // 查找所有以 old_path 开头的文件
        for (auto it = files.begin(); it != files.end(); ++it) {
            const std::string& file_path = it->first;
            if (file_path.size() > old_path.size() &&
                file_path.substr(0, old_path.size()) == old_path &&
                file_path[old_path.size()] == '/') {
                files_to_move.push_back({file_path, it->second});
            }
        }
        
        // 查找所有以 old_path 开头的子目录
        for (const auto& dir : directories) {
            if (dir.size() > old_path.size() &&
                dir.substr(0, old_path.size()) == old_path &&
                dir[old_path.size()] == '/') {
                dirs_to_move.push_back(dir);
            }
        }
        
        // 移动文件
        for (const auto& pair : files_to_move) {
            std::string new_file_path = new_path + pair.first.substr(old_path.size());
            files.erase(pair.first);
            trie.remove(pair.first);
            files[new_file_path] = pair.second;
            trie.insert(new_file_path);
        }
        
        // 移动子目录
        for (const auto& dir : dirs_to_move) {
            std::string new_dir_path = new_path + dir.substr(old_path.size());
            directories.erase(dir);
            trie.remove(dir);
            directories.insert(new_dir_path);
            trie.insert(new_dir_path);
        }
        
        return true;
    }
    
    return false;  // 源路径不存在
}
