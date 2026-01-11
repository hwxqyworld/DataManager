#include "metadata_manager.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

MetadataManager::MetadataManager() {
    // 空构造
}

bool MetadataManager::load(const std::string& filename) {
    std::ifstream f(filename);
    if (!f.is_open()) {
        std::cerr << "MetadataManager: metadata file not found, starting empty\n";
        return false;
    }

    json j;
    f >> j;

    files.clear();
    trie = PathTrie(); // 重建 Trie

    if (!j.contains("files")) return true;

    for (auto& [path, meta] : j["files"].items()) {
        FileMeta fm;
        fm.size = meta["size"].get<uint64_t>();
        fm.stripes = meta["stripes"].get<std::vector<uint64_t>>();

        files[path] = fm;
        trie.insert(path);
    }

    return true;
}

bool MetadataManager::save(const std::string& filename) {
    json j;
    j["files"] = json::object();

    for (auto& [path, meta] : files) {
        j["files"][path] = {
            {"size", meta.size},
            {"stripes", meta.stripes}
        };
    }

    std::ofstream f(filename);
    if (!f.is_open()) {
        std::cerr << "MetadataManager: failed to write metadata file\n";
        return false;
    }

    f << j.dump(4);
    return true;
}

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
