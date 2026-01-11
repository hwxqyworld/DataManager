#ifndef CLOUDRAIDFS_PATH_TRIE_H
#define CLOUDRAIDFS_PATH_TRIE_H

#include <string>
#include <unordered_map>
#include <vector>
#include <sstream>

class PathTrie {
public:
    struct Node {
        bool is_file = false;
        std::unordered_map<std::string, Node*> children;
    };

    PathTrie() {
        root = new Node();
    }

    ~PathTrie() {
        free_node(root);
    }

    // 插入路径，例如 "/sub/a.txt"
    void insert(const std::string& path) {
        std::vector<std::string> parts = split(path);
        Node* cur = root;

        for (auto& p : parts) {
            if (!cur->children.count(p)) {
                cur->children[p] = new Node();
            }
            cur = cur->children[p];
        }
        cur->is_file = true;
    }

    // 删除路径
    void remove(const std::string& path) {
        std::vector<std::string> parts = split(path);
        remove_recursive(root, parts, 0);
    }

    // 列出目录内容
    // path = "/" → ["sub", "hello.txt"]
    // path = "/sub" → ["a.txt", "b"]
    std::vector<std::string> list_dir(const std::string& path) {
        Node* node = find_node(path);
        if (!node) return {};

        std::vector<std::string> result;
        for (auto& kv : node->children) {
            result.push_back(kv.first);
        }
        return result;
    }

    // 判断路径是否存在
    bool exists(const std::string& path) {
        Node* node = find_node(path);
        return node && node->is_file;
    }

private:
    Node* root;

    // 释放节点
    void free_node(Node* n) {
        for (auto& kv : n->children) {
            free_node(kv.second);
        }
        delete n;
    }

    // 分割路径 "/a/b/c.txt" → ["a","b","c.txt"]
    std::vector<std::string> split(const std::string& path) {
        std::vector<std::string> parts;
        std::stringstream ss(path);
        std::string item;

        while (std::getline(ss, item, '/')) {
            if (!item.empty()) parts.push_back(item);
        }
        return parts;
    }

    // 找到路径对应的节点
    Node* find_node(const std::string& path) {
        std::vector<std::string> parts = split(path);
        Node* cur = root;

        for (auto& p : parts) {
            if (!cur->children.count(p)) return nullptr;
            cur = cur->children[p];
        }
        return cur;
    }

    // 递归删除
    bool remove_recursive(Node* node, const std::vector<std::string>& parts, size_t idx) {
        if (idx == parts.size()) {
            node->is_file = false;
            return node->children.empty();
        }

        const std::string& key = parts[idx];
        if (!node->children.count(key)) return false;

        Node* child = node->children[key];
        bool should_delete = remove_recursive(child, parts, idx + 1);

        if (should_delete) {
            delete child;
            node->children.erase(key);
        }

        return (!node->is_file && node->children.empty());
    }
};

#endif
