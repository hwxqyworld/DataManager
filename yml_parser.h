#ifndef SIMPLE_YML_PARSER_H
#define SIMPLE_YML_PARSER_H

#include <string>
#include <vector>
#include <unordered_map>

// 一个非常简单的 YAML 解析器
// 支持：
// key: value
// key:
//   - item
//   - item
// key:
//   subkey: value
//
// 解析结果存储在 YmlNode 中

class YmlNode {
public:
    std::string value;                       // key: value
    std::unordered_map<std::string, YmlNode> map; // key: { ... }
    std::vector<std::string> list;           // key: [item1, item2]

    bool is_value() const { return !value.empty(); }
    bool is_list() const { return !list.empty(); }
    bool is_map() const { return !map.empty(); }
};

class YmlParser {
public:
    bool load_file(const std::string &path);
    const YmlNode &root() const { return root_node; }

private:
    YmlNode root_node;

    static std::string trim(const std::string &s);
    static bool starts_with(const std::string &s, const std::string &p);

    void parse_lines(const std::vector<std::string> &lines);
};

#endif
