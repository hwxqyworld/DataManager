#include "yml_parser.h"
#include <fstream>
#include <sstream>
#include <iostream>

std::string YmlParser::trim(const std::string &s)
{
    size_t b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return "";
    size_t e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
}

bool YmlParser::starts_with(const std::string &s, const std::string &p)
{
    return s.size() >= p.size() && s.compare(0, p.size(), p) == 0;
}

bool YmlParser::load_file(const std::string &path)
{
    std::ifstream fin(path);
    if (!fin.is_open())
        return false;

    std::vector<std::string> lines;
    std::string line;

    while (std::getline(fin, line)) {
        lines.push_back(line);
    }

    parse_lines(lines);
    return true;
}

void YmlParser::parse_lines(const std::vector<std::string> &lines)
{
    root_node = YmlNode();

    std::vector<std::pair<int, YmlNode *>> stack;
    stack.push_back({0, &root_node});

    for (auto raw : lines) {
        std::string line = trim(raw);
        if (line.empty()) continue;
        if (line[0] == '#') continue;

        int indent = 0;
        for (char c : raw) {
            if (c == ' ') indent++;
            else break;
        }

        while (!stack.empty() && indent < stack.back().first) {
            stack.pop_back();
        }

        YmlNode *parent = stack.back().second;

        // list item: "- value"
        if (starts_with(line, "- ")) {
            std::string item = trim(line.substr(2));
            parent->list.push_back(item);
            continue;
        }

        // key: value
        size_t pos = line.find(':');
        if (pos == std::string::npos)
            continue;

        std::string key = trim(line.substr(0, pos));
        std::string val = trim(line.substr(pos + 1));

        // key:
        if (val.empty()) {
            parent->map[key] = YmlNode();
            stack.push_back({indent + 2, &parent->map[key]});
        } else {
            // key: value
            parent->map[key] = YmlNode();
            parent->map[key].value = val;
        }
    }
}
