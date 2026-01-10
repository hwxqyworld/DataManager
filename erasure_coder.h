#ifndef ERASURE_CODER_H
#define ERASURE_CODER_H

#include <vector>
#include <string>
#include <cstdint>

// 通用 (k+m) 纠删码接口
// 支持 m = 1, 2, 3（甚至更多）
// 可用于 RAID5/6/7/8 等任意 RAID(k+m)

class ErasureCoder {
public:
    virtual ~ErasureCoder() = default;

    // 编码：输入一个完整条带（例如 4MB）
    // 输出 k+m 个 chunk，每个 chunk 大小相同
    virtual bool encode(const std::string &data,
                        int k, int m,
                        std::vector<std::string> &out_chunks) = 0;

    // 解码：输入 k 个有效 chunk（顺序与原始位置对应）
    // 缺失的 chunk 用空字符串表示
    // 输出恢复后的完整条带
    virtual bool decode(const std::vector<std::string> &chunks,
                        int k, int m,
                        std::string &out_data) = 0;
};

#endif // ERASURE_CODER_H
