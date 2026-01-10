#ifndef RS_CODER_H
#define RS_CODER_H

#include "erasure_coder.h"
#include <vector>
#include <string>
#include <cstdint>

// Reed-Solomon (k+m) 纠删码实现
// 支持 m = 1, 2, 3（甚至更多）
// 基于 GF(256) 的 Vandermonde 矩阵

class RSCoder : public ErasureCoder {
public:
    RSCoder();

    bool encode(const std::string &data,
                int k, int m,
                std::vector<std::string> &out_chunks) override;

    bool decode(const std::vector<std::string> &chunks,
                int k, int m,
                std::string &out_data) override;

private:
    uint8_t gf_mul(uint8_t a, uint8_t b) const;
    uint8_t gf_inv(uint8_t a) const;

    // 生成 Vandermonde 编码矩阵
    void build_matrix(int k, int m,
                      std::vector<std::vector<uint8_t>> &matrix);

    // 高斯消元恢复矩阵
    bool solve_matrix(std::vector<std::vector<uint8_t>> &mat,
                      std::vector<uint8_t> &vec,
                      std::vector<uint8_t> &solution);
};

#endif // RS_CODER_H
