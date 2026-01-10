#include "rs_coder.h"
#include <cstring>
#include <cstdio>

// GF(256) 乘法表（可选：你可以换成预计算表）
static uint8_t gf_mul_table[256][256];
static bool gf_init = false;

static void gf_init_table() {
    if (gf_init) return;
    gf_init = true;

    for (int a = 0; a < 256; a++) {
        for (int b = 0; b < 256; b++) {
            uint8_t x = 0;
            uint8_t aa = a;
            uint8_t bb = b;
            while (bb) {
                if (bb & 1) x ^= aa;
                bool hi = aa & 0x80;
                aa <<= 1;
                if (hi) aa ^= 0x1D; // x^8 + x^4 + x^3 + x^2 + 1
                bb >>= 1;
            }
            gf_mul_table[a][b] = x;
        }
    }
}

RSCoder::RSCoder() {
    gf_init_table();
}

uint8_t RSCoder::gf_mul(uint8_t a, uint8_t b) const {
    return gf_mul_table[a][b];
}

uint8_t RSCoder::gf_inv(uint8_t a) const {
    for (int i = 1; i < 256; i++) {
        if (gf_mul_table[a][i] == 1) return i;
    }
    return 0;
}

// 生成 Vandermonde 矩阵
void RSCoder::build_matrix(int k, int m,
                           std::vector<std::vector<uint8_t>> &matrix)
{
    matrix.resize(k + m, std::vector<uint8_t>(k));

    for (int row = 0; row < k + m; row++) {
        uint8_t x = row + 1;
        uint8_t v = 1;
        for (int col = 0; col < k; col++) {
            matrix[row][col] = v;
            v = gf_mul(v, x);
        }
    }
}

// 编码：data → k+m 个 chunk
bool RSCoder::encode(const std::string &data,
                     int k, int m,
                     std::vector<std::string> &out_chunks)
{
    size_t chunk_size = (data.size() + k - 1) / k;

    // 填充到等长
    std::string padded = data;
    padded.resize(chunk_size * k, 0);

    // 输出 k+m 个 chunk
    out_chunks.assign(k + m, std::string(chunk_size, 0));

    // 生成编码矩阵
    std::vector<std::vector<uint8_t>> mat;
    build_matrix(k, m, mat);

    // 对每个 chunk 位置编码
    for (int row = 0; row < k + m; row++) {
        for (size_t b = 0; b < chunk_size; b++) {
            uint8_t acc = 0;
            for (int col = 0; col < k; col++) {
                uint8_t v = padded[col * chunk_size + b];
                acc ^= gf_mul(mat[row][col], v);
            }
            out_chunks[row][b] = acc;
        }
    }

    return true;
}

// 解码：从 k 个有效 chunk 恢复原始数据
bool RSCoder::decode(const std::vector<std::string> &chunks,
                     int k, int m,
                     std::string &out_data)
{
    size_t chunk_size = chunks[0].size();

    // 收集有效 chunk
    std::vector<int> valid;
    for (int i = 0; i < k + m; i++) {
        if (!chunks[i].empty()) valid.push_back(i);
        if ((int)valid.size() == k) break;
    }

    if ((int)valid.size() < k) {
        fprintf(stderr, "RSCoder: 有效 chunk 不足 k，无法恢复\n");
        return false;
    }

    // 构造 k×k 的子矩阵
    std::vector<std::vector<uint8_t>> mat(k, std::vector<uint8_t>(k));
    for (int r = 0; r < k; r++) {
        uint8_t x = valid[r] + 1;
        uint8_t v = 1;
        for (int c = 0; c < k; c++) {
            mat[r][c] = v;
            v = gf_mul(v, x);
        }
    }

    // 对每个字节位置求解
    out_data.resize(chunk_size * k);

    for (size_t b = 0; b < chunk_size; b++) {
        std::vector<uint8_t> vec(k);
        for (int r = 0; r < k; r++) {
            vec[r] = chunks[valid[r]][b];
        }

        std::vector<uint8_t> sol(k);
        if (!solve_matrix(mat, vec, sol)) {
            fprintf(stderr, "RSCoder: 高斯消元失败\n");
            return false;
        }

        for (int i = 0; i < k; i++) {
            out_data[i * chunk_size + b] = sol[i];
        }
    }

    return true;
}

// 高斯消元（GF(256)）
bool RSCoder::solve_matrix(std::vector<std::vector<uint8_t>> &mat,
                           std::vector<uint8_t> &vec,
                           std::vector<uint8_t> &sol)
{
    int n = mat.size();

    // 前向消元
    for (int i = 0; i < n; i++) {
        if (mat[i][i] == 0) return false;

        uint8_t inv = gf_inv(mat[i][i]);

        // 归一化
        for (int j = i; j < n; j++) mat[i][j] = gf_mul(mat[i][j], inv);
        vec[i] = gf_mul(vec[i], inv);

        // 消元
        for (int r = i + 1; r < n; r++) {
            uint8_t f = mat[r][i];
            if (f == 0) continue;

            for (int c = i; c < n; c++) {
                mat[r][c] ^= gf_mul(f, mat[i][c]);
            }
            vec[r] ^= gf_mul(f, vec[i]);
        }
    }

    // 回代
    sol.resize(n);
    for (int i = n - 1; i >= 0; i--) {
        uint8_t acc = vec[i];
        for (int j = i + 1; j < n; j++) {
            acc ^= gf_mul(mat[i][j], sol[j]);
        }
        sol[i] = acc;
    }

    return true;
}
