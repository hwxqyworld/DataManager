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
            uint8_t aa = (uint8_t)a;
            uint8_t bb = (uint8_t)b;
            while (bb) {
                if (bb & 1U) x ^= aa;
                bool hi = (aa & 0x80U) != 0;
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
    if (a == 0) return 0;
    for (int i = 1; i < 256; i++) {
        if (gf_mul_table[a][i] == 1) return (uint8_t)i;
    }
    return 0;
}

// 生成 Vandermonde 矩阵
void RSCoder::build_matrix(int k, int m,
                           std::vector<std::vector<uint8_t>> &matrix)
{
    matrix.assign(k + m, std::vector<uint8_t>(k));

    for (int row = 0; row < k + m; row++) {
        uint8_t x = (uint8_t)(row + 1);
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
    if (k <= 0 || m <= 0) {
        fprintf(stderr, "RSCoder::encode: k,m 必须 > 0\n");
        return false;
    }

    // 每个数据块长度（向上取整）
    size_t chunk_size = (data.size() + (size_t)k - 1) / (size_t)k;

    // 填充到 k * chunk_size
    std::string padded = data;
    padded.resize(chunk_size * (size_t)k, 0);

    // 输出 k+m 个 chunk，每个 chunk 长度相同
    out_chunks.assign(k + m, std::string(chunk_size, 0));

    // 生成编码矩阵（k+m 行，k 列）
    std::vector<std::vector<uint8_t>> mat;
    build_matrix(k, m, mat);

    // 对每个块内字节位置编码
    for (int row = 0; row < k + m; row++) {
        for (size_t b = 0; b < chunk_size; b++) {
            uint8_t acc = 0;
            for (int col = 0; col < k; col++) {
                uint8_t v = (uint8_t)padded[(size_t)col * chunk_size + b];
                acc ^= gf_mul(mat[row][col], v);
            }
            out_chunks[row][b] = (char)acc;
        }
    }

    // 在 chunk0 的头部写入原始长度（8 字节）
    uint64_t orig_size = (uint64_t)data.size();
    std::string header(reinterpret_cast<const char*>(&orig_size),
                       sizeof(orig_size));
    out_chunks[0].insert(0, header);

    return true;
}

// 解码：从最多 k 个有效 chunk 恢复原始数据
bool RSCoder::decode(const std::vector<std::string> &chunks,
                     int k, int m,
                     std::string &out_data)
{
    if ((int)chunks.size() != k + m) {
        fprintf(stderr, "RSCoder::decode: chunks.size() 必须是 k+m\n");
        return false;
    }

    // chunk0 里应该有长度头部，优先从这里取
    uint64_t orig_size = 0;
    if (chunks[0].size() >= sizeof(orig_size)) {
        std::memcpy(&orig_size, chunks[0].data(), sizeof(orig_size));
    } else {
        fprintf(stderr, "RSCoder::decode: chunk0 长度不足以包含长度头\n");
        return false;
    }

    // 收集有效 chunk 的索引（包括数据块和校验块），最多 k 个
    std::vector<int> valid;
    valid.reserve(k);
    for (int i = 0; i < k + m; i++) {
        if (!chunks[i].empty()) {
            valid.push_back(i);
            if ((int)valid.size() == k) break;
        }
    }

    if ((int)valid.size() < k) {
        fprintf(stderr, "RSCoder: 有效 chunk 不足 k，无法恢复\n");
        return false;
    }

    // 每个 chunk 的有效负载长度：
    // 对于包含长度头的 chunk0，要扣掉 8 字节
    size_t chunk_size = 0;
    {
        // 找到第一个有效 chunk 的数据长度
        int idx0 = valid[0];
        size_t len0 = chunks[idx0].size();

        if (idx0 == 0) {
            if (len0 < sizeof(orig_size)) {
                fprintf(stderr, "RSCoder::decode: chunk0 长度不足\n");
                return false;
            }
            chunk_size = len0 - sizeof(orig_size);
        } else {
            chunk_size = len0;
        }
    }

    // 构造 k×k 的子矩阵（取 valid 对应的行）
    std::vector<std::vector<uint8_t>> mat(k, std::vector<uint8_t>(k));
    for (int r = 0; r < k; r++) {
        uint8_t x = (uint8_t)(valid[r] + 1);
        uint8_t v = 1;
        for (int c = 0; c < k; c++) {
            mat[r][c] = v;
            v = gf_mul(v, x);
        }
    }

    // 解出的 k 个数据块（总长度 k * chunk_size）
    out_data.assign(chunk_size * (size_t)k, 0);

    // 对每个字节位置做一次求解
    for (size_t b = 0; b < chunk_size; b++) {
        // 构造等式右侧向量 vec
        std::vector<uint8_t> vec(k);
        for (int r = 0; r < k; r++) {
            int idx = valid[r];
            const std::string &chunk = chunks[idx];

            size_t offset;
            if (idx == 0) {
                // 跳过长度头
                if (chunk.size() < sizeof(orig_size) + b + 1) {
                    fprintf(stderr, "RSCoder::decode: chunk0 数据长度不足\n");
                    return false;
                }
                offset = sizeof(orig_size) + b;
            } else {
                if (chunk.size() < b + 1) {
                    fprintf(stderr, "RSCoder::decode: chunk %d 数据长度不足\n", idx);
                    return false;
                }
                offset = b;
            }

            vec[r] = (uint8_t)chunk[offset];
        }

        // 求解线性方程组
        std::vector<uint8_t> sol(k);
        if (!solve_matrix(mat, vec, sol)) {
            fprintf(stderr, "RSCoder: 高斯消元失败\n");
            return false;
        }

        // 把解写回到 out_data 中：第 i 个数据块的第 b 个字节
        for (int i = 0; i < k; i++) {
            out_data[(size_t)i * chunk_size + b] = (char)sol[i];
        }
    }

    // 按原始长度截断（去掉 padding）
    if ((uint64_t)out_data.size() < orig_size) {
        fprintf(stderr, "RSCoder::decode: 解码结果长度不足 orig_size\n");
        return false;
    }
    out_data.resize((size_t)orig_size);

    return true;
}

// 高斯消元（GF(256)）
bool RSCoder::solve_matrix(std::vector<std::vector<uint8_t>> &mat,
                           std::vector<uint8_t> &vec,
                           std::vector<uint8_t> &sol)
{
    int n = (int)mat.size();

    // 前向消元
    for (int i = 0; i < n; i++) {
        if (mat[i][i] == 0) {
            // 简单处理：不做行交换，直接失败
            return false;
        }

        uint8_t inv = gf_inv(mat[i][i]);
        if (inv == 0) return false;

        // 归一化
        for (int j = i; j < n; j++) {
            mat[i][j] = gf_mul(mat[i][j], inv);
        }
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
    sol.assign(n, 0);
    for (int i = n - 1; i >= 0; i--) {
        uint8_t acc = vec[i];
        for (int j = i + 1; j < n; j++) {
            acc ^= gf_mul(mat[i][j], sol[j]);
        }
        sol[i] = acc;
    }

    return true;
}
