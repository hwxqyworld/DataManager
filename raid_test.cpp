#include "local_chunk_store.h"
#include "raid_chunk_store.h"
#include "rs_coder.h"
#include <iostream>
#include <memory>
#include <vector>

int main(int argc, char *argv[])
{
    if (argc != 6) {
        std::cerr << "用法: " << argv[0] << " <dir0> <dir1> <dir2> <dir3> <dir4>\n";
        std::cerr << "示例: " << argv[0]
                  << " /tmp/raid0 /tmp/raid1 /tmp/raid2 /tmp/raid3 /tmp/raid4\n";
        return 1;
    }

    // 1. 构造 5 个本地后端，做 4+1
    std::vector<std::shared_ptr<ChunkStore>> backends;
    for (int i = 1; i <= 5; i++) {
        backends.push_back(std::make_shared<LocalChunkStore>(argv[i]));
    }

    int k = 4;
    int m = 1;

    // 2. 构造 Reed-Solomon 纠删码
    std::shared_ptr<ErasureCoder> coder = std::make_shared<RSCoder>();

    // 3. 构造 RAIDChunkStore
    RAIDChunkStore raid(backends, k, m, coder);

    uint64_t stripe_id = 1;

    // 4. 写入一条测试数据
    std::string data = "Hello RAID 4+1 test. 这是一次跨目录虚拟阵列测试。\n";
    data.append(1024 * 1024, 'X'); // 再填充一点数据，接近 1MB

    std::cout << "写入 stripe_id = " << stripe_id << " 的测试数据..." << std::endl;
    if (!raid.write_chunk(stripe_id, 0, data)) {
        std::cerr << "写入失败\n";
        return 1;
    }
    std::cout << "写入完成。\n";

    // 5. 读取并验证
    std::string out;
    std::cout << "读取 stripe_id = " << stripe_id << " ..." << std::endl;
    if (!raid.read_chunk(stripe_id, 0, out)) {
        std::cerr << "读取失败\n";
        return 1;
    }

    if (out.size() != data.size() || out != data) {
        std::cerr << "数据校验失败：读回内容和写入内容不一致\n";
        return 1;
    }

    std::cout << "数据校验成功，阵列工作正常。\n";
    std::cout << "现在你可以尝试:\n";
    std::cout << "  1) 删除任意一个目录下的 stripes/00000001/\n";
    std::cout << "  2) 再次运行本程序，只执行读取部分，看能否自动修复\n";

    return 0;
}
