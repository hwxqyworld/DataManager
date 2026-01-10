#!/bin/bash
set -e

echo "=== 创建 5 个本地 RAID 目录 ==="
BASE=/tmp/raid_test
MONT=$BASE/mnt
DIRS=("$BASE/raid0" "$BASE/raid1" "$BASE/raid2" "$BASE/raid3" "$BASE/raid4")

for d in "${DIRS[@]}"; do
    mkdir -p "$d"
done

echo
echo "=== 编译 RAID 测试程序 ==="

g++ -std=c++17 -O2 \
    main.cpp \
    file_manager.cpp \
    local_chunk_store.cpp \
    raid_chunk_store.cpp \
    rs_coder.cpp \
    -lfuse3 -o cloudraidfs

echo "编译完成：./cloudraidfs"

echo
echo "=== 第一次运行（写入 + 读取 + 校验） ==="
./cloudraidfs "${MONT}" "${DIRS[@]}"
echo "xxxbejdkfkejxjwksdkjsajwjdjdnsd" > "$MONT/file"
cat "$MONT/file"

echo
echo "=== 模拟坏盘：删除 raid0 的 stripe 1 ==="
rm -rf "$BASE/raid0/stripes/00000001"

echo
echo "=== 第二次运行（读取 + 自动修复） ==="
./cloudraidfs "${MONT}" "${DIRS[@]}"

echo
echo "=== 检查修复结果 ==="
cat "$MONT/file"

echo
echo "=== 测试完成 ==="
