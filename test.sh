#!/bin/bash
set -e

echo "=== 创建 5 个本地 RAID 目录 ==="
BASE=/tmp/raid_test
DIRS=("$BASE/raid0" "$BASE/raid1" "$BASE/raid2" "$BASE/raid3" "$BASE/raid4")

for d in "${DIRS[@]}"; do
    mkdir -p "$d"
done

echo "目录结构："
ls -l "$BASE"

echo
echo "=== 编译 RAID 测试程序 ==="

g++ -std=c++17 -O2 \
    raid_test.cpp \
    local_chunk_store.cpp \
    raid_chunk_store.cpp \
    rs_coder.cpp \
    -o raid_test

echo "编译完成：./raid_test"

echo
echo "=== 第一次运行（写入 + 读取 + 校验） ==="
./raid_test "${DIRS[@]}"

echo
echo "=== 模拟坏盘：删除 raid2 的 stripe 1 ==="
rm -rf "$BASE/raid2/stripes/00000001"

echo "删除后目录结构："
tree "$BASE" || ls -R "$BASE"

echo
echo "=== 第二次运行（读取 + 自动修复） ==="
./raid_test "${DIRS[@]}"

echo
echo "=== 检查修复结果 ==="
tree "$BASE" || ls -R "$BASE"

echo
echo "=== 测试完成 ==="
