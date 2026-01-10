#!/bin/bash
set -e

echo "=== 创建 5 个本地 RAID 目录 ==="
BASE=/tmp/raid_test
MNT=$BASE/mnt
DIRS=("$BASE/raid0" "$BASE/raid1" "$BASE/raid2" "$BASE/raid3" "$BASE/raid4")

mkdir -p "$MNT"
for d in "${DIRS[@]}"; do
    mkdir -p "$d"
done

echo
echo "=== 编译 FUSE RAID 文件系统 ==="

g++ -std=c++17 -O2 \
    main.cpp \
    file_manager.cpp \
    local_chunk_store.cpp \
    raid_chunk_store.cpp \
    rs_coder.cpp \
    -lfuse3 -o cloudraidfs

echo "编译完成：./cloudraidfs"

echo
echo "=== 第一次挂载 ==="
./cloudraidfs "$MNT" "${DIRS[@]}" -f &
PID=$!
sleep 1

echo
echo "=== 写入大文件（跨多个 stripe） ==="
dd if=/dev/urandom of="$MNT/file" bs=1M count=6 status=none
md5sum "$MNT/file"

echo
echo "=== 卸载第一次挂载 ==="
fusermount3 -u "$MNT"
sleep 1

echo
echo "=== 模拟坏盘：删除 raid0 的 stripe 1 ==="
rm -rf "$BASE/raid0/stripes/00000001"

echo
echo "=== 第二次挂载（触发自动修复） ==="
./cloudraidfs "$MNT" "${DIRS[@]}" -f &
PID=$!
sleep 1

echo
echo "=== 读取文件，验证修复 ==="
md5sum "$MNT/file"

echo
echo "=== 卸载第二次挂载 ==="
fusermount3 -u "$MNT"

echo
echo "=== 测试完成 ==="
