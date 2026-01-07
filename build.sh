#!/bin/bash
# build.sh - 简单 FUSE 文件系统构建脚本

set -e  # 遇到错误时退出

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== 构建脚本 ===${NC}"

# 检查是否在正确的目录
if [ ! -f "main.cpp" ] || [ ! -f "Makefile" ]; then
    echo -e "${RED}错误: 请在包含 main.cpp 和 Makefile 的目录中运行此脚本${NC}"
    exit 1
fi

# 检查依赖
echo -e "${YELLOW}检查依赖...${NC}"

# 检查编译器
if command -v g++ &> /dev/null; then
    echo "✓ 找到 g++"
    CXX="g++"
elif command -v clang++ &> /dev/null; then
    echo "✓ 找到 clang++"
    CXX="clang++"
else
    echo -e "${RED}错误: 未找到 C++ 编译器${NC}"
    echo "请安装 g++ 或 clang++"
    exit 1
fi

# 检查 FUSE3
if pkg-config --exists fuse3; then
    echo "✓ 找到 FUSE3 库"
else
    echo -e "${RED}警告: 未找到 FUSE3 开发文件${NC}"
    echo "在 Ubuntu/Debian 上: sudo apt install libfuse3-dev"
    echo "在 Fedora/RHEL 上: sudo dnf install fuse3-devel"
    echo "在 Arch 上: sudo pacman -S fuse3"
    echo "继续构建，但可能会失败..."
fi

# 显示构建选项
echo ""
echo -e "${YELLOW}构建选项:${NC}"
echo "1. 标准构建 (默认)"
echo "2. 调试构建"
echo "3. 发布构建"
echo "4. 使用 clang"
echo "5. 使用 musl (静态)"
echo "6. 清理"
echo "7. 检查依赖"
echo "8. 退出"

read -p "选择选项 [1-8]: " choice

case $choice in
    1)
        echo -e "${GREEN}执行标准构建...${NC}"
        make
        ;;
    2)
        echo -e "${GREEN}执行调试构建...${NC}"
        make debug
        ;;
    3)
        echo -e "${GREEN}执行发布构建...${NC}"
        make release
        ;;
    4)
        echo -e "${GREEN}使用 clang 构建...${NC}"
        make clang
        ;;
    5)
        echo -e "${GREEN}使用 musl 静态构建...${NC}"
        if ! command -v musl-g++ &> /dev/null; then
            echo -e "${RED}错误: musl-g++ 未找到${NC}"
            echo "请先安装 musl:"
            echo "在 Ubuntu/Debian 上: sudo apt install musl-tools"
            exit 1
        fi
        make musl
        ;;
    6)
        echo -e "${GREEN}清理构建文件...${NC}"
        make clean
        ;;
    7)
        echo -e "${GREEN}检查依赖...${NC}"
        make check-deps
        ;;
    8)
        echo "退出"
        exit 0
        ;;
    *)
        echo -e "${YELLOW}无效选择，使用标准构建${NC}"
        make
        ;;
esac

# 检查构建结果
if [ -f "datamanager" ]; then
    echo ""
    echo -e "${GREEN}✓ 构建成功！${NC}"
    echo "生成的可执行文件: ./datamanager"
    echo ""
    echo -e "${YELLOW}使用方法:${NC}"
    echo "1. 创建后端目录: mkdir -p /tmp/backend"
    echo "2. 创建挂载点: mkdir -p /tmp/mnt"
    echo "3. 在前台运行: ./datamanager /tmp/backend /tmp/mnt -f"
    echo "4. 在后台运行: ./datamanager /tmp/backend /tmp/mnt &"
    echo ""
    echo -e "${YELLOW}测试:${NC}"
    echo "在另一个终端中:"
    echo "  ls /tmp/mnt"
    echo "  echo 'test' > /tmp/mnt/test.txt"
    echo "  cat /tmp/mnt/test.txt"
    echo ""
    echo -e "${YELLOW}卸载:${NC}"
    echo "  fusermount3 -u /tmp/mnt"
else
    if [ "$choice" != "6" ]; then
        echo -e "${RED}✗ 构建失败${NC}"
        exit 1
    fi
fi

echo -e "${GREEN}完成！${NC}"