#!/bin/bash
# build.sh - CloudRaidFs build script (FUSE + vcpkg + minio-cpp)

set -e

# -----------------------------
# 颜色
# -----------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== CloudRaidFs Build System ===${NC}"

# -----------------------------
# 版本号（自动从 git 获取）
# -----------------------------
if git describe --tags --dirty --always >/dev/null 2>&1; then
    VERSION=$(git describe --tags --dirty --always)
else
    VERSION="v0.0.0-unknown"
fi

echo -e "${YELLOW}Version: ${VERSION}${NC}"

# -----------------------------
# 参数解析
# -----------------------------
MODE="${1:-release}"
echo -e "${YELLOW}Build mode: ${MODE}${NC}"

# -----------------------------
# 检查编译器
# -----------------------------
if command -v g++ >/dev/null 2>&1; then
    CXX="g++"
elif command -v clang++ >/dev/null 2>&1; then
    CXX="clang++"
else
    echo -e "${RED}错误: 未找到 g++ 或 clang++${NC}"
    exit 1
fi

# -----------------------------
# 检查 FUSE3
# -----------------------------
if pkg-config --exists fuse3; then
    FUSE_CFLAGS=$(pkg-config --cflags fuse3)
    FUSE_LIBS=$(pkg-config --libs fuse3)
else
    echo -e "${RED}错误: 未找到 FUSE3 开发包${NC}"
    echo "Ubuntu: sudo apt install libfuse3-dev"
    exit 1
fi

# -----------------------------
# vcpkg 自动检测
# -----------------------------
VCPKG_ROOT="$HOME/vcpkg/installed/x64-linux"

if [ -d "$VCPKG_ROOT" ]; then
    echo -e "${GREEN}检测到 vcpkg 动态库: $VCPKG_ROOT${NC}"
    VCPKG_INC="-I$VCPKG_ROOT/include"
    VCPKG_LIB="-L$VCPKG_ROOT/lib"
else
    echo -e "${RED}未检测到 vcpkg，请先执行:${NC}"
    echo "  git clone https://github.com/microsoft/vcpkg ~/vcpkg"
    echo "  ~/vcpkg/bootstrap-vcpkg.sh"
    echo "  ~/vcpkg/vcpkg install minio-cpp"
    exit 1
fi

# -----------------------------
# 源文件列表
# -----------------------------
SRC=$(ls *.cpp)

# -----------------------------
# 输出文件
# -----------------------------
OUT="cloudraidfs"

# -----------------------------
# 构建模式
# -----------------------------
case "$MODE" in
    debug)
        CXXFLAGS="-g -O0 -Wall -Wextra -DDEBUG"
        ;;

    release)
        CXXFLAGS="-O3 -DNDEBUG"
        ;;

    clang)
        CXX="clang++"
        CXXFLAGS="-O3 -DNDEBUG"
        ;;

    musl|static)
        if ! command -v musl-g++ >/dev/null 2>&1; then
            echo -e "${RED}错误: musl-g++ 未找到${NC}"
            echo "Ubuntu: sudo apt install musl-tools"
            exit 1
        fi
        CXX="musl-g++"
        CXXFLAGS="-O3 -static -DNDEBUG"
        VCPKG_ROOT="$VCPKG_STATIC"
        VCPKG_INC="-I$VCPKG_STATIC/include"
        VCPKG_LIB="-L$VCPKG_STATIC/lib"
        ;;

    clean)
        echo -e "${GREEN}清理构建文件...${NC}"
        rm -f "$OUT"
        exit 0
        ;;

    *)
        echo -e "${RED}未知构建模式: $MODE${NC}"
        echo "可用模式: debug / release / musl / clang / clean"
        exit 1
        ;;
esac

# -----------------------------
# 执行构建
# -----------------------------
echo -e "${GREEN}开始构建...${NC}"

$CXX $CXXFLAGS \
    $FUSE_CFLAGS \
    $VCPKG_INC \
    -D_DM_VERSION="\"${VERSION}\"" \
    $SRC \
    -o "$OUT" \
    $FUSE_LIBS \
    $VCPKG_LIB \
    -lminiocpp \
    -lcurl -linih -lcurlpp -lpugixml -lINIReader \
    -lneon \
    -lssl \
    -lcrypto \
    -lz \
    -lpthread \
    -std=c++20

echo -e "${GREEN}✓ 构建成功${NC}"
echo "输出文件: ./${OUT}"
echo ""
echo -e "${YELLOW}运行示例:${NC}"
echo "  sudo ./${OUT} config.yml"
