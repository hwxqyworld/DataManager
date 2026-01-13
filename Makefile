# Makefile for cloudraidfs - Production-grade FUSE RAID filesystem
# 自动收集所有 .cpp，支持 glibc/musl/clang/debug/release

# ===========================
# 编译器与标志
# ===========================
CXX ?= g++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -O2 -g
LDFLAGS ?= $(shell pkg-config --libs fuse3 2>/dev/null || echo "-lfuse3") -lcurl

# ===========================
# 源码与目标
# ===========================
TARGET = cloudraidfs

# 自动收集所有 cpp 文件
SRC := $(wildcard *.cpp)
OBJ := $(SRC:.cpp=.o)

# ===========================
# 默认构建
# ===========================
all: $(TARGET)

$(TARGET): $(OBJ)
    $(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp
    $(CXX) $(CXXFLAGS) -c $< -o $@

# ===========================
# 清理
# ===========================
clean:
    rm -f $(OBJ) $(TARGET)

# ===========================
# 安装 / 卸载
# ===========================
install: $(TARGET)
    install -m 755 $(TARGET) /usr/local/bin/

uninstall:
    rm -f /usr/local/bin/$(TARGET)

# ===========================
# 构建模式
# ===========================
debug: CXXFLAGS = -std=c++17 -Wall -Wextra -O0 -g -DDEBUG
debug: $(TARGET)

release: CXXFLAGS = -std=c++17 -Wall -Wextra -O3 -DNDEBUG
release: $(TARGET)

clang: CXX = clang++
clang: $(TARGET)

musl: CXX = musl-g++
musl: LDFLAGS = -static $(shell pkg-config --libs fuse3 2>/dev/null || echo "-lfuse3")
musl: $(TARGET)

# ===========================
# 依赖检查
# ===========================
check-deps:
    @echo "检查 FUSE3..."
    @pkg-config --exists fuse3 && echo "✓ FUSE3 已安装" || echo "✗ 未找到 FUSE3"
    @echo "检查编译器..."
    @$(CXX) --version | head -1

# ===========================
# 帮助
# ===========================
help:
    @echo "可用目标:"
    @echo "  all        - 默认构建"
    @echo "  clean      - 清理"
    @echo "  install    - 安装到 /usr/local/bin"
    @echo "  uninstall  - 卸载"
    @echo "  debug      - 调试构建"
    @echo "  release    - 发布构建"
    @echo "  clang      - 使用 clang 编译"
    @echo "  musl       - 使用 musl 静态编译"
    @echo "  check-deps - 检查依赖"
    @echo "  help       - 显示帮助"
    @echo ""
    @echo "环境变量:"
    @echo "  CXX        - C++ 编译器"
    @echo "  CXXFLAGS   - 编译器标志"
    @echo "  LDFLAGS    - 链接器标志"
    @echo ""
    @echo "示例:"
    @echo "  make release"
    @echo "  make CXX=clang++"
    @echo "  make musl"

.PHONY: all clean install uninstall debug release clang musl check-deps help
