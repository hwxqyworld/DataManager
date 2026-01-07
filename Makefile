# Makefile for datamanager - 最小可运行的 C++ FUSE 文件系统
# 支持 glibc 和 musl，允许 CC/LD 覆盖

# 编译器设置
CXX ?= g++
CXXFLAGS ?= -std=c++17 -Wall -Wextra -O2 -g
LDFLAGS ?= -lfuse3

# 目标文件
TARGET = datamanager
SRC = main.cpp
OBJ = $(SRC:.cpp=.o)

# 默认目标
all: $(TARGET)

# 链接可执行文件
$(TARGET): $(OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# 编译源文件
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 清理构建文件
clean:
	rm -f $(OBJ) $(TARGET)

# 安装（可选）
install: $(TARGET)
	install -m 755 $(TARGET) /usr/local/bin/

# 卸载（可选）
uninstall:
	rm -f /usr/local/bin/$(TARGET)

# 使用 musl 编译（如果可用）
musl: CXX = musl-g++
musl: LDFLAGS = -lfuse3 -static
musl: $(TARGET)

# 使用 clang 编译
clang: CXX = clang++
clang: $(TARGET)

# 调试构建
debug: CXXFLAGS = -std=c++17 -Wall -Wextra -O0 -g -DDEBUG
debug: $(TARGET)

# 发布构建
release: CXXFLAGS = -std=c++17 -Wall -Wextra -O3 -DNDEBUG
release: $(TARGET)

# 检查依赖
check-deps:
	@echo "检查 FUSE3 库..."
	@pkg-config --exists fuse3 && echo "FUSE3 已安装" || echo "错误: FUSE3 未安装"
	@echo "检查 C++ 编译器..."
	@$(CXX) --version | head -1

# 显示帮助
help:
	@echo "可用目标:"
	@echo "  all        - 默认构建 (默认)"
	@echo "  clean      - 清理构建文件"
	@echo "  install    - 安装到 /usr/local/bin"
	@echo "  uninstall  - 卸载"
	@echo "  musl       - 使用 musl 静态编译"
	@echo "  clang      - 使用 clang 编译"
	@echo "  debug      - 调试构建"
	@echo "  release    - 发布构建"
	@echo "  check-deps - 检查依赖"
	@echo "  help       - 显示此帮助"
	@echo ""
	@echo "环境变量:"
	@echo "  CXX        - C++ 编译器 (默认: g++)"
	@echo "  CXXFLAGS   - 编译器标志"
	@echo "  LDFLAGS    - 链接器标志"
	@echo ""
	@echo "示例:"
	@echo "  make                      # 默认构建"
	@echo "  make CXX=clang++          # 使用 clang 编译"
	@echo "  make debug                # 调试构建"
	@echo "  make clean && make        # 清理后重新构建"

.PHONY: all clean install uninstall musl clang debug release check-deps help