# CloudRaidFS

**CloudRaidFS** 是一个基于 FUSE 的分布式纠删码文件系统，支持将数据分片存储到多个本地或远程后端（如本地磁盘、WebDAV），并通过 Reed-Solomon 纠删码提供数据冗余和自动修复能力。

## ✨ 功能特性

- **纠删码存储**：基于 Reed-Solomon (k+m) 纠删码，支持任意 k 个数据块 + m 个校验块配置
- **多后端支持**：
  - 本地目录存储 (`local`)
  - WebDAV 远程存储 (`webdav`)
- **自动数据修复**：读取时自动检测并修复损坏/丢失的数据块
- **透明挂载**：通过 FUSE 挂载为本地文件系统，应用程序无感知
- **多级缓存**：
  - 文件级缓存：缓存完整小文件，适合频繁读取场景
  - Chunk 级缓存：缓存数据块，适合大文件部分读取场景
- **元数据持久化**：元数据自动保存到后端存储，重启后自动恢复

## 🎯 软件定位

CloudRaidFS 适用于以下场景：

| 场景 | 说明 |
|------|------|
| **个人云存储整合** | 将多个网盘/存储服务整合为一个统一的文件系统 |
| **数据冗余备份** | 利用纠删码实现跨存储的数据冗余，防止单点故障 |
| **分布式存储实验** | 学习和研究纠删码、分布式存储原理 |
| **低成本高可靠存储** | 相比传统 RAID，纠删码提供更高的存储效率 |

### 纠删码说明

- **k=2, m=1**：2 个数据块 + 1 个校验块，可容忍 1 个后端故障，存储效率 66.7%
- **k=4, m=2**：4 个数据块 + 2 个校验块，可容忍 2 个后端故障，存储效率 66.7%
- **k=6, m=3**：6 个数据块 + 3 个校验块，可容忍 3 个后端故障，存储效率 66.7%

## 📦 下载安装

### 系统要求

- Linux 操作系统（需要 FUSE3 支持）
- GCC 7+ 或 Clang 5+（支持 C++17）
- libfuse3-dev
- libneon27-dev（WebDAV 支持）

### 安装依赖

**Debian/Ubuntu:**
```bash
sudo apt update
sudo apt install build-essential libfuse3-dev libneon27-dev pkg-config
```

**Fedora/RHEL:**
```bash
sudo dnf install gcc-c++ fuse3-devel neon-devel pkg-config
```

**Arch Linux:**
```bash
sudo pacman -S base-devel fuse3 neon
```

### 编译安装

```bash
# 克隆项目
git clone https://github.com/your-repo/cloudraidfs.git
cd cloudraidfs

# 编译（默认构建）
./build.sh release

# 安装到系统
sudo cp ./cloudraidfs /usr/local/bin/cloudraidfs
```

### 构建选项

| 命令 | 说明 |
|------|------|
| `./build.sh release` | 默认构建（-O3 优化） |
| `./build.sh debug` | 调试构建（-O0 + DEBUG 宏） |
| `./build.sh clang` | 使用 Clang 编译 |
| `./build.sh musl` | 使用 musl 编译 |
| `./build.sh clean` | 清理构建产物 |

## 🚀 使用示例

### 1. 创建配置文件

创建 `config.yml`：

```yaml
# 挂载点
mountpoint: /mnt/cloudraidfs

# 纠删码参数：2 个数据块 + 1 个校验块
k: 2
m: 1

# 文件缓存配置（可选）
cache:
  max_cache_size: 256    # 最大缓存 256MB
  max_file_size: 32      # 最大缓存单文件 32MB
  cache_ttl: 60          # 缓存过期时间 60 秒

# Chunk 缓存配置（可选）
chunk_cache:
  max_cache_size: 256    # 最大缓存 256MB
  cache_ttl: 60          # 缓存过期时间 60 秒

# 后端存储配置（至少需要 k+m 个后端）
backends:
  backend0:
    type: local
    path: /data/chunk0
  backend1:
    type: local
    path: /data/chunk1
  backend2:
    type: webdav
    url: https://webdav.example.com/cloudraidfs
    username: user
    password: pass
```

### 2. 准备后端存储目录

```bash
# 创建本地存储目录
sudo mkdir -p /data/chunk0 /data/chunk1

# 创建挂载点
sudo mkdir -p /mnt/cloudraidfs
```

### 3. 启动文件系统

```bash
# 前台运行（调试用）
cloudraidfs config.yml -f

# 后台运行
cloudraidfs config.yml

# 带调试输出
cloudraidfs config.yml -f -d
```

### 4. 使用文件系统

```bash
# 写入文件
echo "Hello, CloudRaidFS!" > /mnt/cloudraidfs/hello.txt

# 读取文件
cat /mnt/cloudraidfs/hello.txt

# 创建目录结构
mkdir -p /mnt/cloudraidfs/documents
cp ~/myfile.pdf /mnt/cloudraidfs/documents/

# 查看文件列表
ls -la /mnt/cloudraidfs/
```

### 5. 卸载文件系统

```bash
# 正常卸载
fusermount -u /mnt/cloudraidfs

# 强制卸载
fusermount -uz /mnt/cloudraidfs
```

## 📁 项目结构

```filetree
cloudraidfs/
├── main.cpp                 # FUSE 入口，文件系统操作实现
├── file_manager.cpp/h       # 文件读写管理，条带映射
├── metadata_manager.cpp/h   # 元数据管理，文件索引
├── raid_chunk_store.cpp/h   # RAID 层，纠删码分发与恢复
├── rs_coder.cpp/h           # Reed-Solomon 纠删码实现
├── local_chunk_store.cpp/h  # 本地目录后端
├── webdav_chunk_store.cpp/h # WebDAV 后端
├── file_cache.cpp/h         # 文件级缓存
├── chunk_cache.cpp/h        # Chunk 级缓存
├── path_trie.h              # 路径前缀树（目录结构）
├── yml_parser.cpp/h         # YAML 配置解析器
├── config.example.yml       # 配置文件示例
└── build.sh                 # 构建脚本
```

## ⚙️ 配置参数说明

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `mountpoint` | string | ✅ | 文件系统挂载点路径 |
| `k` | int | ✅ | 数据块数量 |
| `m` | int | ✅ | 校验块数量 |
| `backends` | map | ✅ | 后端存储配置（至少 k+m 个） |
| `cache.max_cache_size` | int | ❌ | 文件缓存大小（MB），默认 256 |
| `cache.max_file_size` | int | ❌ | 最大可缓存文件大小（MB），默认 32 |
| `cache.cache_ttl` | int | ❌ | 缓存过期时间（秒），默认 60 |
| `chunk_cache.max_cache_size` | int | ❌ | Chunk 缓存大小（MB），默认 256 |
| `chunk_cache.cache_ttl` | int | ❌ | Chunk 缓存过期时间（秒），默认 60 |

### 后端类型

**本地存储 (local):**
```yaml
backend0:
  type: local
  path: /path/to/storage
```

**WebDAV 存储 (webdav):**
```yaml
backend1:
  type: webdav
  url: https://webdav.example.com/path
  username: user      # 可选
  password: pass      # 可选
```

## 🔧 故障排除

### 常见问题

**Q: 挂载失败，提示 "fuse: bad mount point"**
```bash
# 确保挂载点存在且为空目录
sudo mkdir -p /mnt/cloudraidfs
sudo umount /mnt/cloudraidfs 2>/dev/null
```

**Q: 权限不足**
```bash
# 将用户添加到 fuse 组
sudo usermod -aG fuse $USER
# 重新登录生效
```

**Q: WebDAV 连接失败**
- 检查 URL 是否正确（注意 https/http）
- 验证用户名密码
- 确保 WebDAV 服务器支持 PUT/GET/DELETE/MKCOL 方法

## 📄 许可证

Apache License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！
