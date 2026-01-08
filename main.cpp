// C++ FUSE 文件系统
// 编译: make
// 运行: ./datamanager /backend /mnt/test

#include <fuse3/fuse.h>
#include <cstdio>
#include <cstring>
#include <cerrno>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <string>
#include <iostream>

// 全局变量：后端目录路径
static std::string backend_path;

// 将 FUSE 路径转换为后端真实路径
static std::string to_backend_path(const char *path) {
    std::string full_path = backend_path + path;
    return full_path;
}

// 获取文件属性
static int getattr(const char *path, struct stat *stbuf, struct fuse_file_info *fi) {
    (void)fi; // 未使用参数
    std::string real_path = to_backend_path(path);
    
    int res = lstat(real_path.c_str(), stbuf);
    if (res == -1) {
        return -errno;
    }
    return 0;
}

// 读取目录内容
static int readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                   off_t offset, struct fuse_file_info *fi,
                   unsigned int flags) {
    (void)offset;
    (void)fi;
    (void)flags;

    std::string real_path = to_backend_path(path);
    DIR *dp = opendir(real_path.c_str());
    if (dp == NULL) {
        return -errno;
    }

    struct dirent *de;
    while ((de = readdir(dp)) != NULL) {
        struct stat st;
        memset(&st, 0, sizeof(st));
        st.st_ino = de->d_ino;
        st.st_mode = de->d_type << 12;

        if (filler(buf, de->d_name, &st, 0, 0)) {
            break;
        }
    }

    closedir(dp);
    return 0;
}

// 打开文件
static int open(const char *path, struct fuse_file_info *fi) {
    std::string real_path = to_backend_path(path);
    
    int fd = open(real_path.c_str(), fi->flags);
    if (fd == -1) {
        return -errno;
    }
    
    fi->fh = fd;
    return 0;
}

// 读取文件内容
static int read(const char *path, char *buf, size_t size, off_t offset,
                      struct fuse_file_info *fi) {
    (void)path; // 使用文件句柄而不是路径
    
    int fd = (int)fi->fh;
    int res = pread(fd, buf, size, offset);
    if (res == -1) {
        return -errno;
    }
    return res;
}

// 写入文件内容
static int write(const char *path, const char *buf, size_t size,
                       off_t offset, struct fuse_file_info *fi) {
    (void)path; // 使用文件句柄而不是路径
    
    int fd = (int)fi->fh;
    int res = pwrite(fd, buf, size, offset);
    if (res == -1) {
        return -errno;
    }
    return res;
}

// 关闭文件
static int release(const char *path, struct fuse_file_info *fi) {
    (void)path; // 使用文件句柄而不是路径
    
    int fd = (int)fi->fh;
    close(fd);
    return 0;
}

// 创建文件
static int create(const char *path, mode_t mode, struct fuse_file_info *fi) {
    std::string real_path = to_backend_path(path);
    
    int fd = creat(real_path.c_str(), mode);
    if (fd == -1) {
        return -errno;
    }
    
    fi->fh = fd;
    return 0;
}

// 删除文件
static int unlink(const char *path) {
    std::string real_path = to_backend_path(path);
    
    int res = unlink(real_path.c_str());
    if (res == -1) {
        return -errno;
    }
    return 0;
}

// 创建目录
static int mkdir(const char *path, mode_t mode) {
    std::string real_path = to_backend_path(path);
    
    int res = mkdir(real_path.c_str(), mode);
    if (res == -1) {
        return -errno;
    }
    return 0;
}

// 删除目录
static int rmdir(const char *path) {
    std::string real_path = to_backend_path(path);
    
    int res = rmdir(real_path.c_str());
    if (res == -1) {
        return -errno;
    }
    return 0;
}

// 重命名文件/目录
static int rename(const char *from, const char *to, unsigned int flags) {
    (void)flags; // 简单实现，忽略标志
    
    std::string real_from = to_backend_path(from);
    std::string real_to = to_backend_path(to);
    
    int res = rename(real_from.c_str(), real_to.c_str());
    if (res == -1) {
        return -errno;
    }
    return 0;
}

// 修改文件权限
static int chmod(const char *path, mode_t mode, struct fuse_file_info *fi) {
    (void)fi; // 未使用参数
    
    std::string real_path = to_backend_path(path);
    
    int res = chmod(real_path.c_str(), mode);
    if (res == -1) {
        return -errno;
    }
    return 0;
}

// 修改文件所有者
static int chown(const char *path, uid_t uid, gid_t gid, struct fuse_file_info *fi) {
    (void)fi; // 未使用参数
    
    std::string real_path = to_backend_path(path);
    
    int res = lchown(real_path.c_str(), uid, gid);
    if (res == -1) {
        return -errno;
    }
    return 0;
}

// 截断文件
static int truncate(const char *path, off_t size, struct fuse_file_info *fi) {
    std::string real_path = to_backend_path(path);
    
    int res;
    if (fi != NULL) {
        int fd = (int)fi->fh;
        res = ftruncate(fd, size);
    } else {
        res = truncate(real_path.c_str(), size);
    }
    
    if (res == -1) {
        return -errno;
    }
    return 0;
}

// FUSE 操作结构体
static struct fuse_operations simple_oper = {
    .getattr    = getattr,
    .readdir    = readdir,
    .open       = open,
    .read       = read,
    .write      = write,
    .release    = release,
    .create     = create,
    .unlink     = unlink,
    .mkdir      = mkdir,
    .rmdir      = rmdir,
    .rename     = rename,
    .chmod      = chmod,
    .chown      = chown,
    .truncate   = truncate,
};

int main(int argc, char *argv[]) {
    // 检查参数
    if (argc < 3) {
        std::cerr << "用法: " << argv[0] << " <后端目录> <挂载点> [FUSE 选项]\n";
        std::cerr << "示例: " << argv[0] << " /backend /mnt/test -f -s\n";
        return 1;
    }

    backend_path = argv[1];
    if (backend_path.back() != '/') {
        backend_path += '/';
    }

    struct stat st;
    if (stat(backend_path.c_str(), &st) == -1) {
        std::cerr << "错误: 后端目录 '" << backend_path << "' 不存在\n";
        return 1;
    }

    // fuse3 推荐直接传递 argv/argc
    std::cout << "启动简单 FUSE 文件系统\n";
    std::cout << "后端目录: " << backend_path << "\n";
    std::cout << "挂载点: " << argv[2] << "\n";

    // 跳过 argv[1] (后端目录)，构造新的参数数组
    char *fuse_argv[argc];
    fuse_argv[0] = argv[0];
    fuse_argv[1] = argv[2];
    for (int i = 3; i < argc; i++) {
        fuse_argv[i-1] = argv[i];
    }
    int fuse_argc = argc - 1;

    return fuse_main(fuse_argc, fuse_argv, &simple_oper, NULL);
}