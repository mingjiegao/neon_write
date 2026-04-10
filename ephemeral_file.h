#pragma once
/// ephemeral_file.h — EphemeralFile: 追加写入的临时文件
///
/// 对应 Rust: pageserver/src/virtual_file/owned_buffers_io/write/flush.rs 等
///
/// EphemeralFile 是 InMemoryLayer 的底层存储。它是一个只追加（append-only）的文件。
/// 所有 WAL 记录的 value 都被写入这里，然后通过 IndexEntry 中的 (pos, len) 来定位。
///
/// 关键特性（inmemory_layer.rs:73-77 注释）:
///   "the file backing InMemoryLayer::file is append-only,
///    so it is not necessary to hold a lock on the index while reading or writing from the file.
///    1. It is safe to read and release index before reading from file.
///    2. It is safe to write to file before locking and updating index."
///
/// 简化: 用 std::vector<uint8_t> 模拟文件内容（内存中），不做真正的 I/O。

#include <vector>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

/// 对应 Rust: EphemeralFile
///
/// 追加写入的临时文件。在真正的 Neon 中是磁盘上的文件（通过 VirtualFile），
/// 这里简化为内存中的 byte buffer。
class EphemeralFile {
public:
    EphemeralFile() = default;

    /// 对应 Rust: EphemeralFile::len()
    /// 当前文件大小（字节数）
    uint64_t len() const {
        return static_cast<uint64_t>(buffer_.size());
    }

    /// 对应 Rust: EphemeralFile::write_raw(&raw, ctx)
    /// (inmemory_layer.rs:588)
    ///
    /// 追加写入原始字节。
    /// Rust 原文:
    ///   self.file.write_raw(&raw, ctx).await?;
    void write_raw(const std::vector<uint8_t>& data) {
        buffer_.insert(buffer_.end(), data.begin(), data.end());
    }

    /// 读取 [pos, pos+len) 范围的数据
    /// 在真正的 Neon 中，读取路径通过 get_value_reconstruct_data() 实现，
    /// 最终调用 EphemeralFile 的读接口。
    std::vector<uint8_t> read(uint64_t pos, size_t len) const {
        if (pos + len > buffer_.size()) {
            throw std::runtime_error("EphemeralFile: read out of bounds");
        }
        return std::vector<uint8_t>(
            buffer_.begin() + static_cast<ptrdiff_t>(pos),
            buffer_.begin() + static_cast<ptrdiff_t>(pos + len));
    }

    /// 方便测试查看
    const std::vector<uint8_t>& data() const { return buffer_; }

private:
    std::vector<uint8_t> buffer_;
};
