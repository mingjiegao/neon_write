#pragma once
/// serialized_batch.h — SerializedValueBatch + SerializedValueMeta
///
/// 对应 Rust: libs/wal_decoder/src/serialized_batch.rs
///
/// SerializedValueBatch 是写入路径中的核心数据传输结构。
/// 它把多个 (key, lsn, value) 打包成一个批次：
///   - raw: 所有 value 的字节拼接
///   - metadata: 每个 value 的元信息（key, lsn, 在 raw 中的位置, 长度, will_init）
///
/// 在 commit() 中构造，传递给 TimelineWriter::put_batch()，
/// 最终写入 InMemoryLayer。

#include "types.h"
#include <vector>
#include <tuple>
#include <cstdint>
#include <cstring>
#include <string>

/// 对应 Rust: SerializedValueMeta (serialized_batch.rs:81-89)
///
/// 一个 value 在 batch 中的元信息。
///
/// Rust 原文:
///   pub struct SerializedValueMeta {
///       pub key: CompactKey,
///       pub lsn: Lsn,
///       pub batch_offset: u64,  // 在 raw 中的起始位置
///       pub len: usize,         // 数据长度
///       pub will_init: bool,    // 是否完整初始化
///   }
struct SerializedValueMeta {
    CompactKey key;
    Lsn        lsn;
    uint64_t   batch_offset;  // 在 raw 中的起始偏移
    size_t     len;           // 数据长度
    bool       will_init;     // 是否为 FPI (Full Page Image)
};

/// 对应 Rust: SerializedValueBatch (serialized_batch.rs:100-117)
///
/// 一个批次的序列化值。包含原始字节 + 元信息。
///
/// Rust 原文:
///   pub struct SerializedValueBatch {
///       pub raw: Vec<u8>,              // 所有 value 的字节拼接
///       pub metadata: Vec<ValueMeta>,  // 每个 value 的元信息
///       pub max_lsn: Lsn,             // batch 中最大的 LSN
///       pub len: usize,               // value 的数量
///   }
///
/// 简化: Rust 中 ValueMeta 是 enum { Serialized(SerializedValueMeta), Observed(...) }，
/// 这里只保留 Serialized 变体。Observed 是 shard 0 观察其他 shard 的 key 用的，
/// 与核心写入逻辑无关。
struct SerializedValueBatch {
    std::vector<uint8_t>             raw;       // 所有 value 的字节拼接
    std::vector<SerializedValueMeta> metadata;  // 元信息
    Lsn                              max_lsn;   // 最大 LSN
    size_t                           len;       // value 数量

    SerializedValueBatch() : max_lsn(0), len(0) {}

    /// 对应 Rust: SerializedValueBatch::has_data() — 检查是否有实际数据
    bool has_data() const { return len > 0; }

    /// 对应 Rust: SerializedValueBatch::buffer_size() — raw 的字节大小
    size_t buffer_size() const { return raw.size(); }

    /// 对应 Rust: SerializedValueBatch::extend (serialized_batch.rs:405)
    ///
    /// 把另一个 batch 合并到当前 batch 中。
    /// 需要调整 other 中所有 metadata 的 batch_offset（加上当前 raw 的长度）。
    ///
    /// Rust 原文:
    ///   pub fn extend(&mut self, mut other: SerializedValueBatch) {
    ///       let base = self.raw.len() as u64;
    ///       for meta in &mut other.metadata {
    ///           if let ValueMeta::Serialized(ser) = meta {
    ///               ser.batch_offset += base;
    ///           }
    ///       }
    ///       self.raw.extend(other.raw);
    ///       self.metadata.extend(other.metadata);
    ///       self.max_lsn = std::cmp::max(self.max_lsn, other.max_lsn);
    ///       self.len += other.len;
    ///   }
    void extend(SerializedValueBatch&& other) {
        uint64_t base = static_cast<uint64_t>(raw.size());
        // 调整 other 的 batch_offset
        for (auto& meta : other.metadata) {
            meta.batch_offset += base;
        }
        // 合并 raw
        raw.insert(raw.end(), other.raw.begin(), other.raw.end());
        // 合并 metadata
        metadata.insert(metadata.end(),
                        std::make_move_iterator(other.metadata.begin()),
                        std::make_move_iterator(other.metadata.end()));
        max_lsn = std::max(max_lsn, other.max_lsn);
        len += other.len;
    }

    /// 对应 Rust: SerializedValueBatch::from_values (serialized_batch.rs)
    ///
    /// 从 (key, lsn, value_size, value_bytes) 列表构造 batch。
    /// 简化版：value 用 std::vector<uint8_t> 表示。
    static SerializedValueBatch from_values(
        const std::vector<std::tuple<CompactKey, Lsn, std::vector<uint8_t>, bool>>& values)
    {
        SerializedValueBatch batch;
        for (auto& [key, lsn, data, will_init] : values) {
            uint64_t offset = static_cast<uint64_t>(batch.raw.size());
            batch.raw.insert(batch.raw.end(), data.begin(), data.end());
            batch.metadata.push_back(SerializedValueMeta{
                key, lsn, offset, data.size(), will_init
            });
            batch.max_lsn = std::max(batch.max_lsn, lsn);
            batch.len++;
        }
        return batch;
    }
};
