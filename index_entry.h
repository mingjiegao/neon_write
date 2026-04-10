#pragma once
/// index_entry.h — IndexEntry: InMemoryLayer 索引条目
///
/// 对应 Rust: pageserver/src/tenant/storage_layer/inmemory_layer.rs:106-160
///
/// IndexEntry 是一个 bit-packed 的 u64，编码三个字段:
///   - will_init (1 bit):   该页面版本是否是完整初始化（image）
///   - len (约27 bits):     数据长度
///   - pos (约36 bits):     在 EphemeralFile 中的偏移位置
///
/// Rust 原文 (inmemory_layer.rs:106-115):
///   /// Layout:
///   /// - 1 bit: `will_init`
///   /// - [`MAX_SUPPORTED_BLOB_LEN_BITS`]: `len`
///   /// - [`MAX_SUPPORTED_POS_BITS`]: `pos`
///   #[derive(Debug, Clone, Copy, PartialEq, Eq)]
///   pub struct IndexEntry(u64);
///
/// 简化：我们不做 bit-packing，直接用 struct 存储三个字段。
/// 这样更容易理解，不影响逻辑正确性。

#include "types.h"
#include <cstdint>
#include <string>

/// 对应 Rust: IndexEntryNewArgs (inmemory_layer.rs:153-158)
///
/// 创建 IndexEntry 时的参数。
/// Rust 原文:
///   let IndexEntryNewArgs { base_offset, batch_offset, len, will_init } = arg;
struct IndexEntryNewArgs {
    uint64_t base_offset;   // EphemeralFile 当前末尾偏移（写入 batch 前）
    uint64_t batch_offset;  // 该条目在 batch.raw 中的起始偏移
    size_t   len;           // 数据长度（字节）
    bool     will_init;     // 是否为完整页面初始化
};

/// 对应 Rust: IndexEntry (inmemory_layer.rs:115)
///
/// 存储一个页面版本在 EphemeralFile 中的位置信息。
/// 在 InMemoryLayer::index 中，每个 (CompactKey, Lsn) 对应一个 IndexEntry。
///
/// Rust 中是 bit-packed u64:
///   pos = base_offset + batch_offset  (文件中的绝对位置)
///   len = 数据长度
///   will_init = 是否是 FPI (Full Page Image)
///
/// 这里简化为普通 struct。
struct IndexEntry {
    uint64_t pos;       // = base_offset + batch_offset，文件中的绝对位置
    size_t   len;       // 数据长度
    bool     will_init; // 是否为完整页面初始化 (对应 WAL 中的 FPI)

    /// 对应 Rust: IndexEntry::new (inmemory_layer.rs:153)
    ///
    /// Rust 原文:
    ///   fn new(arg: IndexEntryNewArgs) -> anyhow::Result<Self> {
    ///       let pos = base_offset.checked_add(batch_offset)...;
    ///       ...
    ///       Ok(IndexEntry(will_init_bit | len_bits | pos_bits))
    ///   }
    static IndexEntry create(const IndexEntryNewArgs& args) {
        return IndexEntry{
            args.base_offset + args.batch_offset,  // pos = base_offset + batch_offset
            args.len,
            args.will_init,
        };
    }
};
