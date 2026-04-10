#pragma once
/// types.h — 基础类型定义
///
/// 对应 Neon 源码:
///   - utils::lsn::Lsn          → Lsn (uint64_t)
///   - pageserver_api::key::Key  → CompactKey (int64_t, 简化版)
///
/// Neon 中 Key 是一个 18 字节的复合键 (libs/pageserver_api/src/key.rs:19-26):
///   struct Key { field1: u8, field2: u32, field3: u32, field4: u32, field5: u8, field6: u32 }
/// 通过 to_i128() 编码为 i128。这里简化为 int64_t，足够测试用。

#include <cstdint>
#include <vector>
#include <string>

/// 对应 Rust: utils::lsn::Lsn(u64)
/// WAL 日志序列号，标识 WAL 流中的位置。
using Lsn = uint64_t;

/// 对应 Rust: pageserver_api::key::CompactKey(i128)
/// 简化为 int64_t，测试中 key 值都很小。
/// 在真正的 Neon 中，CompactKey 编码了 (表空间, 数据库, 表, fork, 块号)。
using CompactKey = int64_t;
