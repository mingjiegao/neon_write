#pragma once
/// inmemory_layer.h — InMemoryLayer: 内存层
///
/// 对应 Rust: pageserver/src/tenant/storage_layer/inmemory_layer.rs
///
/// InMemoryLayer 是 Neon 写入路径的核心数据结构。
/// 所有从 Safekeeper 接收到的 WAL 都先写入 InMemoryLayer，
/// 然后在 checkpoint/compaction 时冻结并刷写到磁盘成为 delta/image 层。
///
/// 内部结构:
///   - file (EphemeralFile): 追加写入的临时文件，存储 value 的原始字节
///   - index (BTreeMap<CompactKey, VecMap<Lsn, IndexEntry>>):
///     二级索引，第一级按 key，第二级按 lsn，值为 IndexEntry (文件中的位置)
///
/// Rust 原文 (inmemory_layer.rs:47-84):
///   pub struct InMemoryLayer {
///       start_lsn: Lsn,
///       end_lsn: OnceLock<Lsn>,
///       index: RwLock<BTreeMap<CompactKey, VecMap<Lsn, IndexEntry>>>,
///       file: EphemeralFile,
///       estimated_in_mem_size: AtomicU64,
///       ...
///   }

#include "types.h"
#include "vec_map.h"
#include "index_entry.h"
#include "ephemeral_file.h"
#include "serialized_batch.h"

#include <map>
#include <optional>
#include <atomic>
#include <stdexcept>
#include <cassert>

/// 对应 Rust: InMemoryLayer (inmemory_layer.rs:47-84)
class InMemoryLayer {
public:
    /// 对应 Rust: InMemoryLayer::create (inmemory_layer.rs:531-561)
    ///
    /// 创建一个新的空 InMemoryLayer。
    ///
    /// Rust 原文:
    ///   pub async fn create(conf, timeline_id, tenant_shard_id, start_lsn, ...) -> Result<InMemoryLayer> {
    ///       let file = EphemeralFile::create(...).await?;
    ///       Ok(InMemoryLayer {
    ///           start_lsn,
    ///           end_lsn: OnceLock::new(),
    ///           index: RwLock::new(BTreeMap::new()),
    ///           file,
    ///           estimated_in_mem_size: AtomicU64::new(0),
    ///           ...
    ///       })
    ///   }
    explicit InMemoryLayer(Lsn start_lsn)
        : start_lsn_(start_lsn)
        , end_lsn_()
        , index_()
        , file_()
        , estimated_in_mem_size_(0)
    {}

    /// 对应 Rust: InMemoryLayer::put_batch (inmemory_layer.rs:571-644)
    ///
    /// 写入路径的核心函数。将一个 SerializedValueBatch 写入 InMemoryLayer。
    ///
    /// 步骤:
    ///   1. 记录写入前文件大小 (base_offset)
    ///   2. 将 batch.raw 追加写入 EphemeralFile
    ///   3. 遍历 batch.metadata，为每个 (key, lsn) 创建 IndexEntry 并更新索引
    ///
    /// Rust 原文 (inmemory_layer.rs:571-644):
    ///   pub async fn put_batch(&self, serialized_batch: SerializedValueBatch, ctx: &RequestContext)
    ///       -> anyhow::Result<()>
    ///   {
    ///       self.assert_writable();
    ///       let base_offset = self.file.len();
    ///
    ///       let SerializedValueBatch { raw, metadata, .. } = serialized_batch;
    ///
    ///       // Step 1: 写入文件
    ///       self.file.write_raw(&raw, ctx).await?;
    ///
    ///       // Step 2: 更新索引
    ///       let mut index = self.index.write().await;
    ///       for meta in metadata {
    ///           let SerializedValueMeta { key, lsn, batch_offset, len, will_init } = meta;
    ///           let index_entry = IndexEntry::new(IndexEntryNewArgs {
    ///               base_offset, batch_offset, len, will_init,
    ///           })?;
    ///           let vec_map = index.entry(key).or_default();
    ///           vec_map.append_or_update_last(lsn, index_entry);
    ///           self.estimated_in_mem_size.fetch_add(...);
    ///       }
    ///       Ok(())
    ///   }
    void put_batch(SerializedValueBatch batch) {
        assert_writable();

        // Step 1: 记录写入前的文件偏移
        // 对应 Rust: let base_offset = self.file.len();
        uint64_t base_offset = file_.len();

        // Step 2: 将 raw 数据追加写入 EphemeralFile
        // 对应 Rust: self.file.write_raw(&raw, ctx).await?;
        file_.write_raw(batch.raw);

        // 验证写入后文件大小正确
        // 对应 Rust: assert_eq!(new_size, expected_new_len);
        uint64_t new_size = file_.len();
        uint64_t expected = base_offset + static_cast<uint64_t>(batch.raw.size());
        assert(new_size == expected);

        // Step 3: 更新索引
        // 对应 Rust:
        //   let mut index = self.index.write().await;
        //   for meta in metadata { ... }
        for (const auto& meta : batch.metadata) {
            // 对应 Rust:
            //   let index_entry = IndexEntry::new(IndexEntryNewArgs {
            //       base_offset, batch_offset, len, will_init,
            //   })?;
            IndexEntry entry = IndexEntry::create(IndexEntryNewArgs{
                base_offset, meta.batch_offset, meta.len, meta.will_init
            });

            // 对应 Rust:
            //   let vec_map = index.entry(key).or_default();
            //   vec_map.append_or_update_last(lsn, index_entry);
            auto& vec_map = index_[meta.key];  // or_default: 不存在则默认构造
            auto old = vec_map.append_or_update_last(meta.lsn, entry);

            // 对应 Rust (inmemory_layer.rs:626-633):
            //   if old.is_some() {
            //       warn!("Key {} at {} written twice at same LSN", key, lsn);
            //   }
            if (old.has_value()) {
                // 同一个 (key, lsn) 被写了两次，保留最新的
                // 正常运行时不应该发生，但 Neon 对此做了容错
            }

            // 对应 Rust (inmemory_layer.rs:635-640):
            //   self.estimated_in_mem_size.fetch_add(
            //       (size_of::<CompactKey>() + size_of::<Lsn>() + size_of::<IndexEntry>()) as u64, ...);
            estimated_in_mem_size_ +=
                sizeof(CompactKey) + sizeof(Lsn) + sizeof(IndexEntry);
        }
    }

    /// 对应 Rust: InMemoryLayer::freeze (inmemory_layer.rs:673-700)
    ///
    /// 冻结 layer，设置 end_lsn。冻结后不再接受写入。
    ///
    /// Rust 原文:
    ///   pub async fn freeze(&self, end_lsn: Lsn) {
    ///       assert!(self.start_lsn < end_lsn);
    ///       self.end_lsn.set(end_lsn).expect("end_lsn set only once");
    ///   }
    void freeze(Lsn end_lsn) {
        if (start_lsn_ >= end_lsn) {
            throw std::runtime_error("freeze: start_lsn >= end_lsn");
        }
        if (end_lsn_.has_value()) {
            throw std::runtime_error("freeze: end_lsn set only once");
        }
        end_lsn_ = end_lsn;
    }

    // ============================================================
    // 读取接口（简化版，用于测试验证）
    // ============================================================

    /// 读取 (key, lsn) 处的数据
    /// 在真正的 Neon 中，这对应 get_value_reconstruct_data() 的复杂逻辑。
    /// 这里简化为直接从 EphemeralFile 读取对应的字节。
    std::optional<std::vector<uint8_t>> get(CompactKey key, Lsn lsn) const {
        auto it = index_.find(key);
        if (it == index_.end()) return std::nullopt;
        for (auto& [l, entry] : it->second) {
            if (l == lsn) {
                return file_.read(entry.pos, entry.len);
            }
        }
        return std::nullopt;
    }

    // ============================================================
    // 状态查询
    // ============================================================

    Lsn start_lsn() const { return start_lsn_; }
    std::optional<Lsn> end_lsn() const { return end_lsn_; }
    bool is_frozen() const { return end_lsn_.has_value(); }

    /// 对应 Rust: InMemoryLayer::len() — EphemeralFile 大小
    uint64_t len() const { return file_.len(); }

    uint64_t estimated_in_mem_size() const { return estimated_in_mem_size_; }

    /// 索引中有多少个 key
    size_t key_count() const { return index_.size(); }

    /// 索引中总共有多少个 (key, lsn) 对
    size_t entry_count() const {
        size_t count = 0;
        for (auto& [_, vm] : index_) {
            count += vm.size();
        }
        return count;
    }

    /// 只读访问索引（用于测试）
    const std::map<CompactKey, VecMap<Lsn, IndexEntry>>& index() const { return index_; }

    /// 只读访问文件（用于测试）
    const EphemeralFile& file() const { return file_; }

private:
    /// 对应 Rust: self.assert_writable() (inmemory_layer.rs:576)
    /// 检查 layer 未被冻结
    void assert_writable() const {
        if (end_lsn_.has_value()) {
            throw std::runtime_error("InMemoryLayer is frozen, cannot write");
        }
    }

    Lsn start_lsn_;             // 对应 Rust: start_lsn: Lsn
    std::optional<Lsn> end_lsn_; // 对应 Rust: end_lsn: OnceLock<Lsn>

    /// 对应 Rust: index: RwLock<BTreeMap<CompactKey, VecMap<Lsn, IndexEntry>>>
    ///
    /// 二级索引结构:
    ///   CompactKey → VecMap<Lsn, IndexEntry>
    ///     即: 每个 page key → [(lsn1, entry1), (lsn2, entry2), ...]
    ///
    /// VecMap 是追加有序的，所以同一个 key 的版本按 LSN 递增排列。
    std::map<CompactKey, VecMap<Lsn, IndexEntry>> index_;

    /// 对应 Rust: file: EphemeralFile
    EphemeralFile file_;

    /// 对应 Rust: estimated_in_mem_size: AtomicU64
    uint64_t estimated_in_mem_size_;
};
