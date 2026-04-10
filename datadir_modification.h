#pragma once
/// datadir_modification.h — DatadirModification: 修改上下文
///
/// 对应 Rust: pageserver/src/pgdatadir_mapping.rs
///   - DatadirModification (1676-1709)
///   - begin_modification (217-233)
///   - commit (2867-2948)
///
/// DatadirModification 是一个"修改事务"上下文。
/// WAL ingest 过程中，所有修改先暂存在 pending 缓冲区中，
/// 攒到一定量后调用 commit() 一次性写入 InMemoryLayer。
///
/// Rust 原文 (pgdatadir_mapping.rs:1676-1709):
///   pub struct DatadirModification<'a> {
///       tline: &'a Timeline,
///       pending_metadata_pages: HashMap<CompactKey, Vec<(Lsn, usize, Value)>>,
///       pending_data_batch: Option<SerializedValueBatch>,
///       pending_lsns: Vec<Lsn>,
///       pending_nblocks: i64,
///       lsn: Lsn,
///       ...
///   }

#include "types.h"
#include "serialized_batch.h"
#include "timeline_writer.h"

#include <map>
#include <vector>
#include <tuple>
#include <optional>
#include <algorithm>

/// 对应 Rust: DatadirModification (pgdatadir_mapping.rs:1676-1709)
///
/// 简化: 只保留与写入路径相关的核心字段。
/// 去掉了 pending_deletions, pending_directory_entries 等与核心写入无关的字段。
class DatadirModification {
public:
    /// 对应 Rust: Timeline::begin_modification (pgdatadir_mapping.rs:217-233)
    ///
    /// 创建一个修改上下文，绑定到一个 TimelineWriter。
    ///
    /// Rust 原文:
    ///   pub fn begin_modification(&self, lsn: Lsn) -> DatadirModification<'_> {
    ///       DatadirModification {
    ///           tline: self,
    ///           pending_metadata_pages: HashMap::new(),
    ///           pending_data_batch: None,
    ///           pending_lsns: Vec::new(),
    ///           lsn,
    ///           ...
    ///       }
    ///   }
    explicit DatadirModification(TimelineWriter& writer)
        : writer_(writer)
        , lsn_(0)
    {}

    /// 设置当前 LSN
    /// 对应 Rust: modification.set_lsn(next_record_lsn) (walingest.rs)
    void set_lsn(Lsn lsn) {
        lsn_ = lsn;
    }

    /// 对应 Rust: DatadirModification::put() / put_data()
    /// 简化: 将 (key, lsn, value) 放入 pending_data_batch。
    ///
    /// 在真正的 Neon 中有 put_data（数据页面）和 put（元数据页面）的区分:
    ///   - put_data → 写入 pending_data_batch (已序列化的 SerializedValueBatch)
    ///   - put → 写入 pending_metadata_pages (HashMap)
    ///
    /// 简化: 统一放入 pending_data_batch。
    void put(CompactKey key, const std::vector<uint8_t>& value, bool will_init = false) {
        if (!pending_data_batch_.has_value()) {
            pending_data_batch_.emplace();
        }

        auto& batch = *pending_data_batch_;
        uint64_t offset = static_cast<uint64_t>(batch.raw.size());
        batch.raw.insert(batch.raw.end(), value.begin(), value.end());
        batch.metadata.push_back(SerializedValueMeta{
            key, lsn_, offset, value.size(), will_init
        });
        batch.max_lsn = std::max(batch.max_lsn, lsn_);
        batch.len++;
    }

    /// 对应 Rust: DatadirModification::put() for metadata pages
    ///
    /// 元数据页面走单独的 pending_metadata_pages 路径。
    /// 在 commit() 中通过 SerializedValueBatch::from_values() 统一序列化。
    ///
    /// Rust 原文 (pgdatadir_mapping.rs):
    ///   self.pending_metadata_pages.entry(key).or_default().push((lsn, value_size, value));
    void put_metadata(CompactKey key, const std::vector<uint8_t>& value, bool will_init = false) {
        pending_metadata_.emplace_back(key, lsn_, value, will_init);
    }

    /// 对应 Rust: DatadirModification::commit (pgdatadir_mapping.rs:2867-2948)
    ///
    /// 提交所有 pending 修改，写入 InMemoryLayer。
    ///
    /// Rust 原文:
    ///   pub async fn commit(&mut self, ctx: &RequestContext) -> anyhow::Result<()> {
    ///       let mut writer = self.tline.writer().await;
    ///
    ///       // 1. 序列化 metadata pages
    ///       let metadata_batch = {
    ///           let pending_meta = self.pending_metadata_pages.drain()...collect();
    ///           if pending_meta.is_empty() { None }
    ///           else { Some(SerializedValueBatch::from_values(pending_meta)) }
    ///       };
    ///
    ///       // 2. 取出 data batch
    ///       let data_batch = self.pending_data_batch.take();
    ///
    ///       // 3. 合并
    ///       let maybe_batch = match (data_batch, metadata_batch) {
    ///           (Some(mut data), Some(metadata)) => { data.extend(metadata); Some(data) }
    ///           (Some(data), None) => Some(data),
    ///           (None, Some(metadata)) => Some(metadata),
    ///           (None, None) => None,
    ///       };
    ///
    ///       // 4. 写入 InMemoryLayer
    ///       if let Some(batch) = maybe_batch {
    ///           writer.put_batch(batch, ctx).await?;
    ///       }
    ///
    ///       // 5. 推进 last_record_lsn
    ///       for pending_lsn in self.pending_lsns.drain(..) {
    ///           writer.finish_write(pending_lsn);
    ///       }
    ///   }
    void commit() {
        // Step 1: 序列化 metadata pages
        // 对应 Rust: let metadata_batch = SerializedValueBatch::from_values(pending_meta);
        std::optional<SerializedValueBatch> metadata_batch;
        if (!pending_metadata_.empty()) {
            metadata_batch.emplace(
                SerializedValueBatch::from_values(pending_metadata_));
            pending_metadata_.clear();
        }

        // Step 2: 取出 data batch
        // 对应 Rust: let data_batch = self.pending_data_batch.take();
        std::optional<SerializedValueBatch> data_batch = std::move(pending_data_batch_);
        pending_data_batch_.reset();

        // Step 3: 合并 data_batch 和 metadata_batch
        // 对应 Rust: match (data_batch, metadata_batch) { ... }
        std::optional<SerializedValueBatch> maybe_batch;
        if (data_batch.has_value() && metadata_batch.has_value()) {
            data_batch->extend(std::move(*metadata_batch));
            maybe_batch = std::move(data_batch);
        } else if (data_batch.has_value()) {
            maybe_batch = std::move(data_batch);
        } else if (metadata_batch.has_value()) {
            maybe_batch = std::move(metadata_batch);
        }
        // else: (None, None) → 什么都不做

        // Step 4: 写入 InMemoryLayer
        // 对应 Rust: writer.put_batch(batch, ctx).await?;
        if (maybe_batch.has_value()) {
            writer_.put_batch(std::move(*maybe_batch));
        }

        // Step 5: 推进 last_record_lsn
        // 对应 Rust:
        //   self.pending_lsns.push(self.lsn);
        //   for pending_lsn in self.pending_lsns.drain(..) {
        //       writer.finish_write(pending_lsn);
        //   }
        pending_lsns_.push_back(lsn_);
        for (Lsn lsn : pending_lsns_) {
            writer_.finish_write(lsn);
        }
        pending_lsns_.clear();
    }

    /// 查看有多少 pending 项
    size_t pending_count() const {
        size_t count = pending_metadata_.size();
        if (pending_data_batch_.has_value()) {
            count += pending_data_batch_->len;
        }
        return count;
    }

    Lsn current_lsn() const { return lsn_; }

private:
    TimelineWriter& writer_;
    Lsn lsn_;

    /// 对应 Rust: pending_metadata_pages: HashMap<CompactKey, Vec<(Lsn, usize, Value)>>
    /// 简化: 展平为 vector of tuples
    std::vector<std::tuple<CompactKey, Lsn, std::vector<uint8_t>, bool>> pending_metadata_;

    /// 对应 Rust: pending_data_batch: Option<SerializedValueBatch>
    std::optional<SerializedValueBatch> pending_data_batch_;

    /// 对应 Rust: pending_lsns: Vec<Lsn>
    std::vector<Lsn> pending_lsns_;
};
