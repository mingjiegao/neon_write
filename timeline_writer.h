#pragma once
/// timeline_writer.h — TimelineWriter + TimelineWriterState + OpenLayerAction
///
/// 对应 Rust: pageserver/src/tenant/timeline.rs
///   - TimelineWriterState (7701-7722)
///   - TimelineWriter (7728-7746)
///   - OpenLayerAction (7742-7746)
///   - put_batch (7880-7930)
///   - get_open_layer_action (7830-7877)
///   - handle_open_layer_action (7749-7768)
///   - open_layer (7770-7785)
///   - finish_write (4873-4878, 7981)
///   - freeze_inmem_layer_at (4884-4925)
///
/// TimelineWriter 是写入路径的"调度器"，负责:
///   1. 决定是否需要 roll/open InMemoryLayer (get_open_layer_action)
///   2. 将 batch 写入当前的 InMemoryLayer (put_batch)
///   3. 推进 last_record_lsn (finish_write)

#include "types.h"
#include "inmemory_layer.h"
#include "serialized_batch.h"

#include <memory>
#include <optional>
#include <vector>
#include <functional>
#include <cassert>

/// 对应 Rust: OpenLayerAction (timeline.rs:7742-7746)
///
/// get_open_layer_action 的返回值，决定下一步操作:
///   - None: 继续写入当前 layer
///   - Open: 当前没有 layer，需要创建新的
///   - Roll: 当前 layer 太大，需要冻结旧的并创建新的
///
/// Rust 原文:
///   #[derive(PartialEq)]
///   enum OpenLayerAction {
///       Roll,
///       Open,
///       None,
///   }
enum class OpenLayerAction {
    None,   // 继续写当前 layer
    Open,   // 没有 layer，创建新的
    Roll,   // 冻结当前 layer，创建新的
};

/// 对应 Rust: TimelineWriterState (timeline.rs:7701-7722)
///
/// 持有当前打开的 InMemoryLayer 及相关状态。
///
/// Rust 原文:
///   pub(crate) struct TimelineWriterState {
///       open_layer: Arc<InMemoryLayer>,
///       current_size: u64,
///       prev_lsn: Option<Lsn>,
///       max_lsn: Option<Lsn>,
///       cached_last_freeze_at: Lsn,
///   }
struct TimelineWriterState {
    std::shared_ptr<InMemoryLayer> open_layer;
    uint64_t                       current_size;   // 当前 layer 已写入的字节数
    std::optional<Lsn>             prev_lsn;       // 上一次写入的 LSN
    std::optional<Lsn>             max_lsn;        // 通过当前 writer 的最大 LSN
    Lsn                            cached_last_freeze_at;

    /// 对应 Rust: TimelineWriterState::new (timeline.rs:7713-7721)
    TimelineWriterState(std::shared_ptr<InMemoryLayer> layer, uint64_t size, Lsn last_freeze)
        : open_layer(std::move(layer))
        , current_size(size)
        , prev_lsn(std::nullopt)
        , max_lsn(std::nullopt)
        , cached_last_freeze_at(last_freeze)
    {}
};

/// 对应 Rust: Timeline 中与写入相关的部分
///
/// 简化: 真正的 Timeline 有几千行代码，这里只提取写入路径相关的:
///   - TimelineWriter 的 put_batch / get_open_layer_action / handle_open_layer_action
///   - finish_write (推进 last_record_lsn)
///   - freeze_inmem_layer_at (冻结 layer)
///
/// Rust 原文 (timeline.rs:7728-7731):
///   pub(crate) struct TimelineWriter<'a> {
///       tl: &'a Timeline,
///       write_guard: MutexGuard<'a, Option<TimelineWriterState>>,
///   }
class TimelineWriter {
public:
    /// 构造: 指定 checkpoint_distance（当 layer 大小超过此值时 roll）
    explicit TimelineWriter(uint64_t checkpoint_distance = 256 * 1024 * 1024)
        : checkpoint_distance_(checkpoint_distance)
        , last_record_lsn_(0)
        , last_freeze_at_(0)
    {}

    /// 对应 Rust: TimelineWriter::put_batch (timeline.rs:7880-7930)
    ///
    /// 写入一个 batch 到 InMemoryLayer。
    ///
    /// Rust 原文:
    ///   pub(crate) async fn put_batch(&mut self, batch: SerializedValueBatch, ctx: &RequestContext)
    ///       -> anyhow::Result<()>
    ///   {
    ///       if !batch.has_data() { return Ok(()); }
    ///
    ///       let batch_max_lsn = batch.max_lsn;
    ///       let buf_size: u64 = batch.buffer_size() as u64;
    ///
    ///       let action = self.get_open_layer_action(batch_max_lsn, buf_size);
    ///       let layer = self.handle_open_layer_action(batch_max_lsn, action, ctx).await?;
    ///
    ///       layer.put_batch(batch, ctx).await?;
    ///
    ///       let state = self.write_guard.as_mut().unwrap();
    ///       state.current_size += buf_size;
    ///       state.prev_lsn = Some(batch_max_lsn);
    ///       state.max_lsn = std::cmp::max(state.max_lsn, Some(batch_max_lsn));
    ///
    ///       Ok(())
    ///   }
    void put_batch(SerializedValueBatch batch) {
        if (!batch.has_data()) {
            return;
        }

        Lsn batch_max_lsn = batch.max_lsn;
        uint64_t buf_size = static_cast<uint64_t>(batch.buffer_size());

        // Step 1: 判断是否需要 roll/open layer
        // 对应 Rust: let action = self.get_open_layer_action(batch_max_lsn, buf_size);
        OpenLayerAction action = get_open_layer_action(batch_max_lsn, buf_size);

        // Step 2: 执行 action（可能创建新 layer 或冻结旧 layer）
        // 对应 Rust: let layer = self.handle_open_layer_action(batch_max_lsn, action, ctx).await?;
        auto& layer = handle_open_layer_action(batch_max_lsn, action);

        // Step 3: 写入 InMemoryLayer
        // 对应 Rust: layer.put_batch(batch, ctx).await?;
        layer->put_batch(std::move(batch));

        // Step 4: 更新 writer 状态
        // 对应 Rust:
        //   state.current_size += buf_size;
        //   state.prev_lsn = Some(batch_max_lsn);
        //   state.max_lsn = std::cmp::max(state.max_lsn, Some(batch_max_lsn));
        state_->current_size += buf_size;
        state_->prev_lsn = batch_max_lsn;
        if (!state_->max_lsn.has_value() || batch_max_lsn > *state_->max_lsn) {
            state_->max_lsn = batch_max_lsn;
        }
    }

    /// 对应 Rust: Timeline::finish_write (timeline.rs:4873-4878)
    /// + TimelineWriter::finish_write (timeline.rs:7981)
    ///
    /// 推进 last_record_lsn。
    ///
    /// Rust 原文:
    ///   pub(crate) fn finish_write(&self, new_lsn: Lsn) {
    ///       self.last_record_lsn.advance(new_lsn);
    ///   }
    void finish_write(Lsn new_lsn) {
        if (new_lsn > last_record_lsn_) {
            last_record_lsn_ = new_lsn;
        }
    }

    // ============================================================
    // 状态查询
    // ============================================================

    Lsn last_record_lsn() const { return last_record_lsn_; }
    Lsn last_freeze_at() const { return last_freeze_at_; }

    /// 当前活跃的 InMemoryLayer（可能为空）
    std::shared_ptr<InMemoryLayer> current_layer() const {
        return state_ ? state_->open_layer : nullptr;
    }

    /// 已冻结的 layers
    const std::vector<std::shared_ptr<InMemoryLayer>>& frozen_layers() const {
        return frozen_layers_;
    }

private:
    /// 对应 Rust: TimelineWriter::get_open_layer_action (timeline.rs:7830-7877)
    ///
    /// 决定在写入前需要做什么操作。
    ///
    /// Rust 原文:
    ///   fn get_open_layer_action(&self, lsn: Lsn, new_value_size: u64) -> OpenLayerAction {
    ///       let Some(state) = &state else { return OpenLayerAction::Open; };
    ///       if state.prev_lsn == Some(lsn) { return OpenLayerAction::None; }
    ///       if state.current_size == 0 { return OpenLayerAction::None; }
    ///       if self.tl.should_roll(state.current_size, state.current_size + new_value_size, ...) {
    ///           OpenLayerAction::Roll
    ///       } else {
    ///           OpenLayerAction::None
    ///       }
    ///   }
    OpenLayerAction get_open_layer_action(Lsn lsn, uint64_t new_value_size) const {
        // 没有活跃 layer → 需要 Open
        if (!state_) {
            return OpenLayerAction::Open;
        }
        const auto& state = *state_;

        // 同一个 LSN 内不 roll（Neon 不支持 mid-LSN roll）
        // 对应 Rust (timeline.rs:7852-7858):
        //   if state.prev_lsn == Some(lsn) { return OpenLayerAction::None; }
        if (state.prev_lsn.has_value() && *state.prev_lsn == lsn) {
            return OpenLayerAction::None;
        }

        // 空 layer 不 roll
        // 对应 Rust (timeline.rs:7860-7862):
        //   if state.current_size == 0 { return OpenLayerAction::None; }
        if (state.current_size == 0) {
            return OpenLayerAction::None;
        }

        // 判断是否超过 checkpoint_distance → Roll
        // 对应 Rust: self.tl.should_roll(current_size, current_size + new_value_size, ...)
        // 简化: 只看大小是否超过 checkpoint_distance
        if (state.current_size + new_value_size > checkpoint_distance_) {
            return OpenLayerAction::Roll;
        }

        return OpenLayerAction::None;
    }

    /// 对应 Rust: TimelineWriter::handle_open_layer_action (timeline.rs:7749-7768)
    ///
    /// 执行 get_open_layer_action 返回的操作。
    ///
    /// Rust 原文:
    ///   async fn handle_open_layer_action(&mut self, at: Lsn, action: OpenLayerAction, ctx)
    ///       -> anyhow::Result<&Arc<InMemoryLayer>>
    ///   {
    ///       match action {
    ///           Roll => { self.roll_layer(freeze_at).await?; self.open_layer(at, ctx).await?; }
    ///           Open => self.open_layer(at, ctx).await?,
    ///           None => { assert!(self.write_guard.is_some()); }
    ///       }
    ///       Ok(&self.write_guard.as_ref().unwrap().open_layer)
    ///   }
    std::shared_ptr<InMemoryLayer>& handle_open_layer_action(Lsn at, OpenLayerAction action) {
        switch (action) {
        case OpenLayerAction::Roll: {
            // 冻结当前 layer，然后打开新的
            // 对应 Rust (timeline.rs:7756-7759):
            //   let freeze_at = state.max_lsn.unwrap();
            //   self.roll_layer(freeze_at).await?;
            //   self.open_layer(at, ctx).await?;
            Lsn freeze_at = state_->max_lsn.value();
            roll_layer(freeze_at);
            open_layer(at);
            break;
        }
        case OpenLayerAction::Open:
            // 没有 layer，创建新的
            // 对应 Rust (timeline.rs:7761):
            //   OpenLayerAction::Open => self.open_layer(at, ctx).await?,
            open_layer(at);
            break;
        case OpenLayerAction::None:
            // 继续使用当前 layer
            // 对应 Rust (timeline.rs:7762-7764):
            //   OpenLayerAction::None => { assert!(self.write_guard.is_some()); }
            assert(state_.has_value());
            break;
        }

        return state_->open_layer;
    }

    /// 对应 Rust: TimelineWriter::open_layer (timeline.rs:7770-7785)
    ///
    /// 创建新的 InMemoryLayer。
    ///
    /// Rust 原文:
    ///   async fn open_layer(&mut self, at: Lsn, ctx) -> anyhow::Result<()> {
    ///       let layer = self.tl.get_layer_for_write(at, ...).await?;
    ///       let initial_size = layer.len();
    ///       let last_freeze_at = self.last_freeze_at.load();
    ///       self.write_guard.replace(TimelineWriterState::new(layer, initial_size, last_freeze_at));
    ///   }
    void open_layer(Lsn at) {
        auto layer = std::make_shared<InMemoryLayer>(at);
        uint64_t initial_size = layer->len();  // 新 layer，大小为 0
        state_.emplace(TimelineWriterState(layer, initial_size, last_freeze_at_));
    }

    /// 对应 Rust: TimelineWriter::roll_layer (timeline.rs:7787-7828)
    ///   → freeze_inmem_layer_at (timeline.rs:4884-4925)
    ///
    /// 冻结当前 layer 并放入 frozen_layers。
    ///
    /// Rust 原文 (简化):
    ///   async fn roll_layer(&mut self, freeze_at: Lsn) {
    ///       self.tl.freeze_inmem_layer_at(freeze_at, &mut self.write_guard).await?;
    ///       // write_guard 被清空
    ///   }
    ///
    ///   async fn freeze_inmem_layer_at(&self, at: Lsn, write_lock) {
    ///       guard.open_mut()?.try_freeze_in_memory_layer(at, ...);
    ///       // 通知 flush 线程
    ///   }
    ///
    ///   // layer_manager.rs:448-449 — 关键: end_lsn = freeze_at + 1
    ///   let Lsn(last_record_lsn) = lsn;
    ///   let end_lsn = Lsn(last_record_lsn + 1);
    ///   open_layer.freeze(end_lsn).await;
    void roll_layer(Lsn freeze_at) {
        assert(state_.has_value());

        // 冻结当前 layer
        // 关键: end_lsn = freeze_at + 1（exclusive）
        // 对应 Rust (layer_manager.rs:449):
        //   let end_lsn = Lsn(last_record_lsn + 1);
        Lsn end_lsn = freeze_at + 1;
        state_->open_layer->freeze(end_lsn);

        // 放入 frozen_layers
        // 在真正的 Neon 中，frozen_layers 在 LayerManager 中管理，
        // flush 线程会异步将它们写入磁盘
        frozen_layers_.push_back(state_->open_layer);

        // 更新 last_freeze_at
        // 对应 Rust (layer_manager.rs:473): last_freeze_at.store(end_lsn);
        last_freeze_at_ = end_lsn;

        // 清空 state（对应 Rust: write_guard = None）
        state_.reset();
    }

    uint64_t checkpoint_distance_;   // 触发 roll 的大小阈值
    Lsn last_record_lsn_;           // 对应 Rust: Timeline::last_record_lsn
    Lsn last_freeze_at_;            // 对应 Rust: Timeline::last_freeze_at

    /// 对应 Rust: write_guard: MutexGuard<Option<TimelineWriterState>>
    std::optional<TimelineWriterState> state_;

    /// 对应 Rust: LayerMap::frozen_layers (layer_map.rs:89)
    /// 在真正的 Neon 中，frozen_layers 在 LayerManager 里管理。
    std::vector<std::shared_ptr<InMemoryLayer>> frozen_layers_;
};
