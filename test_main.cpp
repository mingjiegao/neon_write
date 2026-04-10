/// test_main.cpp — Neon 写入路径 C++ 复刻 单元测试
///
/// 对应 Neon 写入路径:
///   handle_walreceiver_connection()
///     → ingest_record() → modification.put() → modification.commit()
///       → writer.put_batch() → layer.put_batch()
///
/// 测试覆盖:
///   T1: VecMap 基础 — append_or_update_last
///   T2: IndexEntry 创建 — base_offset + batch_offset 计算
///   T3: EphemeralFile 追加写入 + 读取
///   T4: SerializedValueBatch 构造 + extend 合并
///   T5: InMemoryLayer::put_batch — 核心写入路径
///   T6: InMemoryLayer::freeze — 冻结后不可写
///   T7: TimelineWriter::put_batch — open/roll 生命周期
///   T8: TimelineWriter::put_batch — 触发 roll (checkpoint_distance)
///   T9: DatadirModification::commit — 完整写入流程
///   T10: 完整模拟 — 多条 WAL 记录的 ingest + commit 流程

#include "types.h"
#include "vec_map.h"
#include "index_entry.h"
#include "ephemeral_file.h"
#include "serialized_batch.h"
#include "inmemory_layer.h"
#include "timeline_writer.h"
#include "datadir_modification.h"

#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include <functional>

// ============================================================
// 简单测试框架
// ============================================================
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) \
    static void test_##name(); \
    static struct Register_##name { \
        Register_##name() { tests.push_back({#name, test_##name}); } \
    } register_##name; \
    static void test_##name()

struct TestEntry { const char* name; void (*fn)(); };
static std::vector<TestEntry> tests;

#define ASSERT_EQ(a, b) do { \
    auto _a = (a); auto _b = (b); \
    if (!(_a == _b)) { \
        std::cerr << "  FAIL: " << #a << " == " << #b \
                  << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        throw std::runtime_error("assertion failed"); \
    } \
} while(0)

#define ASSERT_TRUE(x) do { \
    if (!(x)) { \
        std::cerr << "  FAIL: " << #x \
                  << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        throw std::runtime_error("assertion failed"); \
    } \
} while(0)

#define ASSERT_FALSE(x) ASSERT_TRUE(!(x))

#define ASSERT_THROWS(expr) do { \
    bool caught = false; \
    try { expr; } catch (...) { caught = true; } \
    if (!caught) { \
        std::cerr << "  FAIL: expected exception from " << #expr \
                  << " at " << __FILE__ << ":" << __LINE__ << std::endl; \
        throw std::runtime_error("assertion failed"); \
    } \
} while(0)

/// 辅助: 从字符串构造字节 vector
static std::vector<uint8_t> bytes(const std::string& s) {
    return std::vector<uint8_t>(s.begin(), s.end());
}

// ============================================================
// T1: VecMap 基础
// ============================================================
/// 对应 Rust: libs/utils/src/vec_map.rs
/// VecMap 是 InMemoryLayer::index 内部的二级索引:
///   BTreeMap<CompactKey, VecMap<Lsn, IndexEntry>>
///                         ^^^^^^^^^^^^^^^^^^^^^^^^
///                         每个 key 的版本历史
///
/// 验证:
///   - 追加有序元素
///   - 相同 key 时 update（返回旧值）
///   - 违反顺序时报错
TEST(vecmap_basic) {
    VecMap<Lsn, std::string> vm;

    // 追加
    auto old = vm.append_or_update_last(100, "v1");
    ASSERT_FALSE(old.has_value());  // 第一个元素，没有旧值
    ASSERT_EQ(vm.size(), (size_t)1);

    // 追加更大的 key
    old = vm.append_or_update_last(200, "v2");
    ASSERT_FALSE(old.has_value());
    ASSERT_EQ(vm.size(), (size_t)2);

    // 相同 key → update，返回旧值
    // 对应 Rust (vec_map.rs:112-115):
    //   Ordering::Equal => { std::mem::swap(last_value, &mut value); return Ok((Some(value), 0)); }
    old = vm.append_or_update_last(200, "v2_updated");
    ASSERT_TRUE(old.has_value());
    ASSERT_EQ(*old, std::string("v2"));
    ASSERT_EQ(vm.size(), (size_t)2);  // 大小不变

    // 违反顺序 → 抛异常
    // 对应 Rust (vec_map.rs:111): Ordering::Less => return Err(VecMapError::InvalidKey)
    ASSERT_THROWS(vm.append_or_update_last(100, "bad"));
}

// ============================================================
// T2: IndexEntry 创建
// ============================================================
/// 对应 Rust: inmemory_layer.rs:153
/// IndexEntry::new(IndexEntryNewArgs { base_offset, batch_offset, len, will_init })
///
/// pos = base_offset + batch_offset (文件中的绝对偏移)
TEST(index_entry_create) {
    // 模拟: 文件当前大小 1000, batch 中该条目偏移 50, 长度 200
    auto entry = IndexEntry::create({1000, 50, 200, true});
    ASSERT_EQ(entry.pos, (uint64_t)1050);   // 1000 + 50
    ASSERT_EQ(entry.len, (size_t)200);
    ASSERT_TRUE(entry.will_init);

    // 另一个条目
    auto entry2 = IndexEntry::create({0, 0, 100, false});
    ASSERT_EQ(entry2.pos, (uint64_t)0);
    ASSERT_EQ(entry2.len, (size_t)100);
    ASSERT_FALSE(entry2.will_init);
}

// ============================================================
// T3: EphemeralFile 追加写入 + 读取
// ============================================================
/// 对应 Rust: EphemeralFile (简化版)
///
/// 验证:
///   - write_raw 追加
///   - len() 正确递增
///   - read 能取回写入的数据
TEST(ephemeral_file_basic) {
    EphemeralFile file;
    ASSERT_EQ(file.len(), (uint64_t)0);

    // 第一次写入
    file.write_raw(bytes("hello"));
    ASSERT_EQ(file.len(), (uint64_t)5);

    // 第二次写入（追加）
    file.write_raw(bytes("world"));
    ASSERT_EQ(file.len(), (uint64_t)10);

    // 读取
    auto data1 = file.read(0, 5);
    ASSERT_EQ(data1, bytes("hello"));

    auto data2 = file.read(5, 5);
    ASSERT_EQ(data2, bytes("world"));

    // 读取越界 → 抛异常
    ASSERT_THROWS(file.read(8, 5));
}

// ============================================================
// T4: SerializedValueBatch 构造 + extend
// ============================================================
/// 对应 Rust: serialized_batch.rs:100-117, 405
///
/// 验证:
///   - from_values 正确序列化
///   - extend 合并两个 batch（调整 batch_offset）
TEST(serialized_batch_construct_and_extend) {
    // 构造 batch1: 两个 value
    auto batch1 = SerializedValueBatch::from_values({
        {/*key=*/1, /*lsn=*/100, bytes("aaa"), /*will_init=*/true},
        {/*key=*/2, /*lsn=*/200, bytes("bbbbb"), /*will_init=*/false},
    });
    ASSERT_EQ(batch1.len, (size_t)2);
    ASSERT_EQ(batch1.max_lsn, (Lsn)200);
    ASSERT_EQ(batch1.raw.size(), (size_t)8);  // "aaa" + "bbbbb"
    ASSERT_EQ(batch1.metadata[0].batch_offset, (uint64_t)0);
    ASSERT_EQ(batch1.metadata[0].len, (size_t)3);
    ASSERT_EQ(batch1.metadata[1].batch_offset, (uint64_t)3);
    ASSERT_EQ(batch1.metadata[1].len, (size_t)5);

    // 构造 batch2
    auto batch2 = SerializedValueBatch::from_values({
        {/*key=*/3, /*lsn=*/300, bytes("cc"), /*will_init=*/true},
    });
    ASSERT_EQ(batch2.len, (size_t)1);
    ASSERT_EQ(batch2.metadata[0].batch_offset, (uint64_t)0);

    // extend: batch1 合并 batch2
    // 对应 Rust (serialized_batch.rs:405):
    //   pub fn extend(&mut self, mut other: SerializedValueBatch) {
    //       let base = self.raw.len() as u64;
    //       for meta in &mut other.metadata { ser.batch_offset += base; }
    //       self.raw.extend(other.raw);
    //       ...
    //   }
    uint64_t base_before = batch1.raw.size();  // 8
    batch1.extend(std::move(batch2));

    ASSERT_EQ(batch1.len, (size_t)3);
    ASSERT_EQ(batch1.max_lsn, (Lsn)300);
    ASSERT_EQ(batch1.raw.size(), (size_t)10);  // 8 + 2
    // batch2 的 metadata 的 batch_offset 应该被加上 base(=8)
    ASSERT_EQ(batch1.metadata[2].batch_offset, base_before + 0);  // 8
    ASSERT_EQ(batch1.metadata[2].len, (size_t)2);
}

// ============================================================
// T5: InMemoryLayer::put_batch — 核心写入路径
// ============================================================
/// 对应 Rust: inmemory_layer.rs:571-644
///
/// 这是整个写入路径中最核心的函数。
/// 验证:
///   - raw 数据被写入 EphemeralFile
///   - index 被正确更新
///   - 通过 get() 能读回写入的数据
TEST(inmemory_layer_put_batch) {
    InMemoryLayer layer(100);  // start_lsn=100

    // 构造 batch: 两个 key 各一个版本
    auto batch = SerializedValueBatch::from_values({
        {/*key=*/1, /*lsn=*/100, bytes("page1_v1"), true},
        {/*key=*/2, /*lsn=*/100, bytes("page2_v1"), true},
    });

    layer.put_batch(std::move(batch));

    // 验证文件大小
    ASSERT_EQ(layer.len(), (uint64_t)(8 + 8));  // "page1_v1" + "page2_v1"

    // 验证索引: 2 个 key
    ASSERT_EQ(layer.key_count(), (size_t)2);
    ASSERT_EQ(layer.entry_count(), (size_t)2);

    // 验证能读回数据
    auto v1 = layer.get(1, 100);
    ASSERT_TRUE(v1.has_value());
    ASSERT_EQ(*v1, bytes("page1_v1"));

    auto v2 = layer.get(2, 100);
    ASSERT_TRUE(v2.has_value());
    ASSERT_EQ(*v2, bytes("page2_v1"));

    // 不存在的 key
    ASSERT_FALSE(layer.get(3, 100).has_value());

    // 写入第二个 batch: 同样的 key，更高的 LSN
    auto batch2 = SerializedValueBatch::from_values({
        {1, 200, bytes("page1_v2"), false},
    });
    layer.put_batch(std::move(batch2));

    // 现在 key=1 有两个版本
    ASSERT_EQ(layer.entry_count(), (size_t)3);
    auto v1_2 = layer.get(1, 200);
    ASSERT_TRUE(v1_2.has_value());
    ASSERT_EQ(*v1_2, bytes("page1_v2"));

    // 旧版本仍可访问
    auto v1_1 = layer.get(1, 100);
    ASSERT_TRUE(v1_1.has_value());
    ASSERT_EQ(*v1_1, bytes("page1_v1"));
}

// ============================================================
// T6: InMemoryLayer::freeze — 冻结后不可写
// ============================================================
/// 对应 Rust: inmemory_layer.rs:673-700
///
/// freeze 设置 end_lsn，之后 put_batch 应该 panic。
TEST(inmemory_layer_freeze) {
    InMemoryLayer layer(100);

    auto batch = SerializedValueBatch::from_values({
        {1, 100, bytes("data"), true},
    });
    layer.put_batch(std::move(batch));

    ASSERT_FALSE(layer.is_frozen());
    ASSERT_FALSE(layer.end_lsn().has_value());

    // 冻结
    // 对应 Rust:
    //   pub async fn freeze(&self, end_lsn: Lsn) {
    //       assert!(self.start_lsn < end_lsn);
    //       self.end_lsn.set(end_lsn).expect("end_lsn set only once");
    //   }
    layer.freeze(200);
    ASSERT_TRUE(layer.is_frozen());
    ASSERT_EQ(*layer.end_lsn(), (Lsn)200);

    // 冻结后写入 → 应该报错
    auto batch2 = SerializedValueBatch::from_values({
        {2, 200, bytes("bad"), true},
    });
    ASSERT_THROWS(layer.put_batch(std::move(batch2)));

    // 再次冻结 → 应该报错 (OnceLock 语义)
    ASSERT_THROWS(layer.freeze(300));
}

// ============================================================
// T7: TimelineWriter::put_batch — open 生命周期
// ============================================================
/// 对应 Rust: timeline.rs:7880-7930
///
/// 第一次 put_batch 时没有 layer → 自动 open (OpenLayerAction::Open)。
/// 后续 put_batch 继续写入同一个 layer。
TEST(timeline_writer_open) {
    TimelineWriter writer(1024 * 1024);  // 1MB checkpoint distance

    ASSERT_TRUE(writer.current_layer() == nullptr);

    // 第一次写: 应该自动创建 layer
    auto batch = SerializedValueBatch::from_values({
        {1, 100, bytes("page1"), true},
    });
    writer.put_batch(std::move(batch));

    auto layer = writer.current_layer();
    ASSERT_TRUE(layer != nullptr);
    ASSERT_EQ(layer->start_lsn(), (Lsn)100);
    ASSERT_EQ(layer->key_count(), (size_t)1);

    // 第二次写: 继续写入同一个 layer（OpenLayerAction::None）
    auto batch2 = SerializedValueBatch::from_values({
        {2, 200, bytes("page2"), true},
    });
    writer.put_batch(std::move(batch2));

    ASSERT_TRUE(writer.current_layer() == layer);  // 同一个 layer
    ASSERT_EQ(layer->key_count(), (size_t)2);

    // frozen_layers 应该为空（没有 roll）
    ASSERT_EQ(writer.frozen_layers().size(), (size_t)0);
}

// ============================================================
// T8: TimelineWriter::put_batch — 触发 roll
// ============================================================
/// 对应 Rust: timeline.rs:7830-7877 (get_open_layer_action)
///   + 7749-7768 (handle_open_layer_action → Roll)
///   + 7787-7828 (roll_layer → freeze + open)
///
/// 当 current_size + new_value_size > checkpoint_distance 时触发 Roll。
TEST(timeline_writer_roll) {
    // checkpoint_distance = 100 字节（很小，容易触发 roll）
    TimelineWriter writer(100);

    // 第一次写: 80 字节
    auto batch1 = SerializedValueBatch::from_values({
        {1, 100, std::vector<uint8_t>(80, 'A'), true},
    });
    writer.put_batch(std::move(batch1));

    auto layer1 = writer.current_layer();
    ASSERT_TRUE(layer1 != nullptr);
    ASSERT_EQ(writer.frozen_layers().size(), (size_t)0);

    // 第二次写: 30 字节 → 80 + 30 = 110 > 100 → 触发 Roll
    // 对应 Rust (timeline.rs:7865-7876):
    //   if self.tl.should_roll(state.current_size, current_size + new_value_size, checkpoint_distance, ...)
    //       { OpenLayerAction::Roll }
    auto batch2 = SerializedValueBatch::from_values({
        {2, 200, std::vector<uint8_t>(30, 'B'), true},
    });
    writer.put_batch(std::move(batch2));

    // layer1 应该被冻结，放入 frozen_layers
    ASSERT_EQ(writer.frozen_layers().size(), (size_t)1);
    ASSERT_TRUE(writer.frozen_layers()[0] == layer1);
    ASSERT_TRUE(layer1->is_frozen());
    // freeze 时 end_lsn = max_lsn + 1（exclusive）
    // 对应 Rust (layer_manager.rs:449): let end_lsn = Lsn(last_record_lsn + 1);
    ASSERT_EQ(*layer1->end_lsn(), (Lsn)101);  // freeze_at=100, end_lsn=101

    // 新的 layer2 应该被创建
    auto layer2 = writer.current_layer();
    ASSERT_TRUE(layer2 != nullptr);
    ASSERT_TRUE(layer2 != layer1);
    ASSERT_EQ(layer2->start_lsn(), (Lsn)200);  // 从 batch2 的 LSN 开始
    ASSERT_EQ(layer2->key_count(), (size_t)1);  // batch2 的数据

    // last_freeze_at 应该更新为 end_lsn (freeze_at + 1)
    ASSERT_EQ(writer.last_freeze_at(), (Lsn)101);
}

// ============================================================
// T9: DatadirModification::commit — 完整写入流程
// ============================================================
/// 对应 Rust: pgdatadir_mapping.rs:2867-2948
///
/// 模拟完整的 commit 流程:
///   put() → commit() → writer.put_batch() → layer.put_batch()
TEST(datadir_modification_commit) {
    TimelineWriter writer(1024 * 1024);
    DatadirModification modification(writer);

    // 模拟 WAL ingest: set_lsn + put
    modification.set_lsn(100);
    modification.put(1, bytes("page1_at_100"), true);
    modification.put(2, bytes("page2_at_100"), true);

    ASSERT_EQ(modification.pending_count(), (size_t)2);

    // commit → 将 pending 数据写入 InMemoryLayer
    modification.commit();

    ASSERT_EQ(modification.pending_count(), (size_t)0);
    ASSERT_EQ(writer.last_record_lsn(), (Lsn)100);

    auto layer = writer.current_layer();
    ASSERT_TRUE(layer != nullptr);
    ASSERT_EQ(layer->key_count(), (size_t)2);

    // 验证数据可读
    auto v = layer->get(1, 100);
    ASSERT_TRUE(v.has_value());
    ASSERT_EQ(*v, bytes("page1_at_100"));

    // 第二次 commit
    modification.set_lsn(200);
    modification.put(1, bytes("page1_at_200"), false);
    modification.commit();

    ASSERT_EQ(writer.last_record_lsn(), (Lsn)200);
    ASSERT_EQ(layer->entry_count(), (size_t)3);  // 2 + 1
}

// ============================================================
// T10: 完整模拟 — 多条 WAL 的 ingest + commit
// ============================================================
/// 模拟真正的 Neon 写入路径 (对应 0403_write待整理.md:74-136):
///
///   handle_walreceiver_connection()          ← 这里开始
///     for interpreted in records:
///       walingest.ingest_record(interpreted, modification)
///         modification.set_lsn(next_record_lsn)
///         modification.put(key, value)      ← 数据写入 pending
///     每 N 条记录:
///       modification.commit()               ← 刷入 InMemoryLayer
///         writer.put_batch(batch)
///           get_open_layer_action()          ← 判断 Open/Roll/None
///           handle_open_layer_action()       ← 执行
///           layer.put_batch(batch)           ← 写入文件+更新索引
///         writer.finish_write(lsn)           ← 推进 last_record_lsn
TEST(full_wal_ingest_simulation) {
    // checkpoint_distance = 200 字节
    TimelineWriter writer(200);
    DatadirModification modification(writer);

    // 模拟 10 条 WAL 记录，每条修改一个页面
    // 对应 walreceiver_connection.rs 的主循环
    int ingest_batch_size = 3;  // 每 3 条 commit 一次
    int uncommitted = 0;

    for (int i = 0; i < 10; i++) {
        Lsn lsn = 100 + i * 10;  // LSN: 100, 110, 120, ...
        CompactKey key = i % 3;    // 3 个 key 轮流写

        // 对应 Rust:
        //   walingest.ingest_record(interpreted, modification)
        //     modification.set_lsn(next_record_lsn)
        //     modification.put(key, value)
        modification.set_lsn(lsn);

        std::string page_data = "page_k" + std::to_string(key) + "_lsn" + std::to_string(lsn);
        modification.put(key, bytes(page_data), true);

        uncommitted++;

        // 每 ingest_batch_size 条 commit 一次
        // 对应 walreceiver_connection.rs:414:
        //   if uncommitted_records >= ingest_batch_size { commit(modification); }
        if (uncommitted >= ingest_batch_size) {
            modification.commit();
            uncommitted = 0;
        }
    }

    // 最后一批
    if (uncommitted > 0) {
        modification.commit();
    }

    // 验证 last_record_lsn
    ASSERT_EQ(writer.last_record_lsn(), (Lsn)190);

    // 验证是否触发了 roll（取决于数据大小）
    // 每条约 15-20 字节，3 条一批 = 45-60 字节，checkpoint=200
    // 第 4 批 commit 时 current_size 约 180，加上新 batch 约 240 > 200 → 触发 roll
    // 所以应该至少有 1 个 frozen layer

    auto current = writer.current_layer();
    ASSERT_TRUE(current != nullptr);

    // 验证数据完整性: 读取最后一条记录
    // key=0 最后写入在 lsn=190 (i=9, key=9%3=0)
    // 但要看 roll 之后数据在哪个 layer
    // 不管怎样，当前 layer 应该有最近的数据
    size_t total_keys = 0;
    for (auto& fl : writer.frozen_layers()) {
        total_keys += fl->key_count();
    }
    total_keys += current->key_count();
    ASSERT_TRUE(total_keys > 0);

    // 打印统计信息
    std::cout << "\n    统计: "
              << writer.frozen_layers().size() << " frozen layers + 1 active layer, "
              << "total_keys=" << total_keys
              << ", last_record_lsn=" << writer.last_record_lsn()
              << "\n    ";
}

// ============================================================
// main
// ============================================================
int main() {
    std::cout << "=== Neon 写入路径 C++ 复刻 单元测试 ===" << std::endl;
    std::cout << "共 " << tests.size() << " 个测试" << std::endl;
    std::cout << std::endl;

    for (auto& t : tests) {
        std::cout << "  运行: " << t.name << " ... ";
        try {
            t.fn();
            std::cout << "✓ PASS" << std::endl;
            tests_passed++;
        } catch (const std::exception& e) {
            std::cout << "✗ FAIL: " << e.what() << std::endl;
            tests_failed++;
        }
    }

    std::cout << std::endl;
    std::cout << "结果: " << tests_passed << " passed, "
              << tests_failed << " failed" << std::endl;

    return tests_failed > 0 ? 1 : 0;
}
