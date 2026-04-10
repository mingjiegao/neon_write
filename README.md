# Neon 写入路径 C++ 复刻

将 Neon Pageserver 的 **WAL 写入路径**用 C++ 严格复刻，用于学习理解。每个数据结构和函数都 1:1 对齐 Neon Rust 源码。

## 构建与运行

```bash
cd ~/neon_write
make && ./test_neon_write
```

---

## 一、Neon 写入路径全景

### 1.1 真实的调用链

```
handle_walreceiver_connection()                    // walreceiver_connection.rs:116
│
├─ physical_stream.next()                          // 从 Safekeeper 接收 WAL 流
│
├─ timeline.begin_modification(Lsn(0))             // pgdatadir_mapping.rs:217
│
├─ for interpreted in records:                     // 逐条处理 WAL 记录
│    │
│    ├─ walingest.ingest_record(interpreted, modification)  // walingest.rs:234
│    │    ├─ modification.set_lsn(next_record_lsn)
│    │    └─ modification.put(key, value)           // 暂存到 pending
│    │
│    └─ uncommitted_records += 1
│
├─ 每 ingest_batch_size 条:
│    modification.commit()                          // pgdatadir_mapping.rs:2867
│    │
│    ├─ 合并 pending_metadata + pending_data        // 构建 SerializedValueBatch
│    │
│    ├─ writer.put_batch(batch)                     // timeline.rs:7880
│    │    ├─ get_open_layer_action()                 // 判断: None / Open / Roll
│    │    ├─ handle_open_layer_action()              // 执行: 创建/冻结 layer
│    │    └─ layer.put_batch(batch)                  // inmemory_layer.rs:571
│    │         ├─ file.write_raw(&raw)               // 追加写入 EphemeralFile
│    │         └─ index.write() → 更新 BTreeMap      // 更新内存索引
│    │
│    ├─ writer.finish_write(lsn)                    // 推进 last_record_lsn
│    └─ writer.update_current_logical_size()
│
└─ 循环继续接收下一批 WAL
```

### 1.2 C++ 复刻中的对应关系

| 调用链层级 | Neon Rust 位置 | C++ 文件 | 测试覆盖 |
|-----------|---------------|---------|---------|
| WAL 攒批 + 提交 | `pgdatadir_mapping.rs:2867` | `datadir_modification.h` | T9, T10 |
| Layer 生命周期调度 | `timeline.rs:7880` | `timeline_writer.h` | T7, T8 |
| 写入文件 + 更新索引 | `inmemory_layer.rs:571` | `inmemory_layer.h` | T5, T6 |
| 序列化批次传输 | `serialized_batch.rs:100` | `serialized_batch.h` | T4 |
| 追加写入临时文件 | `EphemeralFile` | `ephemeral_file.h` | T3 |
| 索引条目编码 | `inmemory_layer.rs:115` | `index_entry.h` | T2 |
| 有序追加映射 | `vec_map.rs:18` | `vec_map.h` | T1 |

---

## 二、数据结构详解

### 2.1 VecMap — 有序追加映射

```
C++ 文件:  vec_map.h
Rust 源码: libs/utils/src/vec_map.rs:18
```

**在 Neon 中的位置**：`InMemoryLayer::index` 的内层：

```
index: BTreeMap<CompactKey, VecMap<Lsn, IndexEntry>>
                             ^^^^^^^^^^^^^^^^^^^^^^^^
                             每个 page key 的版本历史
```

**核心特性**：只允许追加比当前最大 key 更大的元素。同 key 则更新最后一个。

```
Rust (vec_map.rs:104-123):                 C++:
append_or_update_last(key, value)          append_or_update_last(key, value)
  Less    → Err(InvalidKey)                  key < last → throw
  Equal   → swap, return old                 key == last → swap, return old
  Greater → push                             key > last → push
```

**为什么用 VecMap 而不是 BTreeMap？**
同一个 page key 的 LSN 版本天然递增（WAL 就是按 LSN 顺序来的），
Vec 的 append 比 BTreeMap 的 insert 更省内存、更快。

### 2.2 IndexEntry — 索引条目

```
C++ 文件:  index_entry.h
Rust 源码: inmemory_layer.rs:106-160
```

**在 Neon 中**：IndexEntry 是一个 bit-packed 的 u64，编码三个字段：

```
Rust 实际布局 (64 bits):
┌─────────┬───────────────────────┬──────────────────────────────────┐
│will_init│        len            │              pos                 │
│ (1 bit) │   (~27 bits)          │          (~36 bits)              │
└─────────┴───────────────────────┴──────────────────────────────────┘

C++ 简化为 struct:
struct IndexEntry {
    uint64_t pos;       // = base_offset + batch_offset
    size_t   len;
    bool     will_init;
};
```

**pos 的计算**（`inmemory_layer.rs:617-622`）：

```
pos = base_offset + batch_offset
      ^^^^^^^^^^^   ^^^^^^^^^^^^
      put_batch 前   该条目在
      文件的大小     batch.raw 中的偏移
```

### 2.3 EphemeralFile — 追加写入临时文件

```
C++ 文件:  ephemeral_file.h
Rust 源码: pageserver/src/virtual_file/ (多个文件)
```

**在 Neon 中**：真正的磁盘文件，通过 VirtualFile 实现异步 I/O。

**关键设计**（`inmemory_layer.rs:73-77` 注释）：

```
"the file backing InMemoryLayer::file is append-only,
 so it is not necessary to hold a lock on the index while reading or writing from the file."

即：文件只追加，读写不需要同步。这是 Neon 高性能的关键之一。
写入时：先写文件，再更新索引（不需要同时锁两者）。
读取时：先读索引得到 pos+len，释放索引锁，再读文件。
```

C++ 简化为内存中的 `vector<uint8_t>`。

### 2.4 SerializedValueBatch — 序列化批次

```
C++ 文件:  serialized_batch.h
Rust 源码: libs/wal_decoder/src/serialized_batch.rs:100-117
```

**在 Neon 中**：WAL 记录解码后的传输格式。多个 (key, lsn, value) 打包为一个批次。

```
SerializedValueBatch:
┌─────────────────────────────────────────────────┐
│ raw: [bytes_of_val1 | bytes_of_val2 | ...]      │  所有 value 的字节拼接
│                                                   │
│ metadata: [                                       │  每个 value 的元信息
│   { key=1, lsn=100, batch_offset=0,  len=8 },   │  ← 指向 raw[0..8)
│   { key=2, lsn=100, batch_offset=8,  len=5 },   │  ← 指向 raw[8..13)
│   { key=3, lsn=200, batch_offset=13, len=4 },   │  ← 指向 raw[13..17)
│ ]                                                 │
│ max_lsn: 200                                      │
│ len: 3                                            │
└─────────────────────────────────────────────────┘
```

**extend 合并**（`serialized_batch.rs:405`）：

```
batch1.raw = [aaa|bbbbb]     metadata: [{offset=0,len=3}, {offset=3,len=5}]
batch2.raw = [cc]             metadata: [{offset=0,len=2}]

batch1.extend(batch2):
  base = batch1.raw.len() = 8
  batch2 的 metadata 的 offset 都 += base:  offset 0 → 8
  拼接 raw: [aaa|bbbbb|cc]
  合并 metadata: [{0,3}, {3,5}, {8,2}]
```

### 2.5 InMemoryLayer — 内存层（核心）

```
C++ 文件:  inmemory_layer.h
Rust 源码: inmemory_layer.rs:47-84
```

**InMemoryLayer 是写入路径的终点。所有 WAL 数据最终都写入这里。**

```
Rust 结构 (inmemory_layer.rs:47-84):
pub struct InMemoryLayer {
    start_lsn: Lsn,                                           // 起始 LSN（inclusive）
    end_lsn: OnceLock<Lsn>,                                   // 结束 LSN（exclusive，冻结时设置）
    index: RwLock<BTreeMap<CompactKey, VecMap<Lsn, IndexEntry>>>,  // 内存索引
    file: EphemeralFile,                                       // 数据文件
    estimated_in_mem_size: AtomicU64,                           // 估算的内存占用
}
```

**内部数据组织图**：

```
InMemoryLayer
│
├── start_lsn = 100
├── end_lsn = None (未冻结) 或 Some(201) (已冻结)
│
├── file (EphemeralFile):     追加写入的字节流
│   ┌─────────────────────────────────────────────────────┐
│   │ [page1_v1 的字节] [page2_v1 的字节] [page1_v2 的字节] │
│   │  pos=0, len=8      pos=8, len=8      pos=16, len=8   │
│   └─────────────────────────────────────────────────────┘
│
└── index (BTreeMap):
    │
    ├── CompactKey=1 → VecMap:
    │     [(Lsn=100, IndexEntry{pos=0, len=8, will_init=true}),
    │      (Lsn=200, IndexEntry{pos=16, len=8, will_init=false})]
    │
    └── CompactKey=2 → VecMap:
          [(Lsn=100, IndexEntry{pos=8, len=8, will_init=true})]
```

**put_batch 流程**（`inmemory_layer.rs:571-644`）：

```
put_batch(SerializedValueBatch { raw, metadata }) {
    ① base_offset = file.len()              // 记录写入前文件大小
    ② file.write_raw(&raw)                  // 追加写入整个 raw
    ③ for meta in metadata:                 // 逐条更新索引
        entry = IndexEntry::new(base_offset + meta.batch_offset, meta.len, ...)
        index[meta.key].append_or_update_last(meta.lsn, entry)
}
```

**生命周期: Open → Write → Freeze**

```
 ┌─────────────┐    put_batch()     ┌──-───────────┐    freeze(end_lsn)    ┌───────-───────┐
 │   Created    │ ──────────────→   │   Writing    │ ──────────────────→   │   Frozen      │
 │ end_lsn=None│                    │ end_lsn=None │                       │ end_lsn=Some  │
 └─────────────┘                    └──-───────────┘                       └─────-─────────┘
                                    可以继续 put_batch                      不可写入
                                                                           等待 flush 到磁盘
```

### 2.6 TimelineWriter — 写入调度器

```
C++ 文件:  timeline_writer.h
Rust 源码: timeline.rs:7728-7930
```

**TimelineWriter 管理 InMemoryLayer 的生命周期，决定何时 open/roll/freeze。**

```
Rust 结构:
pub(crate) struct TimelineWriter<'a> {
    tl: &'a Timeline,                                              // 关联的 Timeline
    write_guard: MutexGuard<'a, Option<TimelineWriterState>>,       // 持有写锁
}

struct TimelineWriterState {
    open_layer: Arc<InMemoryLayer>,    // 当前活跃的 layer
    current_size: u64,                 // 当前 layer 已写入的字节数
    prev_lsn: Option<Lsn>,            // 上一次写入的 LSN
    max_lsn: Option<Lsn>,             // 通过此 writer 的最大 LSN
    cached_last_freeze_at: Lsn,        // 缓存的上次冻结位置
}
```

**put_batch 决策流程**（`timeline.rs:7880-7930`）：

```
put_batch(batch):
    ① action = get_open_layer_action(batch.max_lsn, batch.buffer_size())
    ② layer = handle_open_layer_action(batch.max_lsn, action)
    ③ layer.put_batch(batch)
    ④ state.current_size += buf_size
       state.prev_lsn = batch.max_lsn
       state.max_lsn = max(state.max_lsn, batch.max_lsn)
```

**get_open_layer_action 的判断逻辑**（`timeline.rs:7830-7877`）：

```
get_open_layer_action(lsn, new_value_size):
    if state == None:
        return Open                        // 没有 layer，需要创建
    if state.prev_lsn == lsn:
        return None                        // 同一个 LSN 内不 roll
    if state.current_size == 0:
        return None                        // 空 layer 不 roll
    if current_size + new_value_size > checkpoint_distance:
        return Roll                        // 超过阈值，需要 roll
    return None                            // 继续使用当前 layer
```

**Roll 的完整流程**：

```
Roll:
    freeze_at = state.max_lsn                              // timeline.rs:7757
    end_lsn = freeze_at + 1                                // layer_manager.rs:449（exclusive）
    state.open_layer.freeze(end_lsn)                       // 冻结当前 layer
    frozen_layers.push_back(open_layer)                    // 放入冻结队列
    last_freeze_at = end_lsn                               // layer_manager.rs:473
    state = None                                           // 清空 writer state
    → 然后 open_layer(at) 创建新的 layer
```

### 2.7 DatadirModification — 修改上下文

```
C++ 文件:  datadir_modification.h
Rust 源码: pgdatadir_mapping.rs:1676-1709, 2867-2948
```

**DatadirModification 是"修改事务"，攒批后一次性提交。**

```
Rust 结构:
pub struct DatadirModification<'a> {
    tline: &'a Timeline,
    pending_metadata_pages: HashMap<CompactKey, Vec<(Lsn, usize, Value)>>,  // 元数据
    pending_data_batch: Option<SerializedValueBatch>,                        // 数据页
    pending_lsns: Vec<Lsn>,                                                 // 待提交的 LSN
    lsn: Lsn,                                                               // 当前 LSN
    ...
}
```

**commit 流程**（`pgdatadir_mapping.rs:2867-2948`）：

```
commit():
    ① metadata_batch = SerializedValueBatch::from_values(pending_metadata_pages.drain())
    ② data_batch = pending_data_batch.take()
    ③ maybe_batch = match (data_batch, metadata_batch):
          (Some(data), Some(meta)) → data.extend(meta); Some(data)
          (Some(data), None)       → Some(data)
          (None, Some(meta))       → Some(meta)
          (None, None)             → None
    ④ if maybe_batch.is_some():
          writer.put_batch(batch)          // → TimelineWriter → InMemoryLayer
    ⑤ for lsn in pending_lsns:
          writer.finish_write(lsn)         // 推进 last_record_lsn
```

---

## 三、数据结构嵌套关系

```
DatadirModification                    (pgdatadir_mapping.rs:1676)
 │  攒批: pending_metadata + pending_data
 │
 └─ commit() ──→ TimelineWriter        (timeline.rs:7728)
                  │
                  ├─ state: TimelineWriterState
                  │   ├─ open_layer ──→ InMemoryLayer   (inmemory_layer.rs:47)
                  │   │                  │
                  │   │                  ├─ file: EphemeralFile     追加写入的字节流
                  │   │                  │
                  │   │                  └─ index: BTreeMap<CompactKey, VecMap<Lsn, IndexEntry>>
                  │   │                               │                  │         │
                  │   │                               │                  │    IndexEntry
                  │   │                               │                  │    {pos, len, will_init}
                  │   │                               │                  │
                  │   │                               │                VecMap
                  │   │                               │              (vec_map.rs:18)
                  │   │                               │           追加有序的 (Lsn, IndexEntry) 对
                  │   │                               │
                  │   │                             CompactKey (i128 → int64_t)
                  │   │
                  │   ├─ current_size: u64
                  │   ├─ prev_lsn, max_lsn
                  │   └─ cached_last_freeze_at
                  │
                  └─ frozen_layers: Vec<InMemoryLayer>     已冻结待 flush 的 layers
```

---

## 四、每个测试用例详解

### T1: VecMap 基础 (`vecmap_basic`)

```
测试层级:  最底层 — 内存数据结构
对应 Rust:  libs/utils/src/vec_map.rs:104 (append_or_update_last)
```

**测试什么**：VecMap 是 InMemoryLayer 索引的内层容器，每个 page key 的所有 LSN 版本存在一个 VecMap 里。

**测试场景**：

| 操作 | 输入 | 预期结果 | 对应 Rust 行为 |
|------|------|---------|---------------|
| 首次追加 | `(100, "v1")` | 返回 None, size=1 | `data.push((key, value))` |
| 追加更大 key | `(200, "v2")` | 返回 None, size=2 | `data.push((key, value))` |
| 相同 key 更新 | `(200, "v2_updated")` | 返回 `Some("v2")`, size=2 | `swap(last_value, value)` |
| 违反顺序 | `(100, "bad")` | 抛异常 | `Err(InvalidKey)` |

**在 Neon 中的调用位置**：

```
InMemoryLayer::put_batch (inmemory_layer.rs:624-625):
    let vec_map = index.entry(key).or_default();
    vec_map.append_or_update_last(lsn, index_entry);  ← 就是这里
```

---

### T2: IndexEntry 创建 (`index_entry_create`)

```
测试层级:  最底层 — 索引条目编码
对应 Rust:  inmemory_layer.rs:153-160 (IndexEntry::new)
```

**测试什么**：验证 `pos = base_offset + batch_offset` 的计算。

```
场景: 文件当前大小 1000, batch 中偏移 50, 长度 200

  EphemeralFile:
  ┌───────────────────────┬──────────────────────────────────┐
  │  已有数据 (1000 bytes) │  新写入的 batch                    │
  │                        │  ┌─────────┬──────────┬────┐    │
  │                        │  │ 其他数据  │ 该条目数据 │ ...│    │
  │                        │  │ (50 B)   │ (200 B)  │    │    │
  │                        │  └─────────┴──────────┴────┘    │
  └────────────────────────┴──────────────────────────────────┘
  ↑                         ↑            ↑
  pos=0                   base=1000    pos=1050 (base+batch_offset)
```

---

### T3: EphemeralFile 追加写入 (`ephemeral_file_basic`)

```
测试层级:  底层 — 文件 I/O
对应 Rust:  EphemeralFile (VirtualFile 系列)
```

**测试什么**：追加写入 + 按位置读取。

```
操作序列:
  write_raw("hello")  →  file: [h,e,l,l,o]          len=5
  write_raw("world")  →  file: [h,e,l,l,o,w,o,r,l,d]  len=10
  read(0, 5)          →  "hello"  ✓
  read(5, 5)          →  "world"  ✓
  read(8, 5)          →  越界 → 抛异常
```

---

### T4: SerializedValueBatch 构造 + extend (`serialized_batch_construct_and_extend`)

```
测试层级:  中间层 — 数据传输格式
对应 Rust:  serialized_batch.rs:100-117, 405
```

**测试什么**：batch 的构造和合并。这对应 `commit()` 中 `data_batch.extend(metadata_batch)` 的逻辑。

```
batch1 = from_values([(key=1, lsn=100, "aaa"), (key=2, lsn=200, "bbbbb")])

  batch1.raw:      [a,a,a,b,b,b,b,b]     (8 bytes)
  batch1.metadata: [{key=1, offset=0, len=3},
                    {key=2, offset=3, len=5}]

batch2 = from_values([(key=3, lsn=300, "cc")])

  batch2.raw:      [c,c]                  (2 bytes)
  batch2.metadata: [{key=3, offset=0, len=2}]

batch1.extend(batch2):
  base = 8
  batch2 的 offset 全部 += 8:  0 → 8
  raw:      [a,a,a,b,b,b,b,b,c,c]         (10 bytes)
  metadata: [{key=1, offset=0, len=3},
             {key=2, offset=3, len=5},
             {key=3, offset=8, len=2}]     ← offset 被调整了
```

---

### T5: InMemoryLayer::put_batch (`inmemory_layer_put_batch`)

```
测试层级:  核心 — InMemoryLayer 写入
对应 Rust:  inmemory_layer.rs:571-644
```

**测试什么**：写入路径中最核心的函数。验证数据写入文件和索引更新。

**场景 1: 首次写入两个 key**

```
layer = InMemoryLayer(start_lsn=100)

put_batch({
    (key=1, lsn=100, "page1_v1"),
    (key=2, lsn=100, "page2_v1"),
})

执行过程 (对应 inmemory_layer.rs:571-644):
  ① base_offset = file.len() = 0
  ② file.write_raw("page1_v1" + "page2_v1")  →  file 大小 = 16
  ③ 遍历 metadata:
     key=1: entry = {pos=0+0=0, len=8}   → index[1].append(lsn=100, entry)
     key=2: entry = {pos=0+8=8, len=8}   → index[2].append(lsn=100, entry)

写入后状态:
  file:  [p,a,g,e,1,_,v,1,p,a,g,e,2,_,v,1]   (16 bytes)
  index: { 1 → [(100, {pos=0,  len=8})],
           2 → [(100, {pos=8,  len=8})] }
```

**场景 2: 同一 key 写入新版本**

```
put_batch({ (key=1, lsn=200, "page1_v2") })

执行过程:
  ① base_offset = 16
  ② file.write_raw("page1_v2")  →  file 大小 = 24
  ③ key=1: entry = {pos=16, len=8}  → index[1].append(lsn=200, entry)

写入后状态:
  file:  [...原有 16 bytes...|p,a,g,e,1,_,v,2]   (24 bytes)
  index: { 1 → [(100, {pos=0, len=8}), (200, {pos=16, len=8})],   ← 两个版本
           2 → [(100, {pos=8, len=8})] }

验证: layer.get(1, 100) = "page1_v1"  ✓  (旧版本仍可访问)
      layer.get(1, 200) = "page1_v2"  ✓  (新版本)
```

---

### T6: InMemoryLayer::freeze (`inmemory_layer_freeze`)

```
测试层级:  核心 — Layer 生命周期
对应 Rust:  inmemory_layer.rs:673-700
```

**测试什么**：冻结后的 layer 不接受写入。对应 Neon 中 layer 从 open → frozen 的状态转换。

```
layer = InMemoryLayer(start_lsn=100)
layer.put_batch(...)           ← 正常写入
layer.freeze(200)              ← 冻结，end_lsn = 200
layer.put_batch(...)           ← 抛异常: "InMemoryLayer is frozen"
layer.freeze(300)              ← 抛异常: "end_lsn set only once" (OnceLock 语义)
```

**在 Neon 中**：冻结由 `try_freeze_in_memory_layer`（`layer_manager.rs:441`）触发，
冻结后 layer 进入 `frozen_layers` 队列，等待 flush 线程将其写入磁盘。

---

### T7: TimelineWriter::put_batch — Open 生命周期 (`timeline_writer_open`)

```
测试层级:  调度层 — Layer 自动创建
对应 Rust:  timeline.rs:7880-7930 (put_batch)
           timeline.rs:7830-7877 (get_open_layer_action)
```

**测试什么**：首次写入时自动创建 InMemoryLayer (OpenLayerAction::Open)，后续写入复用同一个 layer。

```
writer = TimelineWriter(checkpoint_distance=1MB)

writer.current_layer() == nullptr           // 初始无 layer

writer.put_batch(batch1)                    // 触发 OpenLayerAction::Open
  → get_open_layer_action: state==None → Open
  → handle_open_layer_action: open_layer(lsn=100)
    → 创建 InMemoryLayer(start_lsn=100)
  → layer.put_batch(batch1)

writer.current_layer() != nullptr           // layer 已创建
writer.frozen_layers().size() == 0          // 没有冻结

writer.put_batch(batch2)                    // 触发 OpenLayerAction::None
  → get_open_layer_action: current_size < checkpoint → None
  → 继续写入同一个 layer

writer.current_layer() == layer             // 仍是同一个 layer
layer.key_count() == 2                      // 两个 batch 的数据都在
```

---

### T8: TimelineWriter::put_batch — 触发 Roll (`timeline_writer_roll`)

```
测试层级:  调度层 — Layer 自动冻结 + 重建
对应 Rust:  timeline.rs:7865-7876 (should_roll 判断)
           timeline.rs:7787-7828 (roll_layer)
           layer_manager.rs:441-484 (try_freeze_in_memory_layer)
```

**测试什么**：当 layer 大小超过 checkpoint_distance 时，自动冻结旧 layer 并创建新的。

```
writer = TimelineWriter(checkpoint_distance=100)

put_batch(80 bytes at lsn=100):
  get_open_layer_action: state==None → Open
  → 创建 layer1 (start_lsn=100), 写入 80 bytes
  → state.current_size = 80

put_batch(30 bytes at lsn=200):
  get_open_layer_action: 80 + 30 = 110 > 100 → Roll!
  → roll_layer(freeze_at=100):
      end_lsn = 100 + 1 = 101          // layer_manager.rs:449
      layer1.freeze(101)                // layer1: [100, 101) 已冻结
      frozen_layers.push(layer1)
      last_freeze_at = 101
      state = None
  → open_layer(lsn=200):
      创建 layer2 (start_lsn=200)
  → layer2.put_batch(30 bytes)

验证:
  frozen_layers = [layer1]              // layer1 被冻结了
  layer1.is_frozen() == true
  layer1.end_lsn() == 101              // exclusive end
  layer2.start_lsn() == 200            // 新 layer 从 200 开始
  layer2 != layer1                      // 确实是新的 layer
  writer.last_freeze_at() == 101
```

**⚠️ 关键细节**：`end_lsn = freeze_at + 1`（`layer_manager.rs:449`）。
freeze_at 是最后写入的 LSN，end_lsn 是 exclusive 上界，所以要 +1。

---

### T9: DatadirModification::commit (`datadir_modification_commit`)

```
测试层级:  最上层 — 完整提交流程
对应 Rust:  pgdatadir_mapping.rs:2867-2948
```

**测试什么**：从 put → commit → put_batch → finish_write 的完整链路。

**⚠️ 与 T8 的关系**：T9 是完全独立的测试。每个 TEST 都创建自己的局部变量，
T8 的 `TimelineWriter writer(100)` 和 T9 的 `TimelineWriter writer(1024*1024)` 是不同的对象，数据不共享。

```
writer = TimelineWriter(1MB)
modification = DatadirModification(writer)

// 第一次 commit
modification.set_lsn(100)
modification.put(1, "page1_at_100")
modification.put(2, "page2_at_100")    pending_count = 2

modification.commit():
  ① metadata_batch = None (没用 put_metadata)
  ② data_batch = pending_data_batch.take()    // 取出 pending
  ③ maybe_batch = Some(data_batch)
  ④ writer.put_batch(data_batch)               // → InMemoryLayer
  ⑤ writer.finish_write(100)                   // last_record_lsn = 100

验证: pending_count = 0,  layer.key_count() = 2,  last_record_lsn = 100

// 第二次 commit
modification.set_lsn(200)
modification.put(1, "page1_at_200")
modification.commit()

验证: layer.entry_count() = 3 (key=1 有两个版本 + key=2 一个版本)
      last_record_lsn = 200
```

---

### T10: 完整模拟 — 多条 WAL 的 ingest (`full_wal_ingest_simulation`)

```
测试层级:  端到端 — 模拟真实 WAL 接收循环
对应 Rust:  walreceiver_connection.rs 主循环 + 所有写入链路
```

**测试什么**：模拟 Neon 从 Safekeeper 接收 10 条 WAL 记录的完整流程。

```
writer = TimelineWriter(checkpoint_distance=200)
modification = DatadirModification(writer)
ingest_batch_size = 3

for i in 0..10:
    lsn = 100 + i*10                      // 100, 110, 120, ..., 190
    key = i % 3                            // 0, 1, 2, 0, 1, 2, 0, 1, 2, 0

    modification.set_lsn(lsn)
    modification.put(key, "page_k{key}_lsn{lsn}")
    uncommitted++

    if uncommitted >= 3:
        modification.commit()              // 每 3 条 commit 一次
        uncommitted = 0

// 最后一批
if uncommitted > 0:
    modification.commit()
```

**commit 时间线**：

```
i=0,1,2:   commit #1 at lsn=120 → batch 包含 3 条 WAL
i=3,4,5:   commit #2 at lsn=150 → batch 包含 3 条 WAL
i=6,7,8:   commit #3 at lsn=180 → batch 包含 3 条 WAL (可能触发 roll)
i=9:       commit #4 at lsn=190 → batch 包含 1 条 WAL
```

**验证**：
- `last_record_lsn == 190`
- 所有 layer 中总 key 数 > 0
- 根据数据大小可能触发 roll，产生 frozen layers

---

## 五、简化说明

| 真实 Neon | C++ 简化 | 原因 |
|-----------|---------|------|
| `CompactKey(i128)` 18字节复合键 | `int64_t` | 测试中 key 值小 |
| `EphemeralFile` 磁盘 I/O | `vector<uint8_t>` 内存 | 不影响逻辑 |
| `IndexEntry` bit-packed u64 | struct {pos,len,will_init} | 可读性优先 |
| `RwLock<BTreeMap>` 异步锁 | `std::map` 无锁 | 单线程测试 |
| `OnceLock<Lsn>` | `optional<Lsn>` + 手动检查 | 语义一致 |
| `ValueMeta::Observed` 变体 | 省略 | 仅 shard 0 需要 |
| `pending_metadata_pages: HashMap` | 展平为 vector | 简化，逻辑一致 |
| 异步 flush 线程 | 同步冻结 | 不影响写入路径逻辑 |
| Compaction backpressure | 省略 | 非写入核心逻辑 |

---

## 六、文件清单

| 文件 | 行数 | 对应 Neon 源码 | 说明 |
|------|------|--------------|------|
| `types.h` | 28 | `utils::lsn`, `key.rs` | Lsn, CompactKey |
| `vec_map.h` | 83 | `libs/utils/src/vec_map.rs` | 追加有序映射 |
| `index_entry.h` | 66 | `inmemory_layer.rs:106-160` | 索引条目 |
| `ephemeral_file.h` | 64 | `virtual_file/` | 追加写入临时文件 |
| `serialized_batch.h` | 126 | `serialized_batch.rs:100` | 序列化批次 |
| `inmemory_layer.h` | 198 | `inmemory_layer.rs:47-644` | 内存层（核心） |
| `timeline_writer.h` | 290 | `timeline.rs:7700-7930` | 写入调度器 |
| `datadir_modification.h` | 178 | `pgdatadir_mapping.rs:1676-2948` | 修改上下文 |
| `test_main.cpp` | 560 | 测试 | 10 个单元测试 |
