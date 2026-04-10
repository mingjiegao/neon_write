#pragma once
/// vec_map.h — VecMap: 有序追加映射
///
/// 对应 Rust: libs/utils/src/vec_map.rs
///
/// VecMap 是一个基于 Vec 的有序映射，只允许追加比当前最大 key 更大（或相等）的元素。
/// 在 InMemoryLayer 中，每个 CompactKey 对应一个 VecMap<Lsn, IndexEntry>，
/// 记录该 key 在各个 LSN 处的页面版本。
///
/// Rust 原文 (vec_map.rs:11-21):
///   /// Ordered map datastructure implemented in a Vec.
///   /// Append only - can only add keys that are larger than the
///   /// current max key.
///   pub struct VecMap<K, V> {
///       data: Vec<(K, V)>,
///       ordering: VecMapOrdering,
///   }

#include <vector>
#include <optional>
#include <stdexcept>
#include <utility>

/// 对应 Rust: VecMap<K, V> (libs/utils/src/vec_map.rs:18)
///
/// 只允许追加比当前最大 key 更大的元素（append-only）。
/// 如果 key == 当前最大 key，则更新最后一个元素。
template <typename K, typename V>
class VecMap {
public:
    /// 对应 Rust: VecMap::default()
    VecMap() = default;

    /// 对应 Rust: VecMap::append_or_update_last (vec_map.rs:104-123)
    ///
    /// 追加新的 (key, value) 或更新最后一个元素。
    /// 返回被替换的旧值（如果是 update）。
    ///
    /// Rust 原文:
    ///   pub fn append_or_update_last(&mut self, key: K, mut value: V)
    ///       -> Result<(Option<V>, usize), VecMapError>
    ///   {
    ///       if let Some((last_key, last_value)) = self.data.last_mut() {
    ///           match key.cmp(last_key) {
    ///               Ordering::Less => return Err(VecMapError::InvalidKey),
    ///               Ordering::Equal => { std::mem::swap(last_value, &mut value); return Ok((Some(value), 0)); }
    ///               Ordering::Greater => {}
    ///           }
    ///       }
    ///       self.data.push((key, value));
    ///       Ok((None, delta_size))
    ///   }
    std::optional<V> append_or_update_last(const K& key, V value) {
        if (!data_.empty()) {
            auto& [last_key, last_value] = data_.back();
            if (key < last_key) {
                throw std::runtime_error("VecMap: key violates ordering constraint");
            }
            if (key == last_key) {
                // Rust: Ordering::Equal → swap
                V old = std::move(last_value);
                last_value = std::move(value);
                return old;
            }
            // key > last_key → fall through to push
        }
        data_.emplace_back(key, std::move(value));
        return std::nullopt;
    }

    /// 元素数量
    size_t size() const { return data_.size(); }
    bool empty() const { return data_.empty(); }

    /// 迭代器访问
    auto begin() const { return data_.begin(); }
    auto end() const { return data_.end(); }
    auto begin() { return data_.begin(); }
    auto end() { return data_.end(); }

    /// 直接访问底层数据
    const std::vector<std::pair<K, V>>& data() const { return data_; }

private:
    std::vector<std::pair<K, V>> data_;
};
