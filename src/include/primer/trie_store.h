#pragma once

#include <optional>
#include <shared_mutex>
#include <utility>

#include "primer/trie.h"

namespace bustub {

// 这个类用于保护从 trie 中返回的值。它持有对根的引用，以便值的引用不会被使无效。
template <class T>
class ValueGuard {
 public:
  ValueGuard(Trie root, const T &value) : root_(std::move(root)), value_(value) {}
  auto operator*() const -> const T & { return value_; }

 private:
  Trie root_;
  const T &value_;
};

// 这个类是对 Trie 类的线程安全封装。它提供了一个简单的接口来访问 trie。它允许并发读取，并且在同一时间只允许一个写操作。
class TrieStore {
 public:
  // 这个函数返回一个 ValueGuard 对象，该对象持有对 trie 中值的引用。如果 key 不存在于 trie 中，将返回 std::nullopt。
  template <class T>
  auto Get(std::string_view key) -> std::optional<ValueGuard<T>>;

  // 这个函数将键值对插入到 trie 中。如果键已经存在于 trie 中，它将覆盖现有值。
  template <class T>
  void Put(std::string_view key, T value);

  // 这个函数将从 trie 中移除键值对。
  void Remove(std::string_view key);

 private:
  // 这个互斥锁保护根节点。每次你想要访问或修改 trie 根节点时，你都需要获取这个锁。
  std::mutex root_lock_;

  // 这个互斥锁序列化所有写操作，只允许在同一时间进行一个写操作。
  std::mutex write_lock_;

  // 存储当前的 trie 根节点。
  Trie root_;
};

}  // namespace bustub
