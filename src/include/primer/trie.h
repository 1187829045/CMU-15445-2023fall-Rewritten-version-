#pragma once

#include <algorithm>
#include <cstddef>
#include <future>  // NOLINT
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace bustub {

/// 一个特殊的类型，将阻止移动构造函数和移动赋值运算符。用于 TrieStore 测试。
class MoveBlocked {
 public:
  explicit MoveBlocked(std::future<int> wait) : wait_(std::move(wait)) {}

  MoveBlocked(const MoveBlocked &) = delete;  // 禁用复制构造函数
  MoveBlocked(MoveBlocked &&that) noexcept {  // 移动构造函数
    if (!that.waited_) {
      that.wait_.get();
    }
    that.waited_ = waited_ = true;
  }

  auto operator=(const MoveBlocked &) -> MoveBlocked & = delete;  // 禁用复制赋值运算符
  auto operator=(MoveBlocked &&that) noexcept -> MoveBlocked & {  // 移动赋值运算符
    if (!that.waited_) {
      that.wait_.get();
    }
    that.waited_ = waited_ = true;
    return *this;
  }

  bool waited_{false};
  std::future<int> wait_;
};

// TrieNode 是前缀树中的一个节点
class TrieNode {
 public:
  // 创建一个没有子节点的 TrieNode
  TrieNode() = default;

  // 创建一个带有一些子节点的 TrieNode
  explicit TrieNode(std::map<char, std::shared_ptr<const TrieNode>> children) : children_(std::move(children)) {}

  virtual ~TrieNode() = default;

  // Clone 返回这个 TrieNode 的一个副本。如果 TrieNode 有一个值，值也会被复制。返回
  // 这个函数的返回类型是一个指向 TrieNode 的 unique_ptr。
  // 你不能使用复制构造函数来克隆节点，因为它不知道 `TrieNode` 是否包含值。
  // 注意：如果你想将 `unique_ptr` 转换为 `shared_ptr`，你可以使用 `std::shared_ptr<T>(std::move(ptr))`。
  virtual auto Clone() const -> std::unique_ptr<TrieNode> { return std::make_unique<TrieNode>(children_); }

  // 一个子节点的映射，键是键中的下一个字符，值是下一个 TrieNode。
  // 你必须在这个结构中存储子节点信息。你不允许移除结构中的 `const`。
  std::map<char, std::shared_ptr<const TrieNode>> children_;

  // 指示节点是否为终端节点
  bool is_value_node_{false};

  // 你可以在这里添加额外的字段和方法，除了存储子节点。但是一般来说，你不需要添加额外的字段来完成这个项目。
};

// TrieNodeWithValue 是一个 TrieNode，它还具有与其关联的类型 T 的值。
template <class T>
class TrieNodeWithValue : public TrieNode {
 public:
  // 创建一个没有子节点且有一个值的 TrieNode
  explicit TrieNodeWithValue(std::shared_ptr<T> value) : value_(std::move(value)) { this->is_value_node_ = true; }

  // 创建一个带有子节点且有一个值的 TrieNode
  TrieNodeWithValue(std::map<char, std::shared_ptr<const TrieNode>> children, std::shared_ptr<T> value)
      : TrieNode(std::move(children)), value_(std::move(value)) {
    this->is_value_node_ = true;
  }

  // 重写 Clone 方法以克隆值。
  //
  // 注意：如果你想将 `unique_ptr` 转换为 `shared_ptr`，你可以使用 `std::shared_ptr<T>(std::move(ptr))`。
  auto Clone() const -> std::unique_ptr<TrieNode> override {
    return std::make_unique<TrieNodeWithValue<T>>(children_, value_);
  }

  // 与这个 TrieNode 关联的值
  std::shared_ptr<T> value_;
};

// Trie 是一种数据结构，它将字符串映射到类型 T 的值。所有对 Trie 的操作都不应该修改 trie 本身。
// 它应该尽可能重用现有节点，并创建新节点来表示新的 trie。
//
// 你不允许在这个项目中移除任何 `const`，或使用 `mutable` 来绕过 const 检查。
class Trie {
 private:
  // Trie 的根节点
  std::shared_ptr<const TrieNode> root_{nullptr};

  // 创建一个具有给定根节点的新 trie
  explicit Trie(std::shared_ptr<const TrieNode> root) : root_(std::move(root)) {}

 public:
  // 创建一个空的 trie
  Trie() = default;

  // 获取与给定键关联的值
  // 1. 如果键不在 trie 中，返回 nullptr。
  // 2. 如果键在 trie 中但类型不匹配，返回 nullptr。
  // 3. 否则，返回值。
  template <class T>
  auto Get(std::string_view key) const -> const T *;

  // 将新的键值对放入 trie。如果键已存在，覆盖值。
  // 返回新的 trie。
  template <class T>
  auto Put(std::string_view key, T value) const -> Trie;

  // 从 trie 中移除键。如果键不存在，返回原始的 trie。
  // 否则，返回新的 trie。
  auto Remove(std::string_view key) const -> Trie;

  // 获取 trie 的根节点，仅应在测试用例中使用。
  auto GetRoot() const -> std::shared_ptr<const TrieNode> { return root_; }
};

}  // namespace bustub
