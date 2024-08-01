#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {
// 你需要遍历前缀树以找到与键对应的节点。如果节点不存在，则返回nullptr。
// 找到节点后，你应该使用`dynamic_cast`将其转换为`const TrieNodeWithValue<T> *`。
// 如果`dynamic_cast`返回`nullptr`，这意味着值的类型不匹配，你应该返回nullptr。
// 否则，返回该值。
template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  if(root_== nullptr){
    return nullptr;
  }
  auto itr = root_;
  for (char ch : key) {
    // 如果当前节点为nullptr，或者没有对应字符的子节点，返回nullptr，程序结束
    if (itr == nullptr || itr->children_.find(ch) == itr->children_.end()) {
      return nullptr;
    }
    // 当前字符存在，接着往后面找
    itr = itr->children_.at(ch);
  }
  const auto *value_node = dynamic_cast<const TrieNodeWithValue<T> *>(itr.get());
  if (value_node != nullptr) {
    return value_node->value_.get();
  }
  return nullptr;
}

template <class T>
void PutCycle(const std::shared_ptr<bustub::TrieNode> &new_root, std::string_view key, T value) {
  // 判断元素是否为空的标志位
  bool flag = false;
  // 在new_root的children找key的第一个元素
  // 利用for(auto &a:b)循环体中修改a，b中对应内容也会修改及pair的特性
  for (auto &pair : new_root->children_) {
    // 如果找到了
    if (key.at(0) == pair.first) {
      flag = true;
      // 剩余键长度大于1
      if (key.size() > 1) {
        // 复制一份找到的子节点，然后递归对其写入
        std::shared_ptr<TrieNode> ptr = pair.second->Clone();
        // 递归写入 .substr(1,key.size()-1)也可以
        // 主要功能是复制子字符串，要求从指定位置开始，并具有指定的长度。
        PutCycle<T>(ptr, key.substr(1), std::move(value));
        // 覆盖原本的子节点
        pair.second = std::shared_ptr<const TrieNode>(ptr);
      } else {
        // 剩余键长度小于等于1，则直接插入
        // 创建新的带value的子节点
        std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
        TrieNodeWithValue node_with_val(pair.second->children_, val_p);
        // 覆盖原本的子节点
        pair.second = std::make_shared<const TrieNodeWithValue<T>>(node_with_val);
      }
      return;
    }
  }
  if (!flag) {
    // 没找到，则新建一个子节点
    char c = key.at(0);
    // 如果为键的最后一个元素
    if (key.size() == 1) {
      // 直接插入children
      std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
      new_root->children_.insert({c, std::make_shared<const TrieNodeWithValue<T>>(val_p)});
    } else {
      // 创建一个空的children节点
      auto ptr = std::make_shared<TrieNode>();
      // 递归
      PutCycle<T>(ptr, key.substr(1), std::move(value));
      // 插入
      new_root->children_.insert({c, std::move(ptr)});
    }
  }
}
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // 注意，`T`可能是不可复制的类型。在创建`shared_ptr`时总是使用`std::move`来处理该值。
  // 第一种情况：值要放在根节点中，无根造根，有根放值
  // 更改根节点的值，即测试中的 trie = trie.Put<uint32_t>("", 123);key为空
  if (key.empty()) {
    std::shared_ptr<T> val_p = std::make_shared<T>(std::move(value));
    // 建立新根
    std::unique_ptr<TrieNodeWithValue<T>> new_root = nullptr;
    // 如果原根节点无子节点
    if (root_->children_.empty()) {
      // 直接修改根节点
      new_root = std::make_unique<TrieNodeWithValue<T>>(std::move(val_p));
    } else {
      new_root = std::make_unique<TrieNodeWithValue<T>>(root_->children_, std::move(val_p));
    }
    // 返回新的Trie
    return Trie(std::move(new_root));
  }
  //你需要遍历前缀树并在必要时创建新节点。如果与键对应的节点已经存在，你应该创建一个新的`TrieNodeWithValue`。
  // 第二种情况：值不放在根节点中
  // 2.1 根节点如果为空，新建一个空的TrieNode;
  // 2.2 如果不为空，调用clone方法复制根节点
  std::shared_ptr<TrieNode> new_root = nullptr;
  if (root_ == nullptr) {
    new_root = std::make_unique<TrieNode>();
  } else {
    new_root = root_->Clone();
  }
  // 递归插入，传递根节点，要放的路径：key, 要放入的值：value
  PutCycle<T>(new_root, key, std::move(value));
  // 返回新的Trie
  return Trie(std::move(new_root));
}
//remove 递归移除方法
bool RemoveCycle(const std::shared_ptr<TrieNode> &new_roottry, std::string_view key) {
  // 在new_root的children找key的第一个元素
  for (auto &pair : new_roottry->children_) {
    // 继续找
    if (key.at(0) != pair.first) {
      continue;
    }
    if (key.size() == 1) {
      // 是键结尾
      if (!pair.second->is_value_node_) {
        return false;
      }
      // 如果子节点为空，直接删除
      if (pair.second->children_.empty()) {
        new_roottry->children_.erase(pair.first);
      } else {
        // 否则转为TireNode
        pair.second = std::make_shared<const TrieNode>(pair.second->children_);
      }
      return true;
    }
    // 拷贝一份当前节点
    std::shared_ptr<TrieNode> ptr = pair.second->Clone();
    // 递归删除
    bool flag = RemoveCycle(ptr, key.substr(1, key.size() - 1));
    // 如果没有可删除的键
    if (!flag) {
      return false;
    }
    // 如果删除后当前节点无value且子节点为空，则删除
    if (ptr->children_.empty() && !ptr->is_value_node_) {
      new_roottry->children_.erase(pair.first);
    } else {
      // 否则将删除的子树覆盖原来的子树
      pair.second = std::shared_ptr<const TrieNode>(ptr);
    }
    return true;
  }
  return false;
}

auto Trie::Remove(std::string_view key) const -> Trie {
  if (this->root_ == nullptr) {
    return *this;
  }
  // 键为空
  if (key.empty()) {
    // 根节点有value
    if (root_->is_value_node_) {
      // 根节点无子节点
      if (root_->children_.empty()) {
        // 直接返回一个空的trie
        return Trie(nullptr);
      }
      // 根节点有子节点,把子结点转给新根
      std::shared_ptr<TrieNode> new_root = std::make_shared<TrieNode>(root_->children_);
      return Trie(new_root);
    }
    // 根节点无value，直接返回
    return *this;
  }
  // 创建一个当前根节点的副本作为新的根节点
  std::shared_ptr<TrieNode> newroot = root_->Clone();
  // 递归删除
  bool flag = RemoveCycle(newroot, key);
  if (!flag) {
    return *this;
  }
  if (newroot->children_.empty() && !newroot->is_value_node_) {
    newroot = nullptr;
  }
  return Trie(std::move(newroot));

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
}

// 下面是模板函数的显式实例化。
// 通常，人们会在头文件中编写模板类和函数的实现。然而，我们将实现分离到.cpp文件中，以使事情更清晰。
// 为了让编译器知道模板函数的实现，我们需要在这里显式实例化它们，以便链接器可以找到它们。

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// 如果你的解决方案无法通过不可复制测试的编译，你可以删除下面的行以获得部分分数。

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
