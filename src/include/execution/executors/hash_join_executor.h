#include <memory>
#include <utility>

#include "aggregation_executor.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/** HashJoinKey 表示连接操作中的键 */
struct HashJoinKey {
  std::vector<Value> hash_keys_;
  /**
   * Compares two hash joi keys for equality
   * @param other the other hash join key to be compared with
   * @return `true` if both hash join key have equivalent values
   */
  auto operator==(const HashJoinKey &other) const -> bool {
    // 比较两个对象的hash_keys_成员中的每个Value对象是否相等
    for (uint32_t i = 0; i < other.hash_keys_.size(); ++i) {
      if (hash_keys_[i].CompareEquals(other.hash_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};
}  // namespace bustub

namespace std {
/** 在 AggregateKey 上实现 std::hash */
template <>
struct hash<bustub::HashJoinKey> {
  auto operator()(const bustub::HashJoinKey &join_key) const -> std::size_t {
    size_t curr_hash = 0;
    for (const auto &key : join_key.hash_keys_) {
      if (!key.IsNull()) {
        // 对每一个非空的value对象，计算出它的哈希值
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {
/**
 * 一个简化的哈希表，具有连接所需的所有必要功能。
 */
class SimpleHashJoinHashTable {
 public:
  /** 插入join key和tuple构建hash表 */
  void InsertKey(const HashJoinKey &join_key, const Tuple &tuple) {
    if (ht_.count(join_key) == 0) {
      std::vector<Tuple> tuple_vector;
      tuple_vector.push_back(tuple);
      ht_.insert({join_key, tuple_vector});
    } else {
      ht_.at(join_key).push_back(tuple);
    }
  }
  /** 获取该join key对应的tuple */
  auto GetValue(const HashJoinKey &join_key) -> std::vector<Tuple> * {
    if (ht_.find(join_key) == ht_.end()) {
      return nullptr;
    }
    return &(ht_.find(join_key)->second);
  }

  /**
   * Clear the hash table
   */
  void Clear() { ht_.clear(); }

 private:
  /** 哈希表只是从聚合键到聚合值的映射*/
  std::unordered_map<HashJoinKey, std::vector<Tuple>> ht_{};
};
/**
 *HashJoinExecutor 在两个表上执行嵌套循环 JOIN。
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
* 构造一个新的 HashJoinExecutor 实例。
* @param exec_ctx 执行器上下文
* @param plan 要执行的 HashJoin 连接计划
* @param left_child 为连接的左侧生成元组的子执行器
* @param right_child 为连接的右侧生成元组的子执行器
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  auto GetLeftJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(tuple, left_child_->GetOutputSchema()));
    }
    return {values};
  }

  auto GetRightJoinKey(const Tuple *tuple) -> HashJoinKey {
    std::vector<Value> values;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      values.emplace_back(expr->Evaluate(tuple, right_child_->GetOutputSchema()));
    }
    return {values};
  }
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  // 遍历哈希表的迭代器
  std::vector<Tuple>::iterator jht_iterator_;
  // 哈希表
  std::unique_ptr<SimpleHashJoinHashTable> jht_;
  // 指向左表的执行器对象
  std::unique_ptr<AbstractExecutor> left_child_;
  // 指向右表的执行器对象
  std::unique_ptr<AbstractExecutor> right_child_;
  Tuple left_tuple_{};
  RID left_rid_{};
  std::vector<Tuple> *right_tuple_{nullptr};
  bool has_done_;
  // 用来判断左边还有没有符合要求的元组
  bool left_bool_;
};

}  // namespace bustub
