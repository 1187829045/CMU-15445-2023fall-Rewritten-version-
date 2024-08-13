//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "execution/expressions/constant_value_expression.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}
void IndexScanExecutor::Init() {
  // 获取当前执行上下文中的 catalog 对象，并通过 `plan_` 中的 `index_oid_` 获取索引信息。
  IndexInfo *index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->index_oid_);

  // 将索引信息转换为 `HashTableIndexForTwoIntegerColumn` 类型的指针，方便进行哈希索引操作。
  auto *hash_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info->index_.get());

  // 清空存储记录标识符 (RIDs) 的向量，以确保没有残留数据。
  rids_.clear();

  // 如果 `plan_` 中存在过滤谓词（`filter_predicate_`），则进行索引扫描。
  if (plan_->filter_predicate_ != nullptr) {

    // 将过滤谓词的右子表达式（通常是常量值）转换为 `ConstantValueExpression` 类型的指针。
    const auto right_exptr = dynamic_cast<ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get());

    // 获取常量值 `v`，即谓词中要查找的值。
    Value v = right_exptr->val_;

    // 使用哈希索引的 `ScanKey` 函数查找所有符合条件的记录标识符 (RIDs)。
    // 通过构造一个带有单个值 `v` 的元组以及索引的键模式来进行查找，并将结果存入 `rids_` 向量中。
    hash_index->ScanKey(Tuple{{v}, index_info->index_->GetKeySchema()}, &rids_, GetExecutorContext()->GetTransaction());
  }

  // 将 `rids_iter_` 设置为 `rids_` 向量的起始迭代器，准备后续的迭代操作。
  rids_iter_ = rids_.begin();
}


auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  IndexInfo *index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid());
  TableInfo *table_info = GetExecutorContext()->GetCatalog()->GetTable(index_info->table_name_);

  TupleMeta meta{};
  do {
    if (rids_iter_ == rids_.end()) {
      return false;
    }
    *rid = *rids_iter_;
    meta = table_info->table_->GetTupleMeta(*rid);
    *tuple = table_info->table_->GetTuple(*rid).second;
    rids_iter_++;
  } while (meta.is_deleted_);

  return true;
}

}  // namespace bustub
