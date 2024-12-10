//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), inserted_(false), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();  // 孩子初始化
}  // namespace bustub
// 这个是物化模型 seqcan是迭代器模型
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (inserted_) {
    return false;
  }
  inserted_ = true;

  // 拿到表和index
  auto exec = GetExecutorContext();
  auto table_info = exec->GetCatalog()->GetTable(plan_->GetTableOid());  // 获取表的信息
  std::vector<std::shared_ptr<IndexInfo>> index_info;
  if (table_info) {
    index_info = exec->GetCatalog()->GetTableIndexes(table_info->name_);  // 获取index信息
  } else {
    return false;
  }

  Tuple insert_tuple;
  RID insert_rid_may_unused;
  TupleMeta insert_meta{0, false};
  std::vector<uint32_t> index_key_attr(index_info.size());
  int32_t count = 0;

  // 拿到next
  while (child_executor_->Next(&insert_tuple, &insert_rid_may_unused)) {
    // 插入进去table当中
    auto insert_rid = table_info->table_->InsertTuple(insert_meta, insert_tuple, exec->GetLockManager(),
                                                      exec->GetTransaction(), plan_->GetTableOid());

    // 插入table失败了
    if (!insert_rid) {
      continue;
    }
    ++count;
    for (auto &index : index_info) {
      // 生成key_tuple 供index插入
      auto key_tuple = insert_tuple.KeyFromTuple(table_info->schema_, index->key_schema_, index->index_->GetKeyAttrs());

      index->index_->InsertEntry(key_tuple, insert_rid.value(), exec->GetTransaction());
    }
  }
  // 输出
  std::vector<Value> result(1);
  result[0] = Value(TypeId::INTEGER, count);

  *tuple = Tuple(result, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub
