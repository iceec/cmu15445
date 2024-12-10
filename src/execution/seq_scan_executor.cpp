//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_info_(), table_iterator_() {}

void SeqScanExecutor::Init() {
  auto exec = GetExecutorContext();                                  // 获取到执行器执行器里面有table
  table_info_ = exec->GetCatalog()->GetTable(plan_->GetTableOid());  // 拿到table_info
  if (table_info_) {
    table_iterator_.emplace(table_info_->table_->MakeIterator());
  }
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!table_info_) {
    return false;
  }

  while (!table_iterator_->IsEnd()) {
    auto iter_rid = table_iterator_->GetRID();
    auto meta = table_info_->table_->GetTupleMeta(iter_rid);
    if (!meta.is_deleted_) {
      *tuple = table_iterator_->GetTuple().second;
      *rid = iter_rid;
      ++(table_iterator_.value());

      if (plan_->filter_predicate_) {
        auto express_value = plan_->filter_predicate_->Evaluate(tuple, table_info_->schema_);
        if (express_value.GetTypeId() == TypeId::BOOLEAN && express_value.GetAs<bool>() == false) {
          continue;
        }
      }
      return true;
    }

    ++(table_iterator_.value());
  }

  return false;
}

}  // namespace bustub
