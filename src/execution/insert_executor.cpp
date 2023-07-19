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

#include "common/logger.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->TableOid());
}

void InsertExecutor::Init() {}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_success_) {
    return false;
  }
  // record the number of inserted rows
  int count = 0;
  while (child_->Next(tuple, rid)) {
    if (table_info_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
      count++;
      // update index
      auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto index : indexes) {
        auto key = tuple->KeyFromTuple(child_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs());
        // LOG_DEBUG("schema: %s", plan_->OutputSchema().ToString().c_str());
        // LOG_DEBUG("schema: %s", child_->GetOutputSchema().ToString().c_str());
        // LOG_DEBUG("schema: %s", index->key_schema_.ToString().c_str());
        index->index_->InsertEntry(key, *rid, exec_ctx_->GetTransaction());
      }
    }
  }
  // return the number of inserted rows
  std::vector<Value> values;
  values.emplace_back(INTEGER, count);
  *tuple = Tuple(values, &plan_->OutputSchema());
  is_success_ = true;
  return true;
}

}  // namespace bustub
