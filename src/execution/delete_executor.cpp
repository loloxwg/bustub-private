//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

void DeleteExecutor::Init() {}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_success_) {
    return false;
  }
  // record the number of deleted rows
  int count = 0;
  while (child_executor_->Next(tuple, rid)) {
    if (table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      count++;
      // update index
      auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto index : indexes) {
        auto key =
            tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(key, *rid, exec_ctx_->GetTransaction());
      }
    }
  }
  // return the number of deleted rows
  std::vector<Value> values;
  values.emplace_back(INTEGER, count);
  *tuple = Tuple(values, &plan_->OutputSchema());
  is_success_ = true;
  return true;
}

}  // namespace bustub
