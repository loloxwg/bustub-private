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
    : AbstractExecutor(exec_ctx), plan_(plan) {
  table_info_ = exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid());
  table_iterator_ = table_info_->table_->Begin(exec_ctx->GetTransaction());
}

/**
 * Initialize the sequential scan
 * Already initialized in constructor
 * So do nothing here
 */
void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (table_iterator_ != table_info_->table_->End()) {
    *tuple = *table_iterator_;
    *rid = table_iterator_->GetRid();
    ++table_iterator_;
    return true;
  }
  return false;
}

}  // namespace bustub
