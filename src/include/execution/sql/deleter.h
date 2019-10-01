#pragma once
#include "execution/exec/execution_context.h"
#include "execution/util/execution_common.h"

namespace terrier::execution::sql {

/**
 * Helper class to perform deletes in SQL Tables.
 */
class EXPORT Deleter {
 public:
  Deleter(exec::ExecutionContext *exec_ctx, std::string table_name)
      : Deleter(exec_ctx, exec_ctx->GetAccessor()->GetTableOid(table_name)) {}

  Deleter(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid);

  storage::ProjectedRow *GetIndexPR(catalog::index_oid_t index_oid);

  bool TableDelete(storage::TupleSlot tuple_slot);

  void IndexDelete(catalog::index_oid_t index_oid, storage::TupleSlot table_tuple_slot);

 private:
  common::ManagedPointer<storage::index::Index> GetIndex(catalog::index_oid_t index_oid);

  catalog::table_oid_t table_oid_;
  exec::ExecutionContext *exec_ctx_;
  common::ManagedPointer<terrier::storage::SqlTable> table_;

  void *index_pr_buffer_;
  storage::ProjectedRow *index_pr_{nullptr};

  std::map<terrier::catalog::index_oid_t, common::ManagedPointer<storage::index::Index>> index_cache_;
};
}  // namespace terrier::execution::sql