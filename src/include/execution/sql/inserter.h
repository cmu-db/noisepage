#pragma once

#include <memory>
#include <vector>
#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "storage/storage_defs.h"

namespace terrier::execution::sql {

class EXPORT Inserter {
 public:
  explicit Inserter(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid);

  terrier::storage::ProjectedRow *GetTablePR();

  storage::ProjectedRow *GetIndexPR(catalog::index_oid_t index_oid);

  storage::TupleSlot TableInsert();

  bool IndexInsert(catalog::index_oid_t index_oid);

 private:

  terrier::common::ManagedPointer<terrier::storage::index::Index>
      GetIndex(terrier::catalog::index_oid_t index_oid);

  catalog::table_oid_t table_oid_;
  exec::ExecutionContext *exec_ctx_;
  std::vector<terrier::catalog::col_oid_t> col_oids_;
  common::ManagedPointer<terrier::storage::SqlTable> table_;

  storage::TupleSlot table_tuple_slot_;
  storage::RedoRecord *table_redo_{nullptr};

  storage::ProjectedRow *table_pr_{nullptr};

  void *index_pr_buffer_;
  storage::ProjectedRow *index_pr_{nullptr};

  std::map<terrier::catalog::index_oid_t,
  common::ManagedPointer<storage::index::Index>> index_cache_;
};

}  // namespace terrier::execution::sql