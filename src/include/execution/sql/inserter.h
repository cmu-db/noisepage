#pragma once

#include <memory>
#include <vector>
#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "storage/storage_defs.h"

namespace terrier::execution::sql {

/**
 * Allows insertion from TPL.
 */
class EXPORT Inserter {
 public:
  /**
   * Constructor
   * @param exec_ctx The execution context
   * @param table_oid The oid of the table to insert into
   */
  explicit Inserter(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid);

  /**
   * @return The table's projected row.
   */
  terrier::storage::ProjectedRow *GetTablePR();

  /**
   * @param index_oid oid of the index to access
   * @return The projected row of the index.
   */
  storage::ProjectedRow *GetIndexPR(catalog::index_oid_t index_oid);

  /**
   * Insert a tuple into the table
   * @return The tuple slot of the inserted tuple.
   */
  storage::TupleSlot TableInsert();

  /**
   * Insert into the index
   * @param index_oid oid of the index to insert into.
   * @return Whether insertion was successful.
   */
  bool IndexInsert(catalog::index_oid_t index_oid);

 private:
  terrier::common::ManagedPointer<terrier::storage::index::Index> GetIndex(terrier::catalog::index_oid_t index_oid);

  catalog::table_oid_t table_oid_;
  exec::ExecutionContext *exec_ctx_;
  std::vector<terrier::catalog::col_oid_t> col_oids_;
  common::ManagedPointer<terrier::storage::SqlTable> table_;

  storage::TupleSlot table_tuple_slot_;
  storage::RedoRecord *table_redo_{nullptr};

  storage::ProjectedRow *table_pr_{nullptr};

  void *index_pr_buffer_;
  storage::ProjectedRow *index_pr_{nullptr};

  std::map<terrier::catalog::index_oid_t, common::ManagedPointer<storage::index::Index>> index_cache_;
};

}  // namespace terrier::execution::sql