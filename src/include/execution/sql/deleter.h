#pragma once

#include <map>
#include <string>
#include "execution/exec/execution_context.h"
#include "execution/util/execution_common.h"

namespace terrier::execution::sql {

/**
 * Helper class to perform deletes in SQL Tables.
 */
class EXPORT Deleter {
 public:
  /**
   * Constructor
   * @param exec_ctx The execution context.
   * @param table_oid Oid of the table
   */
  Deleter(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid);

  /**
   * Destructor.
   */
  ~Deleter();

  /**
   * @param index_oid OID of the index to access.
   * @return PR of the index.
   */
  storage::ProjectedRow *GetIndexPR(catalog::index_oid_t index_oid);

  /**
   * Delete slot from the table.
   * @param tuple_slot slot to delete.
   * @return Whether the deletion was successful.
   */
  bool TableDelete(storage::TupleSlot tuple_slot);

  /**
   * Delete item from an index.
   * @param index_oid oid of the index.
   * @param table_tuple_slot slot corresponding to the item.
   */
  void IndexDelete(catalog::index_oid_t index_oid, storage::TupleSlot table_tuple_slot);

 private:
  common::ManagedPointer<storage::index::Index> GetIndex(catalog::index_oid_t index_oid);

  catalog::table_oid_t table_oid_;
  exec::ExecutionContext *exec_ctx_;
  common::ManagedPointer<terrier::storage::SqlTable> table_;

  void *index_pr_buffer_;
  uint32_t max_pr_size_;
  storage::ProjectedRow *index_pr_{nullptr};

  std::map<terrier::catalog::index_oid_t, common::ManagedPointer<storage::index::Index>> index_cache_;
};
}  // namespace terrier::execution::sql
