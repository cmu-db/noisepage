#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/namespace_handle.h"
#include "catalog/schema.h"
#include "loggers/main_logger.h"
#include "storage/block_layout.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"

namespace terrier::catalog {

std::shared_ptr<NamespaceHandle::NamespaceEntry> NamespaceHandle::GetNamespaceEntry(
    transaction::TransactionContext *txn, nsp_oid_t oid) {
  // TODO(yangjun): we can cache this
  std::vector<col_oid_t> cols;
  for (const auto &c : pg_namespace_->GetSchema().GetColumns()) {
    cols.emplace_back(c.GetOid());
  }
  auto row_pair = pg_namespace_->InitializerForProjectedRow(cols);
  auto read_buffer = common::AllocationUtil::AllocateAligned(row_pair.first.ProjectedRowSize());
  storage::ProjectedRow *read = row_pair.first.InitializeRow(read_buffer);
  // Find the row using sequential scan
  auto tuple_iter = pg_namespace_->begin();
  for (; tuple_iter != pg_namespace_->end(); tuple_iter++) {
    pg_namespace_->Select(txn, *tuple_iter, read);
    if ((*reinterpret_cast<nsp_oid_t *>(read->AccessForceNotNull(row_pair.second[cols[0]]))) == oid) {
      return std::make_shared<NamespaceEntry>(oid, read, row_pair.second);
    }
  }
  delete[] read_buffer;
  return nullptr;
}

}  // namespace terrier::catalog
