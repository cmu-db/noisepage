#include "catalog/type_handle.h"
#include <memory>
#include <utility>

namespace terrier::catalog {
TypeHandle::TypeHandle(Catalog *catalog, std::shared_ptr<catalog::SqlTableRW> pg_type)
    : pg_type_rw_(std::move(pg_type)) {}

std::shared_ptr<TypeHandle::TypeEntry> TypeHandle::GetTypeEntry(transaction::TransactionContext *txn, type_oid_t oid) {
  // TODO(yeshengm) implement this once we've determine where pg_type should go
  return nullptr;
}

}  // namespace terrier::catalog
