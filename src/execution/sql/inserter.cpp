
#include <execution/sql/inserter.h>

#include "execution/sql/inserter.h"
terrier::execution::sql::Inserter::Inserter(terrier::execution::exec::ExecutionContext *exec_ctx,
                                            terrier::catalog::table_oid_t table_oid) {

}
terrier::storage::ProjectedRow *terrier::execution::sql::Inserter::GetTablePR() {
  return nullptr;
}
terrier::storage::ProjectedRow *terrier::execution::sql::Inserter::GetIndexPR(terrier::catalog::index_oid_t index_oid) {
  return nullptr;
}
