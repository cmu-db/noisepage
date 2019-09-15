#pragma once

#include <memory>
#include <vector>
#include "catalog/catalog_defs.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/projected_columns_iterator.h"
#include "storage/storage_defs.h"

namespace terrier::execution::sql{

class EXPORT Inserter {
 public:
  explicit Inserter(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid);

  storage::ProjectedRow *GetTablePR();

  storage::ProjectedRow *GetIndexPR(catalog::index_oid_t index_oid);

 private:
  terrier::storage::SqlTable *table;
};

} // namespace terrier::execution::sql