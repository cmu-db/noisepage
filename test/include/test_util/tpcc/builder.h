#pragma once

#include <utility>

#include "catalog/catalog.h"
#include "catalog/catalog_accessor.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/index/index_builder.h"
#include "storage/index/index_defs.h"
#include "test_util/catalog_test_util.h"
#include "test_util/tpcc/database.h"
#include "test_util/tpcc/schemas.h"

namespace noisepage::tpcc {

/**
 * Builds all of the tables and indexes for TPCC, and returns them in a Database object
 */
class Builder {
 public:
  Builder(const common::ManagedPointer<storage::BlockStore> store,
          const common::ManagedPointer<catalog::Catalog> catalog,
          const common::ManagedPointer<transaction::TransactionManager> txn_manager)
      : store_(store), catalog_(catalog), txn_manager_(txn_manager) {
    NOISEPAGE_ASSERT(store_ != nullptr, "BlockStore cannot be nullptr.");
    NOISEPAGE_ASSERT(catalog_ != nullptr, "Catalog cannot be nullptr.");
    NOISEPAGE_ASSERT(txn_manager_ != nullptr, "TransactionManager cannot be nullptr.");
  }
  Database *Build(storage::index::IndexType index_type);

 private:
  storage::index::Index *BuildIndex(const catalog::IndexSchema &key_schema) {
    storage::index::IndexBuilder index_builder;
    index_builder.SetKeySchema(key_schema);
    return index_builder.Build();
  }

  const common::ManagedPointer<storage::BlockStore> store_;
  const common::ManagedPointer<catalog::Catalog> catalog_;
  const common::ManagedPointer<transaction::TransactionManager> txn_manager_;
};
}  // namespace noisepage::tpcc
