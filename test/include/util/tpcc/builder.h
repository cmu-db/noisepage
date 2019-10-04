#pragma once

#include <utility>
#include "catalog/catalog_defs.h"
#include "catalog/schema.h"
#include "common/macros.h"
#include "storage/index/index_builder.h"
#include "storage/index/index_defs.h"
#include "storage/sql_table.h"
#include "util/tpcc/database.h"
#include "util/tpcc/schemas.h"

namespace terrier::tpcc {

/**
 * Builds all of the tables and indexes for TPCC, and returns them in a Database object
 */
class Builder {
 public:
  explicit Builder(storage::BlockStore *const store)
      : store_(store),
        oid_counter_(1)  // 0 is a reserved oid in the catalog, so we'll start at 1 for our counter
  {}
  Database *Build(storage::index::IndexType index_type);

 private:
  storage::index::Index *BuildIndex(const catalog::IndexSchema &key_schema) {
    storage::index::IndexBuilder index_builder;
    index_builder.SetKeySchema(key_schema);
    return index_builder.Build();
  }

  storage::BlockStore *const store_;
  uint32_t oid_counter_;
};
}  // namespace terrier::tpcc
