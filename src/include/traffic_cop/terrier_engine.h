#pragma once

#include <string>
#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"

namespace terrier::catalog {
class Catalog;
}  // namespace terrier::catalog

namespace terrier::parser {
class PostgresParser;
}  // namespace terrier::parser

namespace terrier::transaction {
class TransactionManager;
}  // namespace terrier::transaction

namespace terrier::trafficcop {

/**
 * The Terrier Engine.
 * [in]Network -> Parser -> Binder -> (MISSING: OPTIMIZER) -> Execution Engine
 */
class TerrierEngine {
 public:
  TerrierEngine(common::ManagedPointer<parser::PostgresParser> parser,
                common::ManagedPointer<transaction::TransactionManager> txn_manager,
                common::ManagedPointer<catalog::Catalog> catalog)
      : parser_(parser), txn_manager_(txn_manager), catalog_(catalog) {}

  void ParseAndBind(catalog::db_oid_t db_oid, const std::string &query);

 private:
  /** Parses SQL statements. */
  common::ManagedPointer<parser::PostgresParser> parser_;
  /** Handles transactions. */
  common::ManagedPointer<transaction::TransactionManager> txn_manager_;
  /** Stores metadata about user tables. */
  common::ManagedPointer<catalog::Catalog> catalog_;
};

}  // namespace terrier::trafficcop
