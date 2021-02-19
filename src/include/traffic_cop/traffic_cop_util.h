#pragma once

#include <memory>
#include <string>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "network/network_defs.h"
#include "optimizer/optimize_result.h"

namespace noisepage::catalog {
class CatalogAccessor;
}

namespace noisepage::parser {
class ParseResult;
class SQLStatement;
class SelectStatement;
}  // namespace noisepage::parser

namespace noisepage::planner {
class AbstractPlanNode;
}

namespace noisepage::optimizer {
class StatsStorage;
class AbstractCostModel;
class PropertySet;
}  // namespace noisepage::optimizer

namespace noisepage::transaction {
class TransactionContext;
}

namespace noisepage::trafficcop {

/**
 * Static helper methods for accessing some of the TrafficCop's functionality without instantiating an object
 */
class TrafficCopUtil {
 public:
  TrafficCopUtil() = delete;

  /**
   * @param txn used by optimizer
   * @param accessor used by optimizer
   * @param query bound ParseResult
   * @param db_oid database oid
   * @param stats_storage used by optimizer
   * @param cost_model used by optimizer
   * @param optimizer_timeout used by optimizer
   * @return physical plan that can be executed
   */
  static std::unique_ptr<optimizer::OptimizeResult> Optimize(
      common::ManagedPointer<transaction::TransactionContext> txn,
      common::ManagedPointer<catalog::CatalogAccessor> accessor, common::ManagedPointer<parser::ParseResult> query,
      catalog::db_oid_t db_oid, common::ManagedPointer<optimizer::StatsStorage> stats_storage,
      std::unique_ptr<optimizer::AbstractCostModel> cost_model, uint64_t optimizer_timeout);

  /**
   * Converts parser statement types (which rely on multiple enums) to a single QueryType enum from the network layer
   * @param statement
   * @return
   */
  static network::QueryType QueryTypeForStatement(common::ManagedPointer<parser::SQLStatement> statement);

 private:
  static void CollectSelectProperties(common::ManagedPointer<parser::SelectStatement> sel_stmt,
                                      optimizer::PropertySet *property_set);
};

}  // namespace noisepage::trafficcop
