#pragma once

#include <memory>
#include <string>

#include "common/managed_pointer.h"
#include "network/network_defs.h"

namespace terrier::catalog {
class CatalogAccessor;
}

namespace terrier::parser {
class ParseResult;
class SQLStatement;
}  // namespace terrier::parser

namespace terrier::planner {
class AbstractPlanNode;
}

namespace terrier::optimizer {
class StatsStorage;
}

namespace terrier::transaction {
class TransactionContext;
}

namespace terrier::trafficcop {

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
   * @param stats_storage used by optimizer
   * @param optimizer_timeout used by optimizer
   * @return physical plan that can be executed
   */
  static std::unique_ptr<planner::AbstractPlanNode> Optimize(
      common::ManagedPointer<transaction::TransactionContext> txn,
      common::ManagedPointer<catalog::CatalogAccessor> accessor, common::ManagedPointer<parser::ParseResult> query,
      common::ManagedPointer<optimizer::StatsStorage> stats_storage, uint64_t optimizer_timeout);

  /**
   * Converts parser statement types (which rely on multiple enums) to a single QueryType enum from the network layer
   * @param statement
   * @return
   */
  static network::QueryType QueryTypeForStatement(common::ManagedPointer<parser::SQLStatement> statement);
};

}  // namespace terrier::trafficcop
