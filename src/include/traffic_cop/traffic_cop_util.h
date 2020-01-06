#pragma once

#include <string>

#include "common/managed_pointer.h"

namespace terrier::catalog {
class CatalogAccessor;
}

namespace terrier::parser {
class ParseResult;
}

namespace terrier::planner {
class AbstractPlanNode;
}

namespace terrier::optimizer {
class OperatorExpression;
class StatsStorage;
}  // namespace terrier::optimizer

namespace terrier::transaction {
class TransactionContext;
}

namespace terrier::trafficcop {

class TrafficCopUtil {
 public:
  TrafficCopUtil() = delete;

  static std::unique_ptr<planner::AbstractPlanNode> ParseBindAndOptimize(
      common::ManagedPointer<transaction::TransactionContext> txn,
      common::ManagedPointer<catalog::CatalogAccessor> accessor,
      common::ManagedPointer<optimizer::StatsStorage> stats_storage, const std::string &query_string,
      const std::string &db_name, uint64_t optimizer_timeout);
};

}  // namespace terrier::trafficcop
