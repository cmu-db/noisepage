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

  static std::unique_ptr<parser::ParseResult> Parse(const std::string &query_string);

  static bool Bind(common::ManagedPointer<catalog::CatalogAccessor> accessor, const std::string &db_name,
                   common::ManagedPointer<parser::ParseResult> query);

  static std::unique_ptr<planner::AbstractPlanNode> Optimize(
      common::ManagedPointer<transaction::TransactionContext> txn,
      common::ManagedPointer<catalog::CatalogAccessor> accessor, common::ManagedPointer<parser::ParseResult> query,
      common::ManagedPointer<optimizer::StatsStorage> stats_storage, uint64_t optimizer_timeout);
};

}  // namespace terrier::trafficcop
