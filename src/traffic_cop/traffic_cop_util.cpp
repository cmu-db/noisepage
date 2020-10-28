#include "traffic_cop/traffic_cop_util.h"

#include "catalog/catalog_accessor.h"
#include "optimizer/abstract_optimizer.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/operator_node.h"
#include "optimizer/optimizer.h"
#include "optimizer/properties.h"
#include "optimizer/property_set.h"
#include "optimizer/query_to_operator_transformer.h"
#include "optimizer/statistics/stats_storage.h"
#include "parser/drop_statement.h"
#include "parser/parser_defs.h"
#include "parser/postgresparser.h"
#include "parser/transaction_statement.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace noisepage::trafficcop {

std::unique_ptr<planner::AbstractPlanNode> TrafficCopUtil::Optimize(
    const common::ManagedPointer<transaction::TransactionContext> txn,
    const common::ManagedPointer<catalog::CatalogAccessor> accessor,
    const common::ManagedPointer<parser::ParseResult> query, const catalog::db_oid_t db_oid,
    common::ManagedPointer<optimizer::StatsStorage> stats_storage,
    std::unique_ptr<optimizer::AbstractCostModel> cost_model, const uint64_t optimizer_timeout) {
  // Optimizer transforms annotated ParseResult to logical expressions (ephemeral Optimizer structure)
  optimizer::QueryToOperatorTransformer transformer(accessor, db_oid);
  auto logical_exprs = transformer.ConvertToOpExpression(query->GetStatement(0), query);

  // TODO(Matt): is the cost model to use going to become an arg to this function eventually?
  optimizer::Optimizer optimizer(std::move(cost_model), optimizer_timeout);
  optimizer::PropertySet property_set;
  std::vector<common::ManagedPointer<parser::AbstractExpression>> output;

  // Build the QueryInfo object. For SELECTs this may require a bunch of other stuff from the original statement.
  // If any more logic like this is needed in the future, we should break this into its own function somewhere since
  // this is Optimizer-specific stuff.
  const auto type = query->GetStatement(0)->GetType();
  if (type == parser::StatementType::SELECT) {
    const auto sel_stmt = query->GetStatement(0).CastManagedPointerTo<parser::SelectStatement>();

    // Output
    output = sel_stmt->GetSelectColumns();  // TODO(Matt): this is making a local copy. Revisit the life cycle and
    // immutability of all of these Optimizer inputs to reduce copies.

    // PropertySort
    if (sel_stmt->GetSelectOrderBy()) {
      std::vector<optimizer::OrderByOrderingType> sort_dirs;
      std::vector<common::ManagedPointer<parser::AbstractExpression>> sort_exprs;
      auto order_by = sel_stmt->GetSelectOrderBy();
      auto types = order_by->GetOrderByTypes();
      auto exprs = order_by->GetOrderByExpressions();
      for (size_t idx = 0; idx < order_by->GetOrderByExpressionsSize(); idx++) {
        sort_exprs.emplace_back(exprs[idx]);
        sort_dirs.push_back(types[idx] == parser::OrderType::kOrderAsc ? optimizer::OrderByOrderingType::ASC
                                                                       : optimizer::OrderByOrderingType::DESC);
      }

      auto sort_prop = new optimizer::PropertySort(sort_exprs, sort_dirs);
      property_set.AddProperty(sort_prop);
    }
  }

  auto query_info = optimizer::QueryInfo(type, std::move(output), &property_set);
  // TODO(Matt): QueryInfo holding a raw pointer to PropertySet obfuscates the required life cycle of PropertySet

  // Optimize, consuming the logical expressions in the process
  return optimizer.BuildPlanTree(txn.Get(), accessor.Get(), stats_storage.Get(), query_info, std::move(logical_exprs));
  // TODO(Matt): I see a lot of copying going on in the Optimizer that maybe shouldn't be happening. BuildPlanTree's
  // signature is copying QueryInfo object (contains a vector of output columns), which then immediately makes a local
  // copy of that vector anyway. Presumably those are immutable expressions, in which case they should be const & to the
  // original vector (or parent object) all the way down.
  // TODO(Matt): Why does the Optimizer need a TransactionContext? It looks like it's an arg all the way down to the
  // cost model. Do we expect that can be transactional?
}

network::QueryType TrafficCopUtil::QueryTypeForStatement(const common::ManagedPointer<parser::SQLStatement> statement) {
  const auto statement_type = statement->GetType();
  switch (statement_type) {
    case parser::StatementType::TRANSACTION: {
      const auto txn_type = statement.CastManagedPointerTo<parser::TransactionStatement>()->GetTransactionType();
      switch (txn_type) {
        case parser::TransactionStatement::CommandType::kBegin:
          return network::QueryType::QUERY_BEGIN;
        case parser::TransactionStatement::CommandType::kCommit:
          return network::QueryType::QUERY_COMMIT;
        case parser::TransactionStatement::CommandType::kRollback:
          return network::QueryType::QUERY_ROLLBACK;
      }
    }
    case parser::StatementType::SELECT:
      return network::QueryType::QUERY_SELECT;
    case parser::StatementType::INSERT:
      return network::QueryType::QUERY_INSERT;
    case parser::StatementType::UPDATE:
      return network::QueryType::QUERY_UPDATE;
    case parser::StatementType::DELETE:
      return network::QueryType::QUERY_DELETE;
    case parser::StatementType::CREATE: {
      const auto create_type = statement.CastManagedPointerTo<parser::CreateStatement>()->GetCreateType();
      switch (create_type) {
        case parser::CreateStatement::CreateType::kTable:
          return network::QueryType::QUERY_CREATE_TABLE;
        case parser::CreateStatement::CreateType::kDatabase:
          return network::QueryType::QUERY_CREATE_DB;
        case parser::CreateStatement::CreateType::kIndex:
          return network::QueryType::QUERY_CREATE_INDEX;
        case parser::CreateStatement::CreateType::kTrigger:
          return network::QueryType::QUERY_CREATE_TRIGGER;
        case parser::CreateStatement::CreateType::kSchema:
          return network::QueryType::QUERY_CREATE_SCHEMA;
        case parser::CreateStatement::CreateType::kView:
          return network::QueryType::QUERY_CREATE_VIEW;
      }
    }
    case parser::StatementType::DROP: {
      const auto drop_type = statement.CastManagedPointerTo<parser::DropStatement>()->GetDropType();
      switch (drop_type) {
        case parser::DropStatement::DropType::kDatabase:
          return network::QueryType::QUERY_DROP_DB;
        case parser::DropStatement::DropType::kTable:
          return network::QueryType::QUERY_DROP_TABLE;
        case parser::DropStatement::DropType::kSchema:
          return network::QueryType::QUERY_DROP_SCHEMA;
        case parser::DropStatement::DropType::kIndex:
          return network::QueryType::QUERY_DROP_INDEX;
        case parser::DropStatement::DropType::kView:
          return network::QueryType::QUERY_DROP_VIEW;
        case parser::DropStatement::DropType::kPreparedStatement:
          return network::QueryType::QUERY_DROP_PREPARED_STATEMENT;
        case parser::DropStatement::DropType::kTrigger:
          return network::QueryType::QUERY_DROP_TRIGGER;
      }
    }
    case parser::StatementType::VARIABLE_SET:
      return network::QueryType::QUERY_SET;
    case parser::StatementType::PREPARE:
      return network::QueryType::QUERY_PREPARE;
    case parser::StatementType::EXECUTE:
      return network::QueryType::QUERY_EXECUTE;
    case parser::StatementType::RENAME:
      return network::QueryType::QUERY_RENAME;
    case parser::StatementType::ALTER:
      return network::QueryType::QUERY_ALTER;
    case parser::StatementType::COPY:
      return network::QueryType::QUERY_COPY;
    case parser::StatementType::ANALYZE:
      return network::QueryType::QUERY_ANALYZE;
    case parser::StatementType::EXPLAIN:
      return network::QueryType::QUERY_EXPLAIN;
    default:
      return network::QueryType::QUERY_INVALID;
  }
}

}  // namespace noisepage::trafficcop
