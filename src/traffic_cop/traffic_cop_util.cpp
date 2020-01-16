#include "traffic_cop/traffic_cop_util.h"

#include <string>
#include <vector>

#include "binder/bind_node_visitor.h"
#include "catalog/catalog_accessor.h"
#include "optimizer/abstract_optimizer.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "optimizer/operator_expression.h"
#include "optimizer/optimizer.h"
#include "optimizer/properties.h"
#include "optimizer/property_set.h"
#include "optimizer/query_to_operator_transformer.h"
#include "optimizer/statistics/stats_storage.h"
#include "parser/parser_defs.h"
#include "parser/postgresparser.h"

namespace terrier::trafficcop {

bool TrafficCopUtil::Bind(const common::ManagedPointer<catalog::CatalogAccessor> accessor, const std::string &db_name,
                          const common::ManagedPointer<parser::ParseResult> query) {
  try {
    binder::BindNodeVisitor visitor(accessor, db_name);
    visitor.BindNameToNode(query->GetStatement(0), query.Get());
  } catch (const Exception &e) {
    // Failed to bind
    // TODO(Matt): handle this in some more verbose manner for the client (return more state)
    return false;
  }
  return true;
}

std::unique_ptr<planner::AbstractPlanNode> TrafficCopUtil::Optimize(
    const common::ManagedPointer<transaction::TransactionContext> txn,
    const common::ManagedPointer<catalog::CatalogAccessor> accessor,
    const common::ManagedPointer<parser::ParseResult> query,
    const common::ManagedPointer<optimizer::StatsStorage> stats_storage, const uint64_t optimizer_timeout) {
  // Optimizer transforms annotated ParseResult to logical expressions (ephemeral Optimizer structure)
  optimizer::QueryToOperatorTransformer transformer(accessor);
  auto logical_exprs = transformer.ConvertToOpExpression(query->GetStatement(0), query.Get());

  // TODO(Matt): is the cost model to use going to become an arg to this function eventually?
  optimizer::Optimizer optimizer(std::make_unique<optimizer::TrivialCostModel>(), optimizer_timeout);
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

}  // namespace terrier::trafficcop
