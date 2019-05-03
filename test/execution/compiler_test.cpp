#include <vector>
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "type/transient_value_factory.h"
#include "util/test_harness.h"

namespace terrier {

class CompilerTest : public TerrierTest {
 protected:
  void SetUp() override { TerrierTest::SetUp(); }

  void TearDown() override { TerrierTest::TearDown(); }
};

static std::shared_ptr<planner::OutputSchema> BuildDummyOutputSchema() {
  std::vector<planner::OutputSchema::Column> cols;
  cols.emplace_back("dummy_col", type::TypeId::INTEGER, true, catalog::col_oid_t{0});
  return std::make_shared<planner::OutputSchema>(cols);
}

TEST_F(CompilerTest, SimpleTest) {
  planner::SeqScanPlanNode::Builder seq_scan_builder;
  auto seq_scan = seq_scan_builder.SetOutputSchema(BuildDummyOutputSchema())
                      .SetScanPredicate(nullptr)  // TODO(WAN): c++ types are wack
                      .SetIsParallelFlag(true)
                      .SetIsForUpdateFlag(false)
                      .SetDatabaseOid(catalog::db_oid_t(0))
                      .SetTableOid(catalog::table_oid_t(0))
                      .SetNamespaceOid(catalog::namespace_oid_t(0))
                      .Build();

  std::vector<std::shared_ptr<parser::AbstractExpression>> children;
  // TODO(WAN): add some kids
  auto agg_term = std::make_shared<parser::AggregateExpression>(parser::ExpressionType::AGGREGATE_COUNT_STAR,
                                                                std::move(children), false);
  planner::AggregatePlanNode::Builder agg_builder;
  auto agg_node =
      agg_builder.SetOutputSchema(BuildDummyOutputSchema())
          .SetAggregateStrategyType(planner::AggregateStrategyType::INVALID)  // TODO(WAN): why do we need this?
          .SetHavingClausePredicate(nullptr)
          .AddAggregateTerm(std::move(agg_term))
          .Build();

  //  tpl::compiler::PlanNodeCompiler compiler;
  //  auto ast = compiler.Compile(agg_node_ptr.get());
  //  ast::AstDump::Dump(ast);

  //  compiler::CompilationContext ctx(region());
  //  ctx.PrepareOp(*reinterpret_cast<terrier::planner::AggregatePlanNode *>(agg_node_ptr.get()));
  //  auto *ast = ctx.GetTranslatorOp(*reinterpret_cast<terrier::planner::AggregatePlanNode
  //  *>(agg_node_ptr.get()))->Translate(); ast::AstDump::Dump(ast);
}

}  // namespace terrier
