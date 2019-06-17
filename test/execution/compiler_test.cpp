#include <catalog/catalog_defs.h>

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include "execution/ast/ast_dump.h"

#include "execution/tpl_test.h"  // NOLINT
#include "execution/compiler/compiler.h"
#include "execution/compiler/query.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/output.h"
#include "execution/sema/sema.h"
#include "execution/sql/execution_structures.h"
#include "execution/sql/value.h"
#include "execution/util/cpu_info.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/module.h"
#include "execution/vm/llvm_engine.h"

#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/operator_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/tuple_value_expression.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/analyze_plan_node.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/create_function_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/create_namespace_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/create_trigger_plan_node.h"
#include "planner/plannodes/create_view_plan_node.h"
#include "planner/plannodes/csv_scan_plan_node.h"
#include "planner/plannodes/delete_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "planner/plannodes/drop_trigger_plan_node.h"
#include "planner/plannodes/drop_view_plan_node.h"
#include "planner/plannodes/export_external_file_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/hash_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/result_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/set_op_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

#include "util/test_harness.h"

namespace tpl::compiler::test {
using namespace terrier::planner;
using namespace terrier::parser;

class CompilerTest : public TplTest {
 public:
  static void CompileAndRun(terrier::planner::AbstractPlanNode *node, terrier::transaction::TransactionContext * txn) {
    std::cout << "Input Plan: " << std::endl;
    std::cout << node->ToJson().dump(2) << std::endl << std::endl;

    // Create the execution context
    exec::OutputPrinter printer(node->GetOutputSchema().get());
    exec::ExecutionContext exec_ctx{txn, printer, node->GetOutputSchema().get()};

    // Create the query object, whose region must outlive all the processing.
    tpl::compiler::Query query(*node, &exec_ctx);


    // Compile and check for errors
    Compiler compiler(&query);
    compiler.Compile();
    if (query.GetReporter()->HasErrors()) {
      EXECUTION_LOG_ERROR("Type-checking error!");
      query.GetReporter()->PrintErrors();
    }
    std::cout << "Converted: " << std::endl;
    auto root = query.GetCompiledFile();
    tpl::ast::AstDump::Dump(root);

    // Convert to bytecode
    auto bytecode_module = vm::BytecodeGenerator::Compile(root, &exec_ctx, "tmp-tpl");
    auto module = std::make_unique<vm::Module>(std::move(bytecode_module));

    // Run the main function
    std::function<u32(exec::ExecutionContext *)> main;
    if (!module->GetFunction("main", vm::ExecutionMode::Interpret, &main)) {
      EXECUTION_LOG_ERROR(
          "Missing 'main' entry function with signature "
          "(*ExecutionContext)->int32");
      return;
    }
    auto memory = std::make_unique<sql::MemoryPool>(nullptr);
    exec_ctx.SetMemoryPool(std::move(memory));
    EXECUTION_LOG_INFO("VM main() returned: {}", main(&exec_ctx));
  }

  /**
   * Constructs a dummy OutputSchema object with a single column
   * @return dummy output schema
   */
  static std::shared_ptr<terrier::planner::OutputSchema> BuildDummyOutputSchema() {
    auto cve = std::make_shared<terrier::parser::ConstantValueExpression>(terrier::type::TransientValueFactory::GetInteger(42));
    terrier::planner::OutputSchema::Column col(terrier::type::TypeId::INTEGER, true, cve);
    std::vector<terrier::planner::OutputSchema::Column> cols;
    cols.push_back(col);
    auto schema = std::make_shared<terrier::planner::OutputSchema>(cols);
    return schema;
  }

  /**
   * Constructs a dummy AbstractExpression predicate
   * @return dummy predicate
   */
  static std::shared_ptr<terrier::parser::AbstractExpression> BuildDummyPredicate() {
    return std::make_shared<terrier::parser::ConstantValueExpression>(terrier::type::TransientValueFactory::GetBoolean(true));
  }

  static std::shared_ptr<terrier::parser::AbstractExpression> BuildConstant(int val) {
    return std::make_shared<terrier::parser::ConstantValueExpression>(terrier::type::TransientValueFactory::GetInteger(val));
  }

    static std::shared_ptr<terrier::parser::AbstractExpression> BuildTVE(uint32_t tuple_idx, uint32_t attr_idx, terrier::type::TypeId type) {
    return std::make_shared<terrier::parser::ExecTupleValueExpression>(tuple_idx, attr_idx, type);
  }

  static std::shared_ptr<terrier::parser::AbstractExpression> BuildComparison(terrier::parser::ExpressionType comp_type,
                                                                              std::shared_ptr<terrier::parser::AbstractExpression> child1,
                                                                              std::shared_ptr<terrier::parser::AbstractExpression> child2) {
    return std::make_shared<terrier::parser::ComparisonExpression>(comp_type, std::vector<std::shared_ptr<terrier::parser::AbstractExpression>>{child1, child2});
  }

  static std::shared_ptr<terrier::parser::AbstractExpression> BuildOperator(terrier::parser::ExpressionType op_type, terrier::type::TypeId ret_type, std::shared_ptr<terrier::parser::AbstractExpression> child) {
    return std::make_shared<terrier::parser::OperatorExpression>(op_type, ret_type, std::vector<std::shared_ptr<terrier::parser::AbstractExpression>>{child});
  }


  static std::shared_ptr<terrier::parser::AbstractExpression> BuildOperator(terrier::parser::ExpressionType op_type, terrier::type::TypeId ret_type, std::shared_ptr<terrier::parser::AbstractExpression> child1, std::shared_ptr<terrier::parser::AbstractExpression> child2) {
    return std::make_shared<terrier::parser::OperatorExpression>(op_type, ret_type, std::vector<std::shared_ptr<terrier::parser::AbstractExpression>>{child1, child2});
  }

  static AggregateTerm BuildAggregateTerm(terrier::parser::ExpressionType agg_type, std::shared_ptr<terrier::parser::AbstractExpression> child) {
    return std::make_shared<terrier::parser::AggregateExpression>(agg_type, std::vector<std::shared_ptr<terrier::parser::AbstractExpression>>{child}, false);
  }

  /**
   * Constructs a dummy SeqScanPlanNode to be used as a child for another plan
   */
  static std::shared_ptr<AbstractPlanNode> BuildDummySeqScanPlan() {
    SeqScanPlanNode::Builder builder;
    return builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
        .SetScanPredicate(CompilerTest::BuildDummyPredicate())
        .SetIsParallelFlag(true)
        .SetIsForUpdateFlag(false)
        .SetDatabaseOid(terrier::catalog::DEFAULT_DATABASE_OID)
        .SetTableOid(terrier::catalog::table_oid_t())
        .SetNamespaceOid(terrier::catalog::namespace_oid_t(0))
        .Build();
  }

  static std::shared_ptr<terrier::parser::AbstractExpression> BuildConstantComparisonPredicate(const std::string &table_name,
                                                                                      const std::string &col_name,
                                                                                      uint32_t val) {
    auto tuple_expr = std::make_shared<terrier::parser::TupleValueExpression>(col_name, table_name);
    auto val_expr = std::make_shared<terrier::parser::ConstantValueExpression>(terrier::type::TransientValueFactory::GetInteger(val));
    return std::make_shared<terrier::parser::ComparisonExpression>(
        terrier::parser::ExpressionType::COMPARE_LESS_THAN,
        std::vector<std::shared_ptr<terrier::parser::AbstractExpression>>{tuple_expr, val_expr});
  }
};

/*
// NOLINTNEXTLINE
TEST_F(CompilerTest, DummySeqScanTest) {
  // SELECT 42 FROM test_1 WHERE true;
  auto txn_mgr = sql::ExecutionStructures::Instance()->GetTxnManager();
  auto txn = txn_mgr->BeginTransaction();
  auto op = BuildDummySeqScanPlan();
  CompileAndRun(op.get(), txn);
  txn_mgr->Commit(txn, nullptr, nullptr);
}
*/

/*
// NOLINTNEXTLINE
TEST_F(CompilerTest, SeqScanTest) {
  // SELECT col1, col2, col1 * col2, col1 >= 100*col2 FROM test_1 WHERE col1 < 500 AND col2 >= 3;
  // Begin a transaction
  auto exec = sql::ExecutionStructures::Instance();
  auto txn_mgr = exec->GetTxnManager();
  auto txn = txn_mgr->BeginTransaction();
  // Get the right oid
  auto test_db_ns = exec->GetTestDBAndNS();
  auto * catalog_table = exec->GetCatalog()->GetUserTable(txn, test_db_ns.first, test_db_ns.second, "test_1");

  SeqScanPlanNode::Builder builder;
  // col1
  auto tve1 = BuildTVE(0, 0, terrier::type::TypeId::INTEGER);
  // col2
  auto tve2 = BuildTVE(0, 1, terrier::type::TypeId::INTEGER);
  // col1 * col2
  auto col_product = BuildOperator(terrier::parser::ExpressionType::OPERATOR_MULTIPLY, terrier::type::TypeId::INTEGER, tve1, tve2);
  // col1 >= 100 * col2
  auto constant_100 = BuildConstant(100);
  auto product_100 = BuildOperator(terrier::parser::ExpressionType::OPERATOR_MULTIPLY, terrier::type::TypeId::INTEGER, tve2, constant_100);
  auto comp = BuildComparison(terrier::parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO, tve1, product_100);
  // Construct predicate
  auto constant_500 = BuildConstant(500);
  auto col1_pred = BuildComparison(terrier::parser::ExpressionType::COMPARE_LESS_THAN, tve1, constant_500);
  auto constant_3 = BuildConstant(3);
  auto col2_pred = BuildComparison(terrier::parser::ExpressionType::COMPARE_GREATER_THAN_OR_EQUAL_TO, tve2, constant_3);
  auto predicate = BuildComparison(terrier::parser::ExpressionType::CONJUNCTION_AND, col1_pred, col2_pred);
  // Construct schema
  OutputSchema::Column col1{terrier::type::TypeId::INTEGER, true, tve1};
  OutputSchema::Column col2{terrier::type::TypeId::INTEGER, true, tve2};
  OutputSchema::Column col3{terrier::type::TypeId::INTEGER, true, col_product};
  OutputSchema::Column col4{terrier::type::TypeId::BOOLEAN, true, comp};
  std::vector<OutputSchema::Column> cols{col1, col2, col3, col4};
  auto schema = std::make_shared<OutputSchema>(cols);
  // Construct node and compile it
  auto op =
      builder.SetOutputSchema(schema)
      .SetScanPredicate(predicate)
      .SetIsParallelFlag(false)
      .SetIsForUpdateFlag(false)
      .SetDatabaseOid(test_db_ns.first)
      .SetNamespaceOid(test_db_ns.second)
      .SetTableOid(catalog_table->Oid())
      .Build();
  CompileAndRun(op.get(), txn);
  txn_mgr->Commit(txn, nullptr, nullptr);
}
*/

/*
// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleAggregateTest) {
  // SELECT col2, SUM(col1) FROM test_1 WHERE col1 < 1000 GROUP BY col2;
  // Begin a transaction
  auto exec = sql::ExecutionStructures::Instance();
  auto txn_mgr = exec->GetTxnManager();
  auto txn = txn_mgr->BeginTransaction();
  // Get the right oids
  auto test_db_ns = exec->GetTestDBAndNS();
  auto * catalog_table = exec->GetCatalog()->GetUserTable(txn, test_db_ns.first, test_db_ns.second, "test_1");

  // Make the seq scan
  std::shared_ptr<AbstractPlanNode> seq_scan;
  {
    // col1 and col2
    auto tve1 = BuildTVE(0, 0, terrier::type::TypeId::INTEGER);
    auto tve2 = BuildTVE(0, 1, terrier::type::TypeId::INTEGER);
    // predicate
    auto constant_1000 = BuildConstant(1000);
    auto predicate = BuildComparison(terrier::parser::ExpressionType::COMPARE_LESS_THAN, tve1, constant_1000);
    // Output Schema
    OutputSchema::Column col1{terrier::type::TypeId::INTEGER, true, tve1};
    OutputSchema::Column col2{terrier::type::TypeId::INTEGER, true, tve2};
    std::vector<OutputSchema::Column> cols{col1, col2};
    auto schema = std::make_shared<OutputSchema>(cols);
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan =
        builder.SetOutputSchema(schema)
            .SetScanPredicate(predicate)
            .SetIsParallelFlag(false)
            .SetIsForUpdateFlag(false)
            .SetDatabaseOid(test_db_ns.first)
            .SetNamespaceOid(test_db_ns.second)
            .SetTableOid(catalog_table->Oid())
            .Build();
  }
  // Make the aggregate
  std::shared_ptr<AbstractPlanNode> agg;
  {
    // First the group by expressions (second column of the seq_scan)
    auto gby_term = BuildTVE(0, 1, terrier::type::TypeId::INTEGER);
    // Then the aggregate expressions SUM(first column of the seq_scan)
    auto agg_term = BuildAggregateTerm(terrier::parser::ExpressionType::AGGREGATE_SUM, BuildTVE(0, 0, terrier::type::TypeId::INTEGER));
    // Now the output expressions
    // The group by term (index = index_in_gby_terms = 0)
    auto out_expr1 = BuildTVE(0, 0, terrier::type::TypeId::INTEGER);
    // The agg term (index = num_group_by_terms + index_in_agg_terms = 1 + 0 = 1)
    auto out_expr2 = BuildTVE(0, 1, terrier::type::TypeId::INTEGER);
    // Output Columns and Schema
    OutputSchema::Column col1{terrier::type::TypeId::INTEGER, true, out_expr1};
    OutputSchema::Column col2{terrier::type::TypeId::INTEGER, true, out_expr2};
    std::vector<OutputSchema::Column> cols{col1, col2};
    auto schema = std::make_shared<OutputSchema>(cols);
    // Build
    AggregatePlanNode::Builder builder;
    agg =
        builder.SetOutputSchema(schema)
        .AddGroupByTerm(gby_term)
        .AddAggregateTerm(agg_term)
        .AddChild(seq_scan)
        .SetAggregateStrategyType(AggregateStrategyType::HASH)
        .SetHavingClausePredicate(nullptr)
        .Build();
  }
  // Compile and Run
  CompileAndRun(agg.get(), txn);
  txn_mgr->Commit(txn, nullptr, nullptr);
  // TODO(Amadou): Somehow check that the result is correct
  // There should be 10 output rows
  // An the second column should sum up to 1000*999 / 2
  // I have manually checked that this is true, but an automated test is better.
}
*/

/*
// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleHashJoinTest) {
  // SELECT t1.col1, t2.col1, t2.col2, t1.col2 + t2.col2 FROM t1 INNER JOIN t2 ON t1.col1=t2.col1
  // WHERE t1.col1 < 500
  // Begin a transaction
  auto exec = sql::ExecutionStructures::Instance();
  auto txn_mgr = exec->GetTxnManager();
  auto txn = txn_mgr->BeginTransaction();
  // Get the right oids
  // TODO: Join with test_2 after catalog is in
  auto test_db_ns = exec->GetTestDBAndNS();
  auto * catalog_table = exec->GetCatalog()->GetUserTable(txn, test_db_ns.first, test_db_ns.second, "test_1");

  // Make the seq scan
  std::shared_ptr<AbstractPlanNode> seq_scan1;
  {
    // col1 and col2
    auto tve1 = BuildTVE(0, 0, terrier::type::TypeId::INTEGER);
    auto tve2 = BuildTVE(0, 1, terrier::type::TypeId::INTEGER);
    // predicate
    auto constant_500 = BuildConstant(500);
    auto predicate = BuildComparison(terrier::parser::ExpressionType::COMPARE_LESS_THAN, tve1, constant_500);
    // Output Schema
    OutputSchema::Column col1{terrier::type::TypeId::INTEGER, true, tve1};
    OutputSchema::Column col2{terrier::type::TypeId::INTEGER, true, tve2};
    std::vector<OutputSchema::Column> cols{col1, col2};
    auto schema = std::make_shared<OutputSchema>(cols);
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan1 =
        builder.SetOutputSchema(schema)
            .SetScanPredicate(predicate)
            .SetIsParallelFlag(false)
            .SetIsForUpdateFlag(false)
            .SetDatabaseOid(test_db_ns.first)
            .SetNamespaceOid(test_db_ns.second)
            .SetTableOid(catalog_table->Oid())
            .Build();
  }
  // Make the seq scan
  // TODO: Use test_2 here
  std::shared_ptr<AbstractPlanNode> seq_scan2;
  {
    // col1 and col2
    auto tve1 = BuildTVE(0, 0, terrier::type::TypeId::INTEGER);
    auto tve2 = BuildTVE(0, 1, terrier::type::TypeId::INTEGER);
    // predicate
    auto constant_500 = BuildConstant(500);
    auto predicate = BuildComparison(terrier::parser::ExpressionType::COMPARE_LESS_THAN, tve1, constant_500);
    // Output Schema
    OutputSchema::Column col1{terrier::type::TypeId::INTEGER, true, tve1};
    OutputSchema::Column col2{terrier::type::TypeId::INTEGER, true, tve2};
    std::vector<OutputSchema::Column> cols{col1, col2};
    auto schema = std::make_shared<OutputSchema>(cols);
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan2 =
        builder.SetOutputSchema(schema)
            .SetScanPredicate(predicate)
            .SetIsParallelFlag(false)
            .SetIsForUpdateFlag(false)
            .SetDatabaseOid(test_db_ns.first)
            .SetNamespaceOid(test_db_ns.second)
            .SetTableOid(catalog_table->Oid())
            .Build();
  }
  // Make hash join
  std::shared_ptr<AbstractPlanNode> hash_join;
  {
    // t1.col1, and t1.col2
    auto tve1_1 = BuildTVE(0, 0, terrier::type::TypeId::INTEGER);
    auto tve1_2 = BuildTVE(0, 1, terrier::type::TypeId::INTEGER);
    // t2.col1, and t2.col2
    auto tve2_1 = BuildTVE(1, 0, terrier::type::TypeId::INTEGER);
    auto tve2_2 = BuildTVE(1, 1, terrier::type::TypeId::INTEGER);
    // t1.col2 + t2.col2
    auto sum = BuildOperator(ExpressionType::OPERATOR_PLUS, terrier::type::TypeId::INTEGER, tve1_2, tve2_2);
    // Predicate
    auto predicate = BuildComparison(ExpressionType::COMPARE_EQUAL, tve1_1, tve2_1);
    // Output Schema
    OutputSchema::Column col1{terrier::type::TypeId::INTEGER, true, tve1_1};
    OutputSchema::Column col2{terrier::type::TypeId::INTEGER, true, tve2_1};
    OutputSchema::Column col3{terrier::type::TypeId::INTEGER, true, tve2_2};
    OutputSchema::Column col4{terrier::type::TypeId::INTEGER, true, sum};
    std::vector<OutputSchema::Column> cols{col1, col2, col3, col4};
    auto schema = std::make_shared<OutputSchema>(cols);
    // Build
    HashJoinPlanNode::Builder builder;
    hash_join =
        builder.AddChild(seq_scan1).AddChild(seq_scan2)
        .SetOutputSchema(schema)
        .AddLeftHashKey(tve1_1).AddRightHashKey(tve2_1)
        .SetJoinType(LogicalJoinType::INNER)
        .SetJoinPredicate(predicate)
        .Build();
  }
  // Compile and Run
  CompileAndRun(hash_join.get(), txn);
  txn_mgr->Commit(txn, nullptr, nullptr);
  // TODO(Amadou): Come up with a way to automatically check that the joined rows are the same.

}
*/

/*
// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleSortTest) {
  // SELECT col1, col2, col1 + col2 FROM test_1 WHERE col1 < 500 ORDER BY col2 ASC, col1 - col2 DESC
  // Begin a transaction
  auto exec = sql::ExecutionStructures::Instance();
  auto txn_mgr = exec->GetTxnManager();
  auto txn = txn_mgr->BeginTransaction();
  // Get the right oids
  // TODO: Join with test_2 after catalog is in
  auto test_db_ns = exec->GetTestDBAndNS();
  auto * catalog_table = exec->GetCatalog()->GetUserTable(txn, test_db_ns.first, test_db_ns.second, "test_1");

  // Make the seq scan
  std::shared_ptr<AbstractPlanNode> seq_scan1;
  {
    // col1 and col2
    auto tve1 = BuildTVE(0, 0, terrier::type::TypeId::INTEGER);
    auto tve2 = BuildTVE(0, 1, terrier::type::TypeId::INTEGER);
    // predicate
    auto constant_500 = BuildConstant(500);
    auto predicate = BuildComparison(terrier::parser::ExpressionType::COMPARE_LESS_THAN, tve1, constant_500);
    // Output Schema
    OutputSchema::Column col1{terrier::type::TypeId::INTEGER, true, tve1};
    OutputSchema::Column col2{terrier::type::TypeId::INTEGER, true, tve2};
    std::vector<OutputSchema::Column> cols{col1, col2};
    auto schema = std::make_shared<OutputSchema>(cols);
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan1 =
        builder.SetOutputSchema(schema)
            .SetScanPredicate(predicate)
            .SetIsParallelFlag(false)
            .SetIsForUpdateFlag(false)
            .SetDatabaseOid(test_db_ns.first)
            .SetNamespaceOid(test_db_ns.second)
            .SetTableOid(catalog_table->Oid())
            .Build();
  }
  // Order By
  std::shared_ptr<AbstractPlanNode> order_by;
  {
    // col1, col2, col1 + col2
    auto tve1 = BuildTVE(0, 0, terrier::type::TypeId::INTEGER);
    auto tve2 = BuildTVE(0, 1, terrier::type::TypeId::INTEGER);
    auto sum = BuildOperator(ExpressionType::OPERATOR_PLUS, terrier::type::TypeId::INTEGER, tve1, tve2);
    // Order By Clause
    SortKey clause1{tve2, OrderByOrderingType::ASC};
    auto diff = BuildOperator(ExpressionType::OPERATOR_MINUS, terrier::type::TypeId::INTEGER, tve1, tve2);
    SortKey clause2{diff, OrderByOrderingType::DESC};
    // Output Schema
    OutputSchema::Column col1{terrier::type::TypeId::INTEGER, true, tve1};
    OutputSchema::Column col2{terrier::type::TypeId::INTEGER, true, tve2};
    OutputSchema::Column col3{terrier::type::TypeId::INTEGER, true, sum};
    std::vector<OutputSchema::Column> cols{col1, col2, col3};
    auto schema = std::make_shared<OutputSchema>(cols);
    // Build
    OrderByPlanNode::Builder builder;
    order_by =
        builder.SetOutputSchema(schema)
        .AddChild(seq_scan1)
        .AddSortKey(clause1.first, clause1.second)
        .AddSortKey(clause2.first, clause2.second)
        .Build();
  }
  // Compile and Run
  CompileAndRun(order_by.get(), txn);
  txn_mgr->Commit(txn, nullptr, nullptr);

  // TODO(Amadou): Add a way to automatically check that the output is order by the second column in asc order,
  // then by the first column in desc order.
}
*/
}  // namespace terrier::planner
