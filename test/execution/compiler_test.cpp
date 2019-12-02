#include "catalog/catalog_defs.h"

#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "execution/ast/ast_dump.h"

#include "execution/compiler/compiler.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/output.h"
#include "execution/sema/sema.h"
#include "execution/sql/value.h"
#include "execution/sql_test.h"  // NOLINT
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/llvm_engine.h"
#include "execution/vm/module.h"

#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

#include "execution/compiler/expression_util.h"
#include "execution/compiler/output_checker.h"
#include "execution/compiler/output_schema_util.h"

namespace terrier::execution::compiler::test {
using namespace terrier::planner;
using namespace terrier::parser;

class CompilerTest : public SqlBasedTest {
 public:
  void SetUp() override {
    SqlBasedTest::SetUp();
    // Make the test tables
    auto exec_ctx = MakeExecCtx();
    sql::TableGenerator table_generator{exec_ctx.get(), BlockStore(), NSOid()};
    table_generator.GenerateTestTables();
  }

  static void CompileAndRun(terrier::planner::AbstractPlanNode *node, exec::ExecutionContext *exec_ctx) {
    // Create the query object, whose region must outlive all the processing.
    // Compile and check for errors
    CodeGen codegen(exec_ctx->GetAccessor());
    Compiler compiler(&codegen, node);
    auto root = compiler.Compile();
    if (codegen.Reporter()->HasErrors()) {
      EXECUTION_LOG_ERROR("Type-checking error! \n {}", codegen.Reporter()->SerializeErrors());
    }

    EXECUTION_LOG_INFO("Converted: \n {}", execution::ast::AstDump::Dump(root));

    // Convert to bytecode
    auto bytecode_module = vm::BytecodeGenerator::Compile(root, exec_ctx, "tmp-tpl");
    auto module = std::make_unique<vm::Module>(std::move(bytecode_module));

    // Run the main function
    std::function<int64_t(exec::ExecutionContext *)> main;
    if (!module->GetFunction("main", vm::ExecutionMode::Interpret, &main)) {
      EXECUTION_LOG_ERROR(
          "Missing 'main' entry function with signature "
          "(*ExecutionContext)->int32");
      return;
    }
    EXECUTION_LOG_INFO("VM main() returned: {}", main(exec_ctx));
  }

  /**
   * Initialize all TPL subsystems
   */
  static void InitTPL() {
    execution::CpuInfo::Instance();
    execution::vm::LLVMEngine::Initialize();
  }

  /**
   * Shutdown all TPL subsystems
   */
  static void ShutdownTPL() {
    terrier::execution::vm::LLVMEngine::Shutdown();
    terrier::LoggersUtil::ShutDown();
  }

};

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleSeqScanTest) {
  // SELECT col1, col2, col1 * col2, col1 >= 100*col2 FROM test_1 WHERE col1 < 500 AND col2 >= 3;
  auto accessor = MakeAccessor();
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table_schema = accessor->GetSchema(table_oid);
  ExpressionMaker expr_maker;
  std::unique_ptr<AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(table_schema.GetColumn("colB").Oid(), type::TypeId::INTEGER);
    // Make New Column
    auto col3 = expr_maker.OpMul(col1, col2);
    auto col4 = expr_maker.ComparisonGe(col1, expr_maker.OpMul(expr_maker.Constant(100), col2));
    seq_scan_out.AddOutput("col1", common::ManagedPointer(col1));
    seq_scan_out.AddOutput("col2", common::ManagedPointer(col2));
    seq_scan_out.AddOutput("col3", common::ManagedPointer(col3));
    seq_scan_out.AddOutput("col4", common::ManagedPointer(col4));
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto comp1 = expr_maker.ComparisonLt(col1, expr_maker.Constant(500));
    auto comp2 = expr_maker.ComparisonGe(col2, expr_maker.Constant(3));
    auto predicate = expr_maker.ConjunctionAnd(comp1, comp2);
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(predicate)
                   .SetIsParallelFlag(false)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid)
                   .Build();
  }

  // Make the output checkers
  SingleIntComparisonChecker col1_checker(std::less<int64_t>(), 0, 500);
  SingleIntComparisonChecker col2_checker(std::greater_equal<int64_t>(), 1, 3);

  MultiChecker multi_checker{std::vector<OutputChecker *>{&col1_checker, &col2_checker}};

  // Create the execution context
  OutputStore store{&multi_checker, seq_scan->GetOutputSchema().Get()};
  exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(seq_scan.get(), exec_ctx.get());
  multi_checker.CheckCorrectness();
}


// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleIndexScanTest) {
  // SELECT colA, colB FROM test_1 WHERE colA = 500;
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto index_oid = accessor->GetIndexOid(NSOid(), "index_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0, &expr_maker};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(table_schema.GetColumn("colB").Oid(), type::TypeId::INTEGER);
    auto const_500 = expr_maker.Constant(500);
    index_scan_out.AddOutput("col1", col1);
    index_scan_out.AddOutput("col2", col2);
    auto schema = index_scan_out.MakeSchema();
    IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid).SetIndexOid(index_oid)
                  .AddIndexColum(catalog::indexkeycol_oid_t(1), const_500)
                  .SetNamespaceOid(NSOid())
                  .SetOutputSchema(std::move(schema))
                  .Build();
  }
  NumChecker num_checker(1);
  SingleIntComparisonChecker col1_checker(std::equal_to<int64_t>(), 0, 500);
  MultiChecker multi_checker{std::vector<OutputChecker *>{&col1_checker, &num_checker}};
  // Create the execution context
  OutputStore store{&multi_checker, index_scan->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(index_scan.get(), exec_ctx.get());
  multi_checker.CheckCorrectness();
}


// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleAggregateTest) {
  // SELECT col2, SUM(col1) FROM test_1 WHERE col1 < 1000 GROUP BY col2;
  // Get accessor
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(table_schema.GetColumn("colB").Oid(), type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(1000));
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(predicate)
                   .SetIsParallelFlag(false)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Make the aggregate
  std::unique_ptr<AbstractPlanNode> agg;
  OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous output
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    // Add group by term
    agg_out.AddGroupByTerm("col2", col2);
    // Add aggregates
    auto sum_col1 = expr_maker.AggSum(col1);
    agg_out.AddAggTerm("sum_col1", sum_col1);
    // Make the output expressions
    agg_out.AddOutput("col2", agg_out.GetGroupByTermForOutput("col2"));
    agg_out.AddOutput("sum_col1", agg_out.GetAggTermForOutput("sum_col1"));
    auto schema = agg_out.MakeSchema();
    // Build
    AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(agg_out.GetGroupByTerm("col2"))
              .AddAggregateTerm(agg_out.GetAggTerm("col1"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }
  // Make the checkers
  NumChecker num_checker{10};
  SingleIntSumChecker sum_checker{1, (1000 * 999) / 2};
  MultiChecker multi_checker{std::vector<OutputChecker *>{&num_checker, &sum_checker}};

  // Compile and Run
  OutputStore store{&multi_checker, agg->GetOutputSchema().Get()};
  exec::OutputPrinter printer(agg->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), agg->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(agg.get(), exec_ctx.get());
  multi_checker.CheckCorrectness();
}


// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleAggregateHavingTest) {
  // SELECT col2, SUM(col1) FROM test_1 WHERE col1 < 1000 GROUP BY col2 HAVING col2 >= 3 AND SUM(col1) < 50000;
  // Get accessor
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(table_schema.GetColumn("colB").Oid(), type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(1000));
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(predicate)
                   .SetIsParallelFlag(false)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Make the aggregate
  std::unique_ptr<AbstractPlanNode> agg;
  OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous output
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    // Add group by term
    agg_out.AddGroupByTerm("col2", col2);
    // Add aggregates
    auto sum_col1 = expr_maker.AggSum(col1);
    agg_out.AddAggTerm("sum_col1", sum_col1);
    // Make the output expressions
    agg_out.AddOutput("col2", agg_out.GetGroupByTermForOutput("col2"));
    agg_out.AddOutput("sum_col1", agg_out.GetAggTermForOutput("sum_col1"));
    auto schema = agg_out.MakeSchema();
    // Make the having clause
    auto having1 = expr_maker.ComparisonGe(agg_out.GetGroupByTermForOutput("col2"), expr_maker.Constant(3));
    auto having2 =
        expr_maker.ComparisonLt(agg_out.GetAggTermForOutput("sum_col1"), expr_maker.Constant(50000));
    auto having = expr_maker.ConjunctionAnd(having1, having2);
    // Build
    AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(agg_out.GetGroupByTerm("col2"))
              .AddAggregateTerm(agg_out.GetAggTerm("col1"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(AggregateStrategyType::HASH)
              .SetHavingClausePredicate(having)
              .Build();
  }
  // Make the checkers
  RowChecker row_checker = [](const std::vector<sql::Val *> vals) {
    // Read cols
    auto col2 = static_cast<sql::Integer *>(vals[0]);
    auto sum_col1 = static_cast<sql::Integer *>(vals[1]);
    ASSERT_FALSE(col2->is_null_ || sum_col1->is_null_);
    // Check col2 >= 3
    ASSERT_GE(col2->val_, 3);
    // Check sum_col1 < 50000
    ASSERT_LT(sum_col1->val_, 50000);
  };
  CorrectnessFn correcteness_fn;
  GenericChecker checker(row_checker, correcteness_fn);

  // Compile and Run
  OutputStore store{&checker, agg->GetOutputSchema().Get()};
  exec::OutputPrinter printer(agg->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), agg->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(agg.get(), exec_ctx.get());
  checker.CheckCorrectness();
}


// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleHashJoinTest) {
  // SELECT t1.col1, t2.col1, t2.col2, t1.col1 + t2.col2 FROM t1 INNER JOIN t2 ON t1.col1=t2.col1
  // WHERE t1.col1 < 500 AND t2.col1 < 80
  // TODO(Amadou): Simple join tests are very similar. Some refactoring is possible.
  // Get accessor
  auto accessor = MakeAccessor();
   ExpressionMaker expr_maker;
 auto table_oid1 = accessor->GetTableOid(NSOid(), "test_1");
  auto table_oid2 = accessor->GetTableOid(NSOid(), "test_2");
  auto table_schema1 = accessor->GetSchema(table_oid1);
  auto table_schema2 = accessor->GetSchema(table_oid2);

  std::unique_ptr<AbstractPlanNode> seq_scan1;
  OutputSchemaHelper seq_scan_out1{0, &expr_maker};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema1.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(table_schema1.GetColumn("colB").Oid(), type::TypeId::INTEGER);
    seq_scan_out1.AddOutput("col1", col1);
    seq_scan_out1.AddOutput("col2", col2);
    auto schema = seq_scan_out1.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(1000));
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan1 = builder.SetOutputSchema(std::move(schema))
                    .SetScanPredicate(predicate)
                    .SetIsParallelFlag(false)
                    .SetIsForUpdateFlag(false)
                    .SetNamespaceOid(NSOid())
                    .SetTableOid(table_oid1)
                    .Build();
  }
  // Make the second seq scan
  std::unique_ptr<AbstractPlanNode> seq_scan2;
  OutputSchemaHelper seq_scan_out2{1, &expr_maker};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema2.GetColumn("col1").Oid(), type::TypeId::SMALLINT);
    auto col2 = expr_maker.CVE(table_schema2.GetColumn("col2").Oid(), type::TypeId::INTEGER);
    seq_scan_out2.AddOutput("col1", col1);
    seq_scan_out2.AddOutput("col2", col2);
    auto schema = seq_scan_out2.MakeSchema();
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(80));
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan2 = builder.SetOutputSchema(std::move(schema))
                    .SetScanPredicate(predicate)
                    .SetIsParallelFlag(false)
                    .SetIsForUpdateFlag(false)
                    .SetNamespaceOid(NSOid())
                    .SetTableOid(table_oid2)
                    .Build();
  }
  // Make hash join
  std::unique_ptr<AbstractPlanNode> hash_join;
  OutputSchemaHelper hash_join_out{0, &expr_maker};
  {
    // t1.col1, and t1.col2
    auto t1_col1 = seq_scan_out1.GetOutput("col1");
    // t2.col1 and t2.col2
    auto t2_col1 = seq_scan_out2.GetOutput("col1");
    auto t2_col2 = seq_scan_out2.GetOutput("col2");
    // t1.col2 + t2.col2
    auto sum = expr_maker.OpSum(t1_col1, t2_col2);
    // Output Schema
    hash_join_out.AddOutput("t1.col1", t1_col1);
    hash_join_out.AddOutput("t2.col1", t2_col1);
    hash_join_out.AddOutput("t2.col2", t2_col2);
    hash_join_out.AddOutput("sum", sum);
    auto schema = hash_join_out.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(t1_col1, t2_col1);
    // Build
    HashJoinPlanNode::Builder builder;
    hash_join = builder.AddChild(std::move(seq_scan1))
                    .AddChild(std::move(seq_scan2))
                    .SetOutputSchema(std::move(schema))
                    .AddLeftHashKey(t1_col1)
                    .AddRightHashKey(t2_col1)
                    .SetJoinType(LogicalJoinType::INNER)
                    .SetJoinPredicate(predicate)
                    .Build();
  }
  // Compile and Run
  // 80 hundred rows should be outputted because of the WHERE clause
  // The joined cols should be equal
  // The 4th column is the sum of the 1nd and 3rd columns
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{80};
  RowChecker row_checker = [&num_output_rows, num_expected_rows](const std::vector<sql::Val *> vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    auto col2 = static_cast<sql::Integer *>(vals[1]);
    auto col3 = static_cast<sql::Integer *>(vals[2]);
    auto col4 = static_cast<sql::Integer *>(vals[3]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_);
    // Check join cols
    ASSERT_EQ(col1->val_, col2->val_);
    // Check that col4 = col1 + col3
    ASSERT_EQ(col4->val_, col1->val_ + col3->val_);
    // Check the number of output row
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  GenericChecker checker(row_checker, correcteness_fn);

  OutputStore store{&checker, hash_join->GetOutputSchema().Get()};
  exec::OutputPrinter printer(hash_join->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), hash_join->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(hash_join.get(), exec_ctx.get());
  checker.CheckCorrectness();
}


// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleSortTest) {
  // SELECT col1, col2, col1 + col2 FROM test_1 WHERE col1 < 500 ORDER BY col2 ASC, col1 - col2 DESC
  // Get accessor
  auto accessor = MakeAccessor();
    ExpressionMaker expr_maker;
auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(table_schema.GetColumn("colB").Oid(), type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(500));
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(predicate)
                   .SetIsParallelFlag(false)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Order By
  std::unique_ptr<AbstractPlanNode> order_by;
  OutputSchemaHelper order_by_out{0, &expr_maker};
  {
    // Output Colums col1, col2, col1 + col2
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    auto sum = expr_maker.OpSum(col1, col2);
    order_by_out.AddOutput("col1", col1);
    order_by_out.AddOutput("col2", col2);
    order_by_out.AddOutput("sum", sum);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    SortKey clause1{col2, optimizer::OrderByOrderingType::ASC};
    auto diff = expr_maker.OpMin(col1, col2);
    SortKey clause2{diff, optimizer::OrderByOrderingType::DESC};
    // Build
    OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(seq_scan))
                   .AddSortKey(clause1.Expr(), clause1.SortType())
                   .AddSortKey(clause2.Expr(), clause2.SortType())
                   .Build();
  }
  // Checkers:
  // There should be 500 output rows, where col1 < 500.
  // The output should be sorted by col2 ASC, then col1 DESC.
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{500};
  int64_t curr_col1{std::numeric_limits<int64_t>::max()};
  int64_t curr_col2{std::numeric_limits<int64_t>::min()};
  RowChecker row_checker = [&num_output_rows, &curr_col1, &curr_col2,
                            num_expected_rows](const std::vector<sql::Val *> vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    auto col2 = static_cast<sql::Integer *>(vals[1]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_);
    // Check col1 and number of outputs
    ASSERT_LT(col1->val_, 500);
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);

    // Check that output is sorted by col2 ASC, then col1 DESC
    ASSERT_LE(curr_col2, col2->val_);
    if (curr_col2 == col2->val_) {
      ASSERT_GE(curr_col1, col1->val_);
    }
    curr_col1 = col1->val_;
    curr_col2 = col2->val_;
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  GenericChecker checker(row_checker, correcteness_fn);

  // Create exec ctx
  OutputStore store{&checker, order_by->GetOutputSchema().Get()};
  exec::OutputPrinter printer(order_by->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), order_by->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(order_by.get(), exec_ctx.get());
  checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleNestedLoopJoinTest) {
  // SELECT t1.col1, t2.col1, t2.col2, t1.col1 + t2.col2 FROM t1 INNER JOIN t2 ON t1.col1=t2.col1
  // WHERE t1.col1 < 500 AND t2.col1 < 80
  // Get accessor
  auto accessor = MakeAccessor();
    ExpressionMaker expr_maker;
auto table_oid1 = accessor->GetTableOid(NSOid(), "test_1");
  auto table_oid2 = accessor->GetTableOid(NSOid(), "test_2");
  auto table_schema1 = accessor->GetSchema(table_oid1);
  auto table_schema2 = accessor->GetSchema(table_oid2);

  std::unique_ptr<AbstractPlanNode> seq_scan1;
  OutputSchemaHelper seq_scan_out1{0, &expr_maker};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema1.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(table_schema1.GetColumn("colB").Oid(), type::TypeId::INTEGER);
    seq_scan_out1.AddOutput("col1", col1);
    seq_scan_out1.AddOutput("col2", col2);
    auto schema = seq_scan_out1.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(1000));
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan1 = builder.SetOutputSchema(std::move(schema))
                    .SetScanPredicate(predicate)
                    .SetIsParallelFlag(false)
                    .SetIsForUpdateFlag(false)
                    .SetNamespaceOid(NSOid())
                    .SetTableOid(table_oid1)
                    .Build();
  }
  // Make the second seq scan
  std::unique_ptr<AbstractPlanNode> seq_scan2;
  OutputSchemaHelper seq_scan_out2{1, &expr_maker};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema2.GetColumn("col1").Oid(), type::TypeId::SMALLINT);
    auto col2 = expr_maker.CVE(table_schema2.GetColumn("col2").Oid(), type::TypeId::INTEGER);
    seq_scan_out2.AddOutput("col1", col1);
    seq_scan_out2.AddOutput("col2", col2);
    auto schema = seq_scan_out2.MakeSchema();
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(80));
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan2 = builder.SetOutputSchema(std::move(schema))
                    .SetScanPredicate(predicate)
                    .SetIsParallelFlag(false)
                    .SetIsForUpdateFlag(false)
                    .SetNamespaceOid(NSOid())
                    .SetTableOid(table_oid2)
                    .Build();
  }
  // Make nested loop join
  std::unique_ptr<AbstractPlanNode> nl_join;
  OutputSchemaHelper nl_join_out{0, &expr_maker};
  {
    // t1.col1, and t1.col2
    auto t1_col1 = seq_scan_out1.GetOutput("col1");
    // t2.col1 and t2.col2
    auto t2_col1 = seq_scan_out2.GetOutput("col1");
    auto t2_col2 = seq_scan_out2.GetOutput("col2");
    // t1.col2 + t2.col2
    auto sum = expr_maker.OpSum(t1_col1, t2_col2);
    // Output Schema
    nl_join_out.AddOutput("t1.col1", t1_col1);
    nl_join_out.AddOutput("t2.col1", t2_col1);
    nl_join_out.AddOutput("t2.col2", t2_col2);
    nl_join_out.AddOutput("sum", sum);
    auto schema = nl_join_out.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(t1_col1, t2_col1);
    // Build

    NestedLoopJoinPlanNode::Builder builder;
    nl_join = builder.AddChild(std::move(seq_scan1))
                  .AddChild(std::move(seq_scan2))
                  .SetOutputSchema(std::move(schema))
                  .SetJoinType(LogicalJoinType::INNER)
                  .SetJoinPredicate(predicate)
                  .Build();
  }
  // Compile and Run
  // 80 hundred rows should be outputted because of the WHERE clause
  // The joined cols should be equal
  // The 4th column is the sum of the 1nd and 3rd columns
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{80};
  RowChecker row_checker = [&num_output_rows, num_expected_rows](const std::vector<sql::Val *> vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    auto col2 = static_cast<sql::Integer *>(vals[1]);
    auto col3 = static_cast<sql::Integer *>(vals[2]);
    auto col4 = static_cast<sql::Integer *>(vals[3]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_);
    // Check join cols
    ASSERT_EQ(col1->val_, col2->val_);
    // Check that col4 = col1 + col3
    ASSERT_EQ(col4->val_, col1->val_ + col3->val_);
    // Check the number of output row
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  GenericChecker checker(row_checker, correcteness_fn);

  // Make Exec Ctx
  OutputStore store{&checker, nl_join->GetOutputSchema().Get()};
  exec::OutputPrinter printer(nl_join->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), nl_join->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(nl_join.get(), exec_ctx.get());
  checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleIndexNestedLoopJoinTest) {
  // SELECT t1.col1, t2.col1, t2.col2, t1.col2 + t2.col2 FROM test_2 AS t2 INNER JOIN test_1 AS t1 ON t1.col1=t2.col1
  // WHERE t1.col1 < 500 AND t2.col1 < 80
  // Get accessor
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;

  // Make the seq scan: Here test_2 is the outer table
  std::unique_ptr<AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    auto table_oid2 = accessor->GetTableOid(NSOid(), "test_2");
    auto table_schema2 = accessor->GetSchema(table_oid2);
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema2.GetColumn("col1").Oid(), type::TypeId::SMALLINT);
    auto col2 = expr_maker.CVE(table_schema2.GetColumn("col2").Oid(), type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(80));
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(predicate)
                   .SetIsParallelFlag(false)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid2)
                   .Build();
  }
  // Make index join
  std::unique_ptr<AbstractPlanNode> index_join;
  OutputSchemaHelper index_join_out{0, &expr_maker};
  {
    // Retrieve table and index
    auto table_oid1 = accessor->GetTableOid(NSOid(), "test_1");
    auto table_schema1 = accessor->GetSchema(table_oid1);
    auto index_oid1 = accessor->GetIndexOid(NSOid(), "index_1");
    // t1.col1, and t1.col2
    auto t1_col1 = expr_maker.CVE(table_schema1.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    // t2.col1, and t2.col2
    auto t2_col1 = seq_scan_out.GetOutput("col1");
    auto t2_col2 = seq_scan_out.GetOutput("col2");
    // t1.col2 + t2.col2
    auto sum = expr_maker.OpSum(t1_col1, t2_col2);
    // Output Schema
    index_join_out.AddOutput("t1.col1", t1_col1);
    index_join_out.AddOutput("t2.col1", t2_col1);
    index_join_out.AddOutput("t2.col2", t2_col2);
    index_join_out.AddOutput("sum", sum);
    auto schema = index_join_out.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(t1_col1, t2_col1);
    // Build
    IndexJoinPlanNode::Builder builder;
    index_join = builder.AddChild(std::move(seq_scan))
                     .SetIndexOid(index_oid1)
                     .SetTableOid(table_oid1)
                     .AddIndexColum(catalog::indexkeycol_oid_t(1), t2_col1)
                     .SetOutputSchema(std::move(schema))
                     .SetJoinType(LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Compile and Run
  // 80 hundred rows should be outputted because of the WHERE clause
  // The joined cols should be equal
  // The 4th column is the sum of the 1nd and 3rd columns
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{80};
  RowChecker row_checker = [&num_output_rows, num_expected_rows](const std::vector<sql::Val *> vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    auto col2 = static_cast<sql::Integer *>(vals[1]);
    auto col3 = static_cast<sql::Integer *>(vals[2]);
    auto col4 = static_cast<sql::Integer *>(vals[3]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_);
    // Check join cols
    ASSERT_EQ(col1->val_, col2->val_);
    // Check that col4 = col1 + col3
    ASSERT_EQ(col4->val_, col1->val_ + col3->val_);
    // Check the number of output row
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  GenericChecker checker(row_checker, correcteness_fn);

  // Make Exec Ctx
  OutputStore store{&checker, index_join->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_join->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_join->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(index_join.get(), exec_ctx.get());
  checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleIndexNestedLoopJoinMultiColumnTest) {
  // SELECT t1.col1, t2.col1, t2.col2, t1.col2 + t2.col2 FROM test_1 AS t1 INNER JOIN test_2 AS t2 ON t1.col1=t2.col1
  // AND t1.col2 = t2.col2 Get accessor
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;

  // Make the seq scan: Here test_1 is the outer table
  std::unique_ptr<AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    auto table_oid1 = accessor->GetTableOid(NSOid(), "test_1");
    auto table_schema1 = accessor->GetSchema(table_oid1);
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema1.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(table_schema1.GetColumn("colB").Oid(), type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetScanPredicate(nullptr)
                   .SetIsParallelFlag(false)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid1)
                   .Build();
  }
  // Make index join
  std::unique_ptr<AbstractPlanNode> index_join;
  OutputSchemaHelper index_join_out{0, &expr_maker};
  {
    // Retrieve table and index
    auto table_oid2 = accessor->GetTableOid(NSOid(), "test_2");
    auto table_schema2 = accessor->GetSchema(table_oid2);
    auto index_oid2 = accessor->GetIndexOid(NSOid(), "index_2_multi");
    // t2.col1, and t2.col2
    auto t2_col1 = expr_maker.CVE(table_schema2.GetColumn("col1").Oid(), type::TypeId::INTEGER);
    auto t2_col2 = expr_maker.CVE(table_schema2.GetColumn("col2").Oid(), type::TypeId::INTEGER);
    // t1.col1, and t1.col2
    auto t1_col1 = seq_scan_out.GetOutput("col1");
    auto t1_col2 = seq_scan_out.GetOutput("col2");
    // t1.col2 + t2.col2
    auto sum = expr_maker.OpSum(t1_col1, t2_col2);
    // Output Schema
    index_join_out.AddOutput("t1.col1", t1_col1);
    index_join_out.AddOutput("t2.col1", t2_col1);
    index_join_out.AddOutput("t2.col2", t2_col2);
    index_join_out.AddOutput("sum", sum);
    auto schema = index_join_out.MakeSchema();
    // Build
    IndexJoinPlanNode::Builder builder;
    index_join = builder.AddChild(std::move(seq_scan))
                     .SetIndexOid(index_oid2)
                     .SetTableOid(table_oid2)
                     .AddIndexColum(catalog::indexkeycol_oid_t(1), t1_col1)
                     .AddIndexColum(catalog::indexkeycol_oid_t(2), t1_col2)
                     .SetOutputSchema(std::move(schema))
                     .SetJoinType(LogicalJoinType::INNER)
                     .SetJoinPredicate(nullptr)
                     .Build();
  }
  // Compile and Run
  // The joined cols should be equal
  // The 4th column is the sum of the 1nd and 3rd columns
  // With very high probababilty, there should be less 1000 columns outputted due to NULLs.
  uint32_t max_output_rows{1000};
  uint32_t num_output_rows{0};
  RowChecker row_checker = [&num_output_rows, &max_output_rows](const std::vector<sql::Val *> vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    auto col2 = static_cast<sql::Integer *>(vals[1]);
    auto col3 = static_cast<sql::Integer *>(vals[2]);
    auto col4 = static_cast<sql::Integer *>(vals[3]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_);
    // Check join cols
    ASSERT_EQ(col1->val_, col2->val_);
    // Check that col4 = col1 + col3
    ASSERT_EQ(col4->val_, col1->val_ + col3->val_);
    num_output_rows++;
    ASSERT_LT(num_output_rows, max_output_rows);
  };
  CorrectnessFn correcteness_fn;
  GenericChecker checker(row_checker, correcteness_fn);

  // Make Exec Ctx
  OutputStore store{&checker, index_join->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_join->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_join->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(index_join.get(), exec_ctx.get());
  checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleInsertTest) {
  // SELECT COUNT(*) FROM test_1 WHERE test_1.colB == 23
  // INSERT INTO test_1 (colA, colB, colC, colD) VALUES (0,23,2,3)
  // SELECT COUNT(*) FROM test_1 WHERE test_1.colB == 23,
  auto accessor = MakeAccessor();
  auto table_oid1 = accessor->GetTableOid(NSOid(), "test_1");
  auto index_oid1 = accessor->GetIndexOid(NSOid(), "index_1");
  auto table_schema1 = accessor->GetSchema(table_oid1);
  auto col_oid_0 = table_schema1.GetColumn("colA").Oid();
  auto col_oid_1 = table_schema1.GetColumn("colB").Oid();
  auto col_oid_2 = table_schema1.GetColumn("colC").Oid();
  auto col_oid_3 = table_schema1.GetColumn("colD").Oid();
  // Get original count
  std::shared_ptr<AbstractPlanNode> seq_scan1;
  OutputSchemaHelper seq_scan_out1{0};
  {
    auto col1 = ExpressionUtil::CVE(table_schema1.GetColumn("colB").Oid(), type::TypeId::INTEGER);
    seq_scan_out1.AddOutput("col1", col1);
    auto schema = seq_scan_out1.MakeSchema();

    auto predicate = ExpressionUtil::ComparisonEq(col1, ExpressionUtil::Constant(23));
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan1 = builder.SetOutputSchema(schema)
        .SetScanPredicate(predicate)
        .SetIsParallelFlag(false)
        .SetIsForUpdateFlag(false)
        .SetNamespaceOid(NSOid())
        .SetTableOid(table_oid1)
        .Build();
  }



  uint32_t num_rows_count{0};

  exec::OutputCallback counter = [&num_rows_count](byte *data,
      uint32_t num_tuples, uint32_t size){
    num_rows_count = num_tuples;
  };

  uint32_t orig_num_rows = num_rows_count;

  MultiOutputCallback count_callback{std::vector<exec::OutputCallback>{counter}};
  auto count_exec_ctx = MakeExecCtx(std::move(count_callback),
      seq_scan1->GetOutputSchema().get());
  CompileAndRun(seq_scan1.get(), count_exec_ctx.get());

  std::shared_ptr<AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0};
  {
    // Get Table columns
    auto col1 = ExpressionUtil::CVE(table_schema1.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    auto const_0 = ExpressionUtil::Constant(0);
    index_scan_out.AddOutput("col1", col1);
    auto schema = index_scan_out.MakeSchema();
    IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid1).SetIndexOid(index_oid1)
        .AddIndexColum(catalog::indexkeycol_oid_t(1), const_0)
        .SetNamespaceOid(NSOid())
        .SetOutputSchema(schema).Build();
  }
  num_rows_count = 0;

  CompileAndRun(index_scan.get(), count_exec_ctx.get());

  uint32_t orig_index_count = num_rows_count;

  // make InsertPlanNode
  std::shared_ptr<AbstractPlanNode> insert;
  {
    std::vector<type::TransientValue> values;

    values.push_back(type::TransientValueFactory::GetInteger(0));
    values.push_back(type::TransientValueFactory::GetInteger(23));
    values.push_back(type::TransientValueFactory::GetInteger(2));
    values.push_back(type::TransientValueFactory::GetInteger(3));
    InsertPlanNode::Builder builder;
    insert = builder
        .AddParameterInfo(col_oid_0)
        .AddParameterInfo(col_oid_1)
        .AddParameterInfo(col_oid_2)
        .AddParameterInfo(col_oid_3)
        .SetIndexOids({index_oid1})
        .AddValues(std::move(values))
        .SetNamespaceOid(NSOid())
        .SetTableOid(table_oid1)
        .Build();
  }

  MultiOutputCallback callback{std::vector<exec::OutputCallback>{}};
  auto exec_ctx = MakeExecCtx(std::move(callback), insert->GetOutputSchema().get());
  // Run & Check
  CompileAndRun(insert.get(), exec_ctx.get());

  num_rows_count = 0;
  CompileAndRun(seq_scan1.get(), count_exec_ctx.get());
  ASSERT_EQ(num_rows_count, orig_num_rows + 1);

  num_rows_count= 0;
  CompileAndRun(index_scan.get(), count_exec_ctx.get());
  ASSERT_EQ(num_rows_count, orig_index_count + 1);
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, InsertIntoSelectTest) {
  // SELECT COUNT(*) FROM test_1 WHERE test_1.colB == 5
  // INSERT INTO test_1 (colA, colB, colC, colD)
  //  SELECT col2, col2, col2, col2 FROM test_2 WHERE col2 == 5
  // SELECT COUNT(*) FROM test_1 WHERE test_1.colB == 5,
  auto accessor = MakeAccessor();
  auto table_oid1 = accessor->GetTableOid(NSOid(), "test_1");
  auto index_oid1 = accessor->GetIndexOid(NSOid(), "index_1");
  auto table_schema1 = accessor->GetSchema(table_oid1);
  auto col_oid_0 = table_schema1.GetColumn("colA").Oid();
  auto col_oid_1 = table_schema1.GetColumn("colB").Oid();
  auto col_oid_2 = table_schema1.GetColumn("colC").Oid();
  auto col_oid_3 = table_schema1.GetColumn("colD").Oid();
  // Get original count
  std::shared_ptr<AbstractPlanNode> seq_scan1;
  OutputSchemaHelper seq_scan_out1{0};
  {
    auto col1 = ExpressionUtil::CVE(table_schema1.GetColumn("colB").Oid(), type::TypeId::INTEGER);
    seq_scan_out1.AddOutput("col1", col1);
    auto schema = seq_scan_out1.MakeSchema();

    auto predicate = ExpressionUtil::ComparisonEq(col1, ExpressionUtil::Constant(5));
    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan1 = builder.SetOutputSchema(schema)
        .SetScanPredicate(predicate)
        .SetIsParallelFlag(false)
        .SetIsForUpdateFlag(false)
        .SetNamespaceOid(NSOid())
        .SetTableOid(table_oid1)
        .Build();
  }



  uint32_t num_rows_count{0};

  exec::OutputCallback counter = [&num_rows_count](byte *data,
                                                   uint32_t num_tuples, uint32_t size){
    num_rows_count = num_tuples;
  };


  MultiOutputCallback count_callback{std::vector<exec::OutputCallback>{counter}};
  auto count_exec_ctx = MakeExecCtx(std::move(count_callback),
                                    seq_scan1->GetOutputSchema().get());
  CompileAndRun(seq_scan1.get(), count_exec_ctx.get());
  uint32_t orig_num_rows = num_rows_count;

  std::shared_ptr<AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0};
  {
    // Get Table columns
    auto col1 = ExpressionUtil::CVE(table_schema1.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    auto const_0 = ExpressionUtil::Constant(5);
    index_scan_out.AddOutput("col1", col1);
    auto schema = index_scan_out.MakeSchema();
    IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid1).SetIndexOid(index_oid1)
        .AddIndexColum(catalog::indexkeycol_oid_t(1), const_0)
        .SetNamespaceOid(NSOid())
        .SetOutputSchema(schema).Build();
  }
  num_rows_count = 0;

  CompileAndRun(index_scan.get(), count_exec_ctx.get());

  uint32_t orig_index_count = num_rows_count;

  // scan node for SELECT part of INSERT INTO SELECT

  auto table_oid2 = accessor->GetTableOid(NSOid(), "test_2");
  auto table_schema2 = accessor->GetSchema(table_oid2);
  std::shared_ptr<AbstractPlanNode> insert_seq_scan;

  OutputSchemaHelper insert_seq_scan1{0};
  {
    auto col1 = ExpressionUtil::CVE(table_schema2.GetColumn("col2").Oid(), type::TypeId::INTEGER);
    insert_seq_scan1.AddOutput("colA", col1);
    insert_seq_scan1.AddOutput("colB", col1);
    insert_seq_scan1.AddOutput("colC", col1);
    insert_seq_scan1.AddOutput("colD", col1);
    auto schema = insert_seq_scan1.MakeSchema();

    auto predicate = ExpressionUtil::ComparisonEq(col1, ExpressionUtil::Constant(5));
    // Build
    SeqScanPlanNode::Builder builder;
    insert_seq_scan = builder.SetOutputSchema(schema)
        .SetScanPredicate(predicate)
        .SetIsParallelFlag(false)
        .SetIsForUpdateFlag(false)
        .SetNamespaceOid(NSOid())
        .SetTableOid(table_oid2)
        .Build();
  }

  // make InsertPlanNode
  std::shared_ptr<AbstractPlanNode> insert;
  {
    std::vector<type::TransientValue> values;

    InsertPlanNode::Builder builder;
    insert = builder
        .AddParameterInfo(col_oid_0)
        .AddParameterInfo(col_oid_1)
        .AddParameterInfo(col_oid_2)
        .AddParameterInfo(col_oid_3)
        .SetIndexOids({index_oid1})
        .SetNamespaceOid(NSOid())
        .SetTableOid(table_oid1)
        .AddChild(insert_seq_scan)
        .Build();
  }

  MultiOutputCallback callback{std::vector<exec::OutputCallback>{}};
  auto exec_ctx = MakeExecCtx(std::move(callback), insert->GetOutputSchema().get());
  // Run & Check
  CompileAndRun(insert.get(), exec_ctx.get());

  size_t num_rows_inserted = 0;
  exec::OutputCallback new_counter = [&num_rows_inserted](byte *data,
                                                   uint32_t num_tuples, uint32_t size){
    num_rows_inserted = num_tuples;
  };

  MultiOutputCallback n_count_callback{std::vector<exec::OutputCallback>{new_counter}};
  auto n_count_exec_ctx = MakeExecCtx(std::move(n_count_callback),
                                    insert_seq_scan->GetOutputSchema().get());
  CompileAndRun(insert_seq_scan.get(), n_count_exec_ctx.get());

  num_rows_count = 0;
  CompileAndRun(seq_scan1.get(), count_exec_ctx.get());
  ASSERT_EQ(num_rows_count, orig_num_rows + num_rows_inserted);

  num_rows_count= 0;
  CompileAndRun(index_scan.get(), count_exec_ctx.get());
  ASSERT_EQ(num_rows_count, orig_index_count + num_rows_inserted);
}

/*
// NOLINTNEXTLINE
TEST_F(CompilerTest, TPCHQ1Test) {
  // TODO: This should be in the benchmarks
  // Find a cleaner way to create these tables
  auto exec_ctx = MakeExecCtx();
  ExpressionMaker expr_maker;
  sql::TableGenerator generator(exec_ctx.get());
  generator.GenerateTableFromFile("../sample_tpl/tables/lineitem.schema", "../sample_tpl/tables/lineitem.data");

  // Get the table from the catalog
  auto accessor = MakeAccessor();
  auto * catalog_table = accessor->GetUserTable("lineitem");
  // Scan the table
  std::unique_ptr<AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // Read all needed columns
    auto l_returnflag = expr_maker.TVE(0, catalog_table->ColNumToOffset(8), terrier::type::TypeId::VARCHAR);
    auto l_linestatus = expr_maker.TVE(0, catalog_table->ColNumToOffset(9), terrier::type::TypeId::VARCHAR);
    auto l_extendedprice = expr_maker.TVE(0, catalog_table->ColNumToOffset(5), terrier::type::TypeId::DECIMAL);
    auto l_discount = expr_maker.TVE(0, catalog_table->ColNumToOffset(6), terrier::type::TypeId::DECIMAL);
    auto l_tax = expr_maker.TVE(0, catalog_table->ColNumToOffset(7), terrier::type::TypeId::DECIMAL);
    auto l_quantity = expr_maker.TVE(0, catalog_table->ColNumToOffset(4), terrier::type::TypeId::DECIMAL);
    auto l_shipdate = expr_maker.TVE(0, catalog_table->ColNumToOffset(10), terrier::type::TypeId::DATE);
    // Make the output schema
    seq_scan_out.AddOutput("l_returnflag", l_returnflag);
    seq_scan_out.AddOutput("l_linestatus", l_linestatus);
    seq_scan_out.AddOutput("l_extendedprice", l_extendedprice);
    seq_scan_out.AddOutput("l_discount", l_discount);
    seq_scan_out.AddOutput("l_tax", l_tax);
    seq_scan_out.AddOutput("l_quantity", l_quantity);
    auto schema = seq_scan_out.MakeSchema();

    // Make the predicate
    seq_scan_out.AddOutput("l_shipdate", l_shipdate);
    auto date_val = (date::sys_days(date::year(1998)/12/01)) - date::days(90);
    auto date_const = expr_maker.Constant(date::year_month_day(date_val));
    auto predicate = expr_maker.ComparisonLt(l_shipdate, date_const);

    // Build
    SeqScanPlanNode::Builder builder;
    seq_scan =
        builder.SetOutputSchema(std::move(schema))
            .SetScanPredicate(nullptr)
            .SetIsParallelFlag(false)
            .SetIsForUpdateFlag(false)
            .SetDatabaseOid(accessor->GetDBOid())
            .SetNamespaceOid(accessor->GetNSOid())
            .SetTableOid(catalog_table->Oid())
            .Build();
  }
  // Make the aggregate
  std::unique_ptr<AbstractPlanNode> agg;
  OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous layer's output
    auto l_returnflag = seq_scan_out.GetOutput("l_returnflag");
    auto l_linestatus = seq_scan_out.GetOutput("l_linestatus");
    auto l_quantity = seq_scan_out.GetOutput("l_quantity");
    auto l_extendedprice = seq_scan_out.GetOutput("l_extendedprice");
    auto l_discount = seq_scan_out.GetOutput("l_discount");
    auto l_tax = seq_scan_out.GetOutput("l_tax");
    // Make the aggregate expressions
    auto sum_qty = expr_maker.AggSum(l_quantity);
    auto sum_base_price = expr_maker.AggSum(l_extendedprice);
    auto one_const = expr_maker.Constant(1.0);
    auto disc_price = expr_maker.OpMul(l_extendedprice, expr_maker.OpSum(one_const, l_discount));
    auto sum_disc_price = expr_maker.AggSum(disc_price);
    auto charge = expr_maker.OpMul(disc_price, expr_maker.OpSum(one_const, l_tax));
    auto sum_charge = expr_maker.AggSum(charge);
    auto avg_qty = expr_maker.AggAvg(l_quantity);
    auto avg_price = expr_maker.AggAvg(l_extendedprice);
    auto avg_disc = expr_maker.AggAvg(l_discount);
    auto count_order = expr_maker.AggCountStar();
    // Add them to the helper.
    agg_out.AddGroupByTerm("l_returnflag", l_returnflag);
    agg_out.AddGroupByTerm("l_linestatus", l_linestatus);
    agg_out.AddAggTerm("sum_qty", sum_qty);
    agg_out.AddAggTerm("sum_base_price", sum_base_price);
    agg_out.AddAggTerm("sum_disc_price", sum_disc_price);
    agg_out.AddAggTerm("sum_charge", sum_charge);
    agg_out.AddAggTerm("avg_qty", avg_qty);
    agg_out.AddAggTerm("avg_price", avg_price);
    agg_out.AddAggTerm("avg_disc", avg_disc);
    agg_out.AddAggTerm("count_order", count_order);
    // Make the output schema
    agg_out.AddOutput("l_returnflag", agg_out.GetGroupByTermForOutput("l_returnflag"));
    agg_out.AddOutput("l_linestatus", agg_out.GetGroupByTermForOutput("l_linestatus"));
    agg_out.AddOutput("sum_qty", agg_out.GetAggTermForOutput("sum_qty"));
    agg_out.AddOutput("sum_base_price", agg_out.GetAggTermForOutput("sum_base_price"));
    agg_out.AddOutput("sum_disc_price", agg_out.GetAggTermForOutput("sum_disc_price"));
    agg_out.AddOutput("sum_charge", agg_out.GetAggTermForOutput("sum_charge"));
    agg_out.AddOutput("avg_qty", agg_out.GetAggTermForOutput("avg_qty"));
    agg_out.AddOutput("avg_price", agg_out.GetAggTermForOutput("avg_price"));
    agg_out.AddOutput("avg_disc", agg_out.GetAggTermForOutput("avg_disc"));
    agg_out.AddOutput("count_order", agg_out.GetAggTermForOutput("count_order"));
    auto schema = agg_out.MakeSchema();
    // Build
    AggregatePlanNode::Builder builder;
    agg =
        builder.SetOutputSchema(std::move(schema))
            .AddGroupByTerm(l_returnflag)
            .AddGroupByTerm(l_linestatus)
            .AddAggregateTerm(sum_qty)
            .AddAggregateTerm(sum_base_price)
            .AddAggregateTerm(sum_disc_price)
            .AddAggregateTerm(sum_charge)
            .AddAggregateTerm(avg_qty)
            .AddAggregateTerm(avg_price)
            .AddAggregateTerm(avg_disc)
            .AddAggregateTerm(count_order)
            .AddChild(std::move(seq_scan))
            .SetAggregateStrategyType(AggregateStrategyType::HASH)
            .SetHavingClausePredicate(nullptr)
            .Build();
  }
  // Compile and Run
  // TODO(How to auto check this test?)
  CorrectnessFn correcteness_fn;
  RowChecker row_checker;
  GenericChecker checker(row_checker, correcteness_fn);
  // Make Exec Ctx
  OutputStore store{&checker, agg->GetOutputSchema().Get()};
  exec::OutputPrinter printer(agg->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  exec_ctx = MakeExecCtx(std::move(callback), agg->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(agg.get(), exec_ctx.get());
  checker.CheckCorrectness();
}
*/
}  // namespace terrier::execution::compiler::test

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  terrier::execution::compiler::test::CompilerTest::InitTPL();
  int ret = RUN_ALL_TESTS();
  terrier::execution::compiler::test::CompilerTest::ShutdownTPL();
  return ret;
}