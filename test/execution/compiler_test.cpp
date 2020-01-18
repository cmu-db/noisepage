#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/ast/ast_dump.h"
#include "execution/compiler/compiler.h"
#include "execution/compiler/expression_util.h"
#include "execution/compiler/output_checker.h"
#include "execution/compiler/output_schema_util.h"
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
#include "planner/plannodes/delete_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

namespace terrier::execution::compiler::test {
class CompilerTest : public SqlBasedTest {
 public:
  void SetUp() override {
    SqlBasedTest::SetUp();
    // Make the test tables
    auto exec_ctx = MakeExecCtx();
    sql::TableGenerator table_generator{exec_ctx.get(), BlockStore(), NSOid()};
    table_generator.GenerateTestTables();
  }

  static void CompileAndRun(planner::AbstractPlanNode *node, exec::ExecutionContext *exec_ctx) {
    // Create the query object, whose region must outlive all the processing.
    // Compile and check for errors
    CodeGen codegen(exec_ctx);
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
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
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
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid)
                   .Build();
  }

  // Make the output checkers
  SingleIntComparisonChecker col1_checker(std::less<>(), 0, 500);
  SingleIntComparisonChecker col2_checker(std::greater_equal<>(), 1, 3);

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
TEST_F(CompilerTest, SimpleSeqScanWithProjectionTest) {
  // SELECT col1, col2, col1 * col2, col1 >= 100*col2 FROM test_1 WHERE col1 < 500 AND col2 >= 3;
  // This test first selects col1 and col2, and then constructs the 4 output columns.
  auto accessor = MakeAccessor();
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table_schema = accessor->GetSchema(table_oid);
  ExpressionMaker expr_maker;
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // Get Table columns
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    // Make New Column
    seq_scan_out.AddOutput("col1", common::ManagedPointer(col1));
    seq_scan_out.AddOutput("col2", common::ManagedPointer(col2));
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto comp1 = expr_maker.ComparisonLt(col1, expr_maker.Constant(500));
    auto comp2 = expr_maker.ComparisonGe(col2, expr_maker.Constant(3));
    auto predicate = expr_maker.ConjunctionAnd(comp1, comp2);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid)
                   .Build();
  }
  std::unique_ptr<planner::AbstractPlanNode> proj;
  OutputSchemaHelper proj_out{0, &expr_maker};
  {
    auto col1 = seq_scan_out.GetOutput("col1");
    auto col2 = seq_scan_out.GetOutput("col2");
    auto col3 = expr_maker.OpMul(col1, col2);
    auto col4 = expr_maker.ComparisonGe(col1, expr_maker.OpMul(expr_maker.Constant(100), col2));
    proj_out.AddOutput("col1", common::ManagedPointer(col1));
    proj_out.AddOutput("col2", common::ManagedPointer(col2));
    proj_out.AddOutput("col3", common::ManagedPointer(col3));
    proj_out.AddOutput("col4", common::ManagedPointer(col4));
    auto schema = proj_out.MakeSchema();
    planner::ProjectionPlanNode::Builder builder;
    proj = builder.SetOutputSchema(std::move(schema)).AddChild(std::move(seq_scan)).Build();
  }

  // Make the output checkers
  SingleIntComparisonChecker col1_checker(std::less<>(), 0, 500);
  SingleIntComparisonChecker col2_checker(std::greater_equal<>(), 1, 3);

  MultiChecker multi_checker{std::vector<OutputChecker *>{&col1_checker, &col2_checker}};

  // Create the execution context
  OutputStore store{&multi_checker, proj->GetOutputSchema().Get()};
  exec::OutputPrinter printer(proj->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), proj->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(proj.get(), exec_ctx.get());
  multi_checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleSeqScanWithParamsTest) {
  // SELECT col1, col2, col1 * col2, col1 >= param1*col2 FROM test_1 WHERE col1 < param2 AND col2 >= param3;
  // param1 = 100; param2 = 500; param3 = 3
  auto accessor = MakeAccessor();
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table_schema = accessor->GetSchema(table_oid);
  ExpressionMaker expr_maker;
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    // Make New Column
    auto col3 = expr_maker.OpMul(col1, col2);
    auto param1 = expr_maker.PVE(type::TypeId::INTEGER, 0);
    auto col4 = expr_maker.ComparisonGe(col1, expr_maker.OpMul(param1, col2));
    seq_scan_out.AddOutput("col1", common::ManagedPointer(col1));
    seq_scan_out.AddOutput("col2", common::ManagedPointer(col2));
    seq_scan_out.AddOutput("col3", common::ManagedPointer(col3));
    seq_scan_out.AddOutput("col4", common::ManagedPointer(col4));
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto param2 = expr_maker.PVE(type::TypeId::INTEGER, 1);
    auto param3 = expr_maker.PVE(type::TypeId::INTEGER, 2);
    auto comp1 = expr_maker.ComparisonLt(col1, param2);
    auto comp2 = expr_maker.ComparisonGe(col2, param3);
    auto predicate = expr_maker.ConjunctionAnd(comp1, comp2);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid)
                   .Build();
  }

  // Make the output checkers
  SingleIntComparisonChecker col1_checker(std::less<>(), 0, 500);
  SingleIntComparisonChecker col2_checker(std::greater_equal<>(), 1, 3);

  MultiChecker multi_checker{std::vector<OutputChecker *>{&col1_checker, &col2_checker}};

  // Create the execution context
  OutputStore store{&multi_checker, seq_scan->GetOutputSchema().Get()};
  exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get());
  std::vector<type::TransientValue> params;
  params.emplace_back(type::TransientValueFactory::GetInteger(100));
  params.emplace_back(type::TransientValueFactory::GetInteger(500));
  params.emplace_back(type::TransientValueFactory::GetInteger(3));
  exec_ctx->SetParams(std::move(params));

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
  std::unique_ptr<planner::AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    auto const_500 = expr_maker.Constant(500);
    index_scan_out.AddOutput("col1", col1);
    index_scan_out.AddOutput("col2", col2);
    auto schema = index_scan_out.MakeSchema();
    planner::IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid)
                     .SetColumnOids({cola_oid, colb_oid})
                     .SetIndexOid(index_oid)
                     .AddIndexColumn(catalog::indexkeycol_oid_t(1), const_500)
                     .SetNamespaceOid(NSOid())
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::Exact)
                     .SetScanLimit(0)
                     .SetScanPredicate(nullptr)
                     .Build();
  }
  NumChecker num_checker(1);
  SingleIntComparisonChecker col1_checker(std::equal_to<>(), 0, 500);
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
TEST_F(CompilerTest, SimpleIndexScanAsendingTest) {
  // SELECT colA, colB FROM test_1 WHERE colA BETWEEN 495 AND 505;
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto index_oid = accessor->GetIndexOid(NSOid(), "index_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<planner::AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    index_scan_out.AddOutput("col1", col1);
    index_scan_out.AddOutput("col2", col2);
    auto schema = index_scan_out.MakeSchema();
    planner::IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid)
                     .SetColumnOids({cola_oid, colb_oid})
                     .SetIndexOid(index_oid)
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(495))
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(505))
                     .SetNamespaceOid(NSOid())
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::Ascending)
                     .SetScanLimit(0)
                     .SetScanPredicate(nullptr)

                     .Build();
  }

  // Make the checker
  uint32_t num_output_rows = 0;
  uint32_t num_expected_rows = 11;
  int64_t curr_col1{std::numeric_limits<int64_t>::min()};
  RowChecker row_checker = [&num_output_rows, num_expected_rows, &curr_col1](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    auto col2 = static_cast<sql::Integer *>(vals[1]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_);
    // Check col1 and number of outputs
    int32_t col1_val = 495 + num_output_rows;
    ASSERT_EQ(col1->val_, col1_val);
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);

    // Check that output is sorted by col1 ASC
    ASSERT_LE(curr_col1, col1->val_);
    curr_col1 = col1->val_;
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  GenericChecker checker(row_checker, correcteness_fn);
  // Create the execution context
  OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(index_scan.get(), exec_ctx.get());
  checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleIndexScanLimitAsendingTest) {
  // SELECT colA, colB FROM test_1 WHERE colA BETWEEN 495 AND 505;
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto index_oid = accessor->GetIndexOid(NSOid(), "index_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<planner::AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    index_scan_out.AddOutput("col1", col1);
    index_scan_out.AddOutput("col2", col2);
    auto schema = index_scan_out.MakeSchema();
    planner::IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid)
                     .SetColumnOids({cola_oid, colb_oid})
                     .SetIndexOid(index_oid)
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(495))
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(505))
                     .SetNamespaceOid(NSOid())
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::AscendingLimit)
                     .SetScanLimit(5)
                     .SetScanPredicate(nullptr)
                     .Build();
  }

  // Make the checker
  uint32_t num_output_rows = 0;
  uint32_t num_expected_rows = 5;
  int64_t curr_col1{std::numeric_limits<int64_t>::min()};
  RowChecker row_checker = [&num_output_rows, num_expected_rows, &curr_col1](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    auto col2 = static_cast<sql::Integer *>(vals[1]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_);
    // Check col1 and number of outputs
    int32_t col1_val = 495 + num_output_rows;
    ASSERT_EQ(col1->val_, col1_val);
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);

    // Check that output is sorted by col1 ASC
    ASSERT_LE(curr_col1, col1->val_);
    curr_col1 = col1->val_;
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  GenericChecker checker(row_checker, correcteness_fn);
  // Create the execution context
  OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(index_scan.get(), exec_ctx.get());
  checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleIndexScanDesendingTest) {
  // SELECT colA, colB FROM test_1 WHERE colA BETWEEN 495 AND 505;
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto index_oid = accessor->GetIndexOid(NSOid(), "index_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<planner::AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    index_scan_out.AddOutput("col1", col1);
    index_scan_out.AddOutput("col2", col2);
    auto schema = index_scan_out.MakeSchema();
    planner::IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid)
                     .SetColumnOids({cola_oid, colb_oid})
                     .SetIndexOid(index_oid)
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(495))
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(505))
                     .SetNamespaceOid(NSOid())
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::Descending)
                     .SetScanLimit(0)
                     .SetScanPredicate(nullptr)
                     .Build();
  }

  // Make the checker
  uint32_t num_output_rows = 0;
  uint32_t num_expected_rows = 11;
  int64_t curr_col1{std::numeric_limits<int64_t>::max()};
  RowChecker row_checker = [&num_output_rows, num_expected_rows, &curr_col1](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    auto col2 = static_cast<sql::Integer *>(vals[1]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_);
    // Check col1 and number of outputs
    int32_t col1_val = 505 - num_output_rows;
    ASSERT_EQ(col1->val_, col1_val);
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);

    // Check that output is sorted by col1 DESC
    ASSERT_GE(curr_col1, col1->val_);
    curr_col1 = col1->val_;
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  GenericChecker checker(row_checker, correcteness_fn);
  // Create the execution context
  OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(index_scan.get(), exec_ctx.get());
  checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleIndexScanLimitDesendingTest) {
  // SELECT colA, colB FROM test_1 WHERE colA BETWEEN 495 AND 505;
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto index_oid = accessor->GetIndexOid(NSOid(), "index_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<planner::AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    index_scan_out.AddOutput("col1", col1);
    index_scan_out.AddOutput("col2", col2);
    auto schema = index_scan_out.MakeSchema();
    planner::IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid)
                     .SetColumnOids({cola_oid, colb_oid})
                     .SetIndexOid(index_oid)
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(495))
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(505))
                     .SetNamespaceOid(NSOid())
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::DescendingLimit)
                     .SetScanLimit(5)
                     .SetScanPredicate(nullptr)
                     .Build();
  }

  // Make the checker
  uint32_t num_output_rows = 0;
  uint32_t num_expected_rows = 5;
  int64_t curr_col1{std::numeric_limits<int64_t>::max()};
  RowChecker row_checker = [&num_output_rows, num_expected_rows, &curr_col1](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    auto col2 = static_cast<sql::Integer *>(vals[1]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_);
    // Check col1 and number of outputs
    int32_t col1_val = 505 - num_output_rows;
    ASSERT_EQ(col1->val_, col1_val);
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);

    // Check that output is sorted by col1 DESC
    ASSERT_GE(curr_col1, col1->val_);
    curr_col1 = col1->val_;
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  GenericChecker checker(row_checker, correcteness_fn);
  // Create the execution context
  OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get());

  // Run & Check
  CompileAndRun(index_scan.get(), exec_ctx.get());
  checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleAggregateTest) {
  // SELECT col2, SUM(col1) FROM test_1 WHERE col1 < 1000 GROUP BY col2;
  // Get accessor
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(1000));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
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
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(agg_out.GetGroupByTerm("col2"))
              .AddAggregateTerm(agg_out.GetAggTerm("col1"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
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
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(1000));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
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
    auto having2 = expr_maker.ComparisonLt(agg_out.GetAggTermForOutput("sum_col1"), expr_maker.Constant(50000));
    auto having = expr_maker.ConjunctionAnd(having1, having2);
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(agg_out.GetGroupByTerm("col2"))
              .AddAggregateTerm(agg_out.GetAggTerm("col1"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(having)
              .Build();
  }
  // Make the checkers
  RowChecker row_checker = [](const std::vector<sql::Val *> &vals) {
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

  std::unique_ptr<planner::AbstractPlanNode> seq_scan1;
  OutputSchemaHelper seq_scan_out1{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema1.GetColumn("colA").Oid();
    auto colb_oid = table_schema1.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    seq_scan_out1.AddOutput("col1", col1);
    seq_scan_out1.AddOutput("col2", col2);
    auto schema = seq_scan_out1.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(1000));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan1 = builder.SetOutputSchema(std::move(schema))
                    .SetColumnOids({cola_oid, colb_oid})
                    .SetScanPredicate(predicate)
                    .SetIsForUpdateFlag(false)
                    .SetNamespaceOid(NSOid())
                    .SetTableOid(table_oid1)
                    .Build();
  }
  // Make the second seq scan
  std::unique_ptr<planner::AbstractPlanNode> seq_scan2;
  OutputSchemaHelper seq_scan_out2{1, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema2.GetColumn("col1").Oid();
    auto colb_oid = table_schema2.GetColumn("col2").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::SMALLINT);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    seq_scan_out2.AddOutput("col1", col1);
    seq_scan_out2.AddOutput("col2", col2);
    auto schema = seq_scan_out2.MakeSchema();
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(80));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan2 = builder.SetOutputSchema(std::move(schema))
                    .SetColumnOids({cola_oid, colb_oid})
                    .SetScanPredicate(predicate)
                    .SetIsForUpdateFlag(false)
                    .SetNamespaceOid(NSOid())
                    .SetTableOid(table_oid2)
                    .Build();
  }
  // Make hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join;
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
    planner::HashJoinPlanNode::Builder builder;
    hash_join = builder.AddChild(std::move(seq_scan1))
                    .AddChild(std::move(seq_scan2))
                    .SetOutputSchema(std::move(schema))
                    .AddLeftHashKey(t1_col1)
                    .AddRightHashKey(t2_col1)
                    .SetJoinType(planner::LogicalJoinType::INNER)
                    .SetJoinPredicate(predicate)
                    .Build();
  }
  // Compile and Run
  // 80 hundred rows should be outputted because of the WHERE clause
  // The joined cols should be equal
  // The 4th column is the sum of the 1nd and 3rd columns
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{80};
  RowChecker row_checker = [&num_output_rows, num_expected_rows](const std::vector<sql::Val *> &vals) {
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
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(500));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
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
    planner::SortKey clause1{col2, optimizer::OrderByOrderingType::ASC};
    auto diff = expr_maker.OpMin(col1, col2);
    planner::SortKey clause2{diff, optimizer::OrderByOrderingType::DESC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(seq_scan))
                   .AddSortKey(clause1.first, clause1.second)
                   .AddSortKey(clause2.first, clause2.second)
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
                            num_expected_rows](const std::vector<sql::Val *> &vals) {
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

  std::unique_ptr<planner::AbstractPlanNode> seq_scan1;
  OutputSchemaHelper seq_scan_out1{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema1.GetColumn("colA").Oid();
    auto colb_oid = table_schema1.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    seq_scan_out1.AddOutput("col1", col1);
    seq_scan_out1.AddOutput("col2", col2);
    auto schema = seq_scan_out1.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(1000));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan1 = builder.SetOutputSchema(std::move(schema))
                    .SetColumnOids({cola_oid, colb_oid})
                    .SetScanPredicate(predicate)
                    .SetIsForUpdateFlag(false)
                    .SetNamespaceOid(NSOid())
                    .SetTableOid(table_oid1)
                    .Build();
  }
  // Make the second seq scan
  std::unique_ptr<planner::AbstractPlanNode> seq_scan2;
  OutputSchemaHelper seq_scan_out2{1, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema2.GetColumn("col1").Oid();
    auto colb_oid = table_schema2.GetColumn("col2").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::SMALLINT);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    seq_scan_out2.AddOutput("col1", col1);
    seq_scan_out2.AddOutput("col2", col2);
    auto schema = seq_scan_out2.MakeSchema();
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(80));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan2 = builder.SetOutputSchema(std::move(schema))
                    .SetColumnOids({cola_oid, colb_oid})
                    .SetScanPredicate(predicate)
                    .SetIsForUpdateFlag(false)
                    .SetNamespaceOid(NSOid())
                    .SetTableOid(table_oid2)
                    .Build();
  }
  // Make nested loop join
  std::unique_ptr<planner::AbstractPlanNode> nl_join;
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

    planner::NestedLoopJoinPlanNode::Builder builder;
    nl_join = builder.AddChild(std::move(seq_scan1))
                  .AddChild(std::move(seq_scan2))
                  .SetOutputSchema(std::move(schema))
                  .SetJoinType(planner::LogicalJoinType::INNER)
                  .SetJoinPredicate(predicate)
                  .Build();
  }
  // Compile and Run
  // 80 hundred rows should be outputted because of the WHERE clause
  // The joined cols should be equal
  // The 4th column is the sum of the 1nd and 3rd columns
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{80};
  RowChecker row_checker = [&num_output_rows, num_expected_rows](const std::vector<sql::Val *> &vals) {
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
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    auto table_oid2 = accessor->GetTableOid(NSOid(), "test_2");
    auto table_schema2 = accessor->GetSchema(table_oid2);
    // OIDs
    auto cola_oid = table_schema2.GetColumn("col1").Oid();
    auto colb_oid = table_schema2.GetColumn("col2").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(80));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid2)
                   .Build();
  }
  // Make index join
  std::unique_ptr<planner::AbstractPlanNode> index_join;
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
    planner::IndexJoinPlanNode::Builder builder;
    index_join = builder.AddChild(std::move(seq_scan))
                     .SetIndexOid(index_oid1)
                     .SetTableOid(table_oid1)
                     .AddIndexColumn(catalog::indexkeycol_oid_t(1), t2_col1)
                     .SetOutputSchema(std::move(schema))
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Compile and Run
  // 80 hundred rows should be outputted because of the WHERE clause
  // The joined cols should be equal
  // The 4th column is the sum of the 1nd and 3rd columns
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{80};
  RowChecker row_checker = [&num_output_rows, num_expected_rows](const std::vector<sql::Val *> &vals) {
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
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    auto table_oid1 = accessor->GetTableOid(NSOid(), "test_1");
    auto table_schema1 = accessor->GetSchema(table_oid1);
    // OIDs
    auto cola_oid = table_schema1.GetColumn("colA").Oid();
    auto colb_oid = table_schema1.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid})
                   .SetScanPredicate(nullptr)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid1)
                   .Build();
  }
  // Make index join
  std::unique_ptr<planner::AbstractPlanNode> index_join;
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
    planner::IndexJoinPlanNode::Builder builder;
    index_join = builder.AddChild(std::move(seq_scan))
                     .SetIndexOid(index_oid2)
                     .SetTableOid(table_oid2)
                     .AddIndexColumn(catalog::indexkeycol_oid_t(1), t1_col1)
                     .AddIndexColumn(catalog::indexkeycol_oid_t(2), t1_col2)
                     .SetOutputSchema(std::move(schema))
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(nullptr)
                     .Build();
  }
  // Compile and Run
  // The joined cols should be equal
  // The 4th column is the sum of the 1nd and 3rd columns
  // With very high probababilty, there should be less 1000 columns outputted due to NULLs.
  uint32_t max_output_rows{1000};
  uint32_t num_output_rows{0};
  RowChecker row_checker = [&num_output_rows, &max_output_rows](const std::vector<sql::Val *> &vals) {
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
TEST_F(CompilerTest, SimpleDeleteTest) {
  // DELETE FROM test_1 WHERE colA BETWEEN 495 AND 505
  // Then check that the following finds the no tuples:
  // SELECT * FROM test_1 WHERE colA BETWEEN 495 AND 505.
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid1 = accessor->GetTableOid(NSOid(), "test_1");
  auto index_oid1 = accessor->GetIndexOid(NSOid(), "index_1");
  auto table_schema1 = accessor->GetSchema(table_oid1);

  // SeqScan for delete
  std::unique_ptr<planner::AbstractPlanNode> seq_scan1;
  OutputSchemaHelper seq_scan_out1{0, &expr_maker};
  {
    auto col1 = expr_maker.CVE(table_schema1.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    seq_scan_out1.AddOutput("col1", col1);
    auto schema = seq_scan_out1.MakeSchema();

    auto pred1 = expr_maker.ComparisonGe(col1, expr_maker.Constant(495));
    auto pred2 = expr_maker.ComparisonLe(col1, expr_maker.Constant(505));
    auto predicate = expr_maker.ConjunctionAnd(pred1, pred2);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan1 = builder.SetOutputSchema(std::move(schema))
                    .SetColumnOids({table_schema1.GetColumn("colA").Oid()})
                    .SetScanPredicate(predicate)
                    .SetIsForUpdateFlag(false)
                    .SetNamespaceOid(NSOid())
                    .SetTableOid(table_oid1)
                    .Build();
  }

  // make DeletePlanNode
  std::unique_ptr<planner::AbstractPlanNode> delete_node;
  {
    planner::DeletePlanNode::Builder builder;
    delete_node = builder.SetTableOid(table_oid1).AddChild(std::move(seq_scan1)).Build();
  }
  // Execute delete
  {
    // Make Exec Ctx
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{}};
    auto exec_ctx = MakeExecCtx(std::move(callback), delete_node->GetOutputSchema().Get());
    CompileAndRun(delete_node.get(), exec_ctx.get());
  }

  // Now scan through table to check content.
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema1.GetColumn("colA").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    // Make predicate
    auto pred1 = expr_maker.ComparisonGe(col1, expr_maker.Constant(495));
    auto pred2 = expr_maker.ComparisonLe(col1, expr_maker.Constant(505));
    auto predicate = expr_maker.ConjunctionAnd(pred1, pred2);
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid1)
                   .Build();
  }
  // Execute Table Scan
  {
    NumChecker checker{0};
    OutputStore store{&checker, seq_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get());
    CompileAndRun(seq_scan.get(), exec_ctx.get());
    checker.CheckCorrectness();
  }

  // Do an index scan to look a deleted value
  std::unique_ptr<planner::AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0, &expr_maker};
  {
    // Get Table columns
    auto col1 = expr_maker.CVE(table_schema1.GetColumn("colA").Oid(), type::TypeId::INTEGER);

    index_scan_out.AddOutput("col1", col1);
    auto schema = index_scan_out.MakeSchema();
    planner::IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid1)
                     .SetColumnOids({table_schema1.GetColumn("colA").Oid()})
                     .SetIndexOid(index_oid1)
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(495))
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(505))
                     .SetScanPredicate(nullptr)
                     .SetNamespaceOid(NSOid())
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::Ascending)
                     .SetScanLimit(0)
                     .Build();
  }

  // Execute index scan
  {
    NumChecker checker{0};
    OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get());
    CompileAndRun(index_scan.get(), exec_ctx.get());
    checker.CheckCorrectness();
  }
}

TEST_F(CompilerTest, SimpleUpdateTest) {
  // UPDATE test_1 SET colA = -colA, colB = 500 WHERE colA BETWEEN 495 AND 505
  // Then check that the following finds the tuples:
  // SELECT * FROM test_1 WHERE colA BETWEEN -505 AND -495.
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid1 = accessor->GetTableOid(NSOid(), "test_1");
  auto index_oid1 = accessor->GetIndexOid(NSOid(), "index_1");
  auto table_schema1 = accessor->GetSchema(table_oid1);

  // SeqScan for delete
  std::unique_ptr<planner::AbstractPlanNode> seq_scan1;
  OutputSchemaHelper seq_scan_out1{0, &expr_maker};
  {
    auto col1 = expr_maker.CVE(table_schema1.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(table_schema1.GetColumn("colB").Oid(), type::TypeId::INTEGER);
    auto col3 = expr_maker.CVE(table_schema1.GetColumn("colC").Oid(), type::TypeId::INTEGER);
    auto col4 = expr_maker.CVE(table_schema1.GetColumn("colD").Oid(), type::TypeId::INTEGER);
    seq_scan_out1.AddOutput("col1", col1);
    seq_scan_out1.AddOutput("col2", col2);
    seq_scan_out1.AddOutput("col3", col3);
    seq_scan_out1.AddOutput("col4", col4);
    auto schema = seq_scan_out1.MakeSchema();

    auto pred1 = expr_maker.ComparisonGe(col1, expr_maker.Constant(495));
    auto pred2 = expr_maker.ComparisonLe(col1, expr_maker.Constant(505));
    auto predicate = expr_maker.ConjunctionAnd(pred1, pred2);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan1 = builder.SetOutputSchema(std::move(schema))
                    .SetColumnOids({table_schema1.GetColumn("colA").Oid(), table_schema1.GetColumn("colB").Oid(),
                                    table_schema1.GetColumn("colC").Oid(), table_schema1.GetColumn("colD").Oid()})
                    .SetScanPredicate(predicate)
                    .SetIsForUpdateFlag(false)
                    .SetNamespaceOid(NSOid())
                    .SetTableOid(table_oid1)
                    .Build();
  }

  // make DeletePlanNode
  std::unique_ptr<planner::AbstractPlanNode> update_node;
  {
    planner::UpdatePlanNode::Builder builder;
    auto col1 = seq_scan_out1.GetOutput("col1");
    auto col3 = seq_scan_out1.GetOutput("col3");
    auto col4 = seq_scan_out1.GetOutput("col4");
    auto neg_col1 = expr_maker.OpMul(expr_maker.Constant(-1), col1);
    auto const_500 = expr_maker.Constant(500);
    planner::SetClause clause1{table_schema1.GetColumn("colA").Oid(), neg_col1};
    planner::SetClause clause2{table_schema1.GetColumn("colB").Oid(), const_500};
    planner::SetClause clause3{table_schema1.GetColumn("colC").Oid(), col3};
    planner::SetClause clause4{table_schema1.GetColumn("colD").Oid(), col4};

    update_node = builder.SetTableOid(table_oid1)
                      .AddChild(std::move(seq_scan1))
                      .AddSetClause(clause1)
                      .AddSetClause(clause2)
                      .AddSetClause(clause3)
                      .AddSetClause(clause4)
                      .SetIndexedUpdate(true)
                      .Build();
  }
  // Execute delete
  {
    // Make Exec Ctx
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{}};
    auto exec_ctx = MakeExecCtx(std::move(callback), update_node->GetOutputSchema().Get());
    CompileAndRun(update_node.get(), exec_ctx.get());
  }

  // Now scan through table to check content.
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema1.GetColumn("colA").Oid();
    auto colb_oid = table_schema1.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    // Make predicate
    auto pred1 = expr_maker.ComparisonLe(col1, expr_maker.Constant(-495));
    auto pred2 = expr_maker.ComparisonGe(col1, expr_maker.Constant(-505));
    auto predicate = expr_maker.ConjunctionAnd(pred1, pred2);
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid1)
                   .Build();
  }

  // Make checker
  // Create the checkers
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{11};
  RowChecker row_checker = [&num_output_rows, num_expected_rows](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    auto col2 = static_cast<sql::Integer *>(vals[1]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_);
    int32_t col1_val = -495 - static_cast<int32_t>(num_output_rows);
    ASSERT_EQ(col1->val_, col1_val);
    ASSERT_EQ(col2->val_, 500);

    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  // Execute Table Scan
  {
    GenericChecker checker(row_checker, correcteness_fn);
    OutputStore store{&checker, seq_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get());
    CompileAndRun(seq_scan.get(), exec_ctx.get());
    checker.CheckCorrectness();
  }

  // Do an index scan to look an updated value
  // TODO(Amadou): Replace with directional scan.
  std::unique_ptr<planner::AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema1.GetColumn("colA").Oid();
    auto colb_oid = table_schema1.GetColumn("colB").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);

    index_scan_out.AddOutput("col1", col1);
    index_scan_out.AddOutput("col2", col2);
    auto schema = index_scan_out.MakeSchema();
    planner::IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid1)
                     .SetColumnOids({cola_oid, colb_oid})
                     .SetIndexOid(index_oid1)
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(-505))
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(-495))
                     .SetScanPredicate(nullptr)
                     .SetNamespaceOid(NSOid())
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::Descending)
                     .SetScanLimit(0)
                     .Build();
  }

  // Execute index scan
  {
    num_output_rows = 0;
    GenericChecker checker(row_checker, correcteness_fn);
    OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get());
    CompileAndRun(index_scan.get(), exec_ctx.get());
    checker.CheckCorrectness();
  }
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleInsertTest) {
  // INSERT INTO test_1 (colA, colB, colC, colD) VALUES (-1, 1, 2, 3), (-2, 1, 2, 3)
  // Then check that the following finds the new tuples:
  // SELECT colA, colB, colC, colD FROM test_1 WHERE test_1.colA < 0.
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid1 = accessor->GetTableOid(NSOid(), "test_1");
  auto index_oid1 = accessor->GetIndexOid(NSOid(), "index_1");
  auto table_schema1 = accessor->GetSchema(table_oid1);

  // make InsertPlanNode
  std::unique_ptr<planner::AbstractPlanNode> insert;
  {
    std::vector<ExpressionMaker::ManagedExpression> values1;
    std::vector<ExpressionMaker::ManagedExpression> values2;

    values1.push_back(expr_maker.Constant(-1));
    values1.push_back(expr_maker.Constant(1));
    values1.push_back(expr_maker.Constant(2));
    values1.push_back(expr_maker.Constant(3));
    values2.push_back(expr_maker.Constant(-2));
    values2.push_back(expr_maker.Constant(1));
    values2.push_back(expr_maker.Constant(2));
    values2.push_back(expr_maker.Constant(3));
    planner::InsertPlanNode::Builder builder;
    insert = builder.AddParameterInfo(table_schema1.GetColumn("colA").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("colB").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("colC").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("colD").Oid())
                 .SetIndexOids({index_oid1})
                 .AddValues(std::move(values1))
                 .AddValues(std::move(values2))
                 .SetNamespaceOid(NSOid())
                 .SetTableOid(table_oid1)
                 .Build();
  }
  // Execute insert
  {
    // Make Exec Ctx
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{}};
    auto exec_ctx = MakeExecCtx(std::move(callback), insert->GetOutputSchema().Get());
    CompileAndRun(insert.get(), exec_ctx.get());
  }

  // Now scan through table to check content.
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema1.GetColumn("colA").Oid();
    auto colb_oid = table_schema1.GetColumn("colB").Oid();
    auto colc_oid = table_schema1.GetColumn("colC").Oid();
    auto cold_oid = table_schema1.GetColumn("colD").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    auto col3 = expr_maker.CVE(colc_oid, type::TypeId::INTEGER);
    auto col4 = expr_maker.CVE(cold_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    seq_scan_out.AddOutput("col3", col3);
    seq_scan_out.AddOutput("col4", col4);
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(0));
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid, colc_oid, cold_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid1)
                   .Build();
  }
  // Create the checkers
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{2};
  RowChecker row_checker = [&num_output_rows, num_expected_rows](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    auto col2 = static_cast<sql::Integer *>(vals[1]);
    auto col3 = static_cast<sql::Integer *>(vals[2]);
    auto col4 = static_cast<sql::Integer *>(vals[3]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_ || col3->is_null_ || col4->is_null_);
    int32_t col1_val = -1 - static_cast<int32_t>(num_output_rows);
    ASSERT_EQ(col1->val_, col1_val);
    ASSERT_EQ(col2->val_, 1);
    ASSERT_EQ(col3->val_, 2);
    ASSERT_EQ(col4->val_, 3);

    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  // Execute Table Scan
  {
    GenericChecker checker(row_checker, correcteness_fn);
    OutputStore store{&checker, seq_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get());
    CompileAndRun(seq_scan.get(), exec_ctx.get());
    checker.CheckCorrectness();
  }

  // Do an index scan to look for inserted value
  std::unique_ptr<planner::AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema1.GetColumn("colA").Oid();
    auto colb_oid = table_schema1.GetColumn("colB").Oid();
    auto colc_oid = table_schema1.GetColumn("colC").Oid();
    auto cold_oid = table_schema1.GetColumn("colD").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    auto col3 = expr_maker.CVE(colc_oid, type::TypeId::INTEGER);
    auto col4 = expr_maker.CVE(cold_oid, type::TypeId::INTEGER);

    index_scan_out.AddOutput("col1", col1);
    index_scan_out.AddOutput("col2", col2);
    index_scan_out.AddOutput("col3", col3);
    index_scan_out.AddOutput("col4", col4);
    auto schema = index_scan_out.MakeSchema();
    planner::IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid1)
                     .SetColumnOids({cola_oid, colb_oid, colc_oid, cold_oid})
                     .SetIndexOid(index_oid1)
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(-10000))
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(-1))
                     .SetScanPredicate(nullptr)
                     .SetNamespaceOid(NSOid())
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::Descending)
                     .SetScanLimit(0)
                     .Build();
  }

  // Execute index scan
  {
    num_output_rows = 0;
    GenericChecker checker(row_checker, correcteness_fn);
    OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get());
    CompileAndRun(index_scan.get(), exec_ctx.get());
    checker.CheckCorrectness();
  }
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, InsertIntoSelectWithParamTest) {
  // INSERT INTO test_1
  // SELECT -colA, col2, col3, col4 FROM test_1 WHERE colA BETWEEN param1 AND param2.
  // Set param1 = 495 and param2 = 505
  // The do the following to check inserts:
  // SELECT * FROM test_1 WHERE colA < 0.
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid1 = accessor->GetTableOid(NSOid(), "test_1");
  auto index_oid1 = accessor->GetIndexOid(NSOid(), "index_1");
  auto table_schema1 = accessor->GetSchema(table_oid1);
  // SeqScan for insert
  std::unique_ptr<planner::AbstractPlanNode> seq_scan1;
  OutputSchemaHelper seq_scan_out1{0, &expr_maker};
  {
    auto col1 = expr_maker.CVE(table_schema1.GetColumn("colA").Oid(), type::TypeId::INTEGER);
    auto neg_col1 = expr_maker.OpMul(expr_maker.Constant(-1), col1);
    auto col2 = expr_maker.CVE(table_schema1.GetColumn("colB").Oid(), type::TypeId::INTEGER);
    auto col3 = expr_maker.CVE(table_schema1.GetColumn("colC").Oid(), type::TypeId::INTEGER);
    auto col4 = expr_maker.CVE(table_schema1.GetColumn("colD").Oid(), type::TypeId::INTEGER);
    seq_scan_out1.AddOutput("col1", neg_col1);
    seq_scan_out1.AddOutput("col2", col2);
    seq_scan_out1.AddOutput("col3", col3);
    seq_scan_out1.AddOutput("col4", col4);
    auto schema = seq_scan_out1.MakeSchema();
    auto param1 = expr_maker.PVE(type::TypeId::INTEGER, 0);
    auto param2 = expr_maker.PVE(type::TypeId::INTEGER, 1);
    auto pred1 = expr_maker.ComparisonGe(col1, param1);
    auto pred2 = expr_maker.ComparisonLe(col1, param2);
    auto predicate = expr_maker.ConjunctionAnd(pred1, pred2);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan1 = builder.SetOutputSchema(std::move(schema))
                    .SetColumnOids({table_schema1.GetColumn("colA").Oid(), table_schema1.GetColumn("colB").Oid(),
                                    table_schema1.GetColumn("colC").Oid(), table_schema1.GetColumn("colD").Oid()})
                    .SetScanPredicate(predicate)
                    .SetIsForUpdateFlag(false)
                    .SetNamespaceOid(NSOid())
                    .SetTableOid(table_oid1)
                    .Build();
  }

  // Insertion node
  std::unique_ptr<planner::AbstractPlanNode> insert;
  {
    planner::InsertPlanNode::Builder builder;
    insert = builder.AddParameterInfo(table_schema1.GetColumn("colA").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("colB").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("colC").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("colD").Oid())
                 .SetIndexOids({index_oid1})
                 .SetNamespaceOid(NSOid())
                 .SetTableOid(table_oid1)
                 .AddChild(std::move(seq_scan1))
                 .Build();
  }

  // Execute insert
  {
    // Make Exec Ctx
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{}};
    auto exec_ctx = MakeExecCtx(std::move(callback), insert->GetOutputSchema().Get());
    std::vector<type::TransientValue> params;
    params.emplace_back(type::TransientValueFactory::GetInteger(495));
    params.emplace_back(type::TransientValueFactory::GetInteger(505));
    exec_ctx->SetParams(std::move(params));
    CompileAndRun(insert.get(), exec_ctx.get());
  }

  // Now scan through table to check content.
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema1.GetColumn("colA").Oid();
    auto colb_oid = table_schema1.GetColumn("colB").Oid();
    auto colc_oid = table_schema1.GetColumn("colC").Oid();
    auto cold_oid = table_schema1.GetColumn("colD").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    auto col3 = expr_maker.CVE(colc_oid, type::TypeId::INTEGER);
    auto col4 = expr_maker.CVE(cold_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    seq_scan_out.AddOutput("col3", col3);
    seq_scan_out.AddOutput("col4", col4);
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(0));
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid, colc_oid, cold_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid1)
                   .Build();
  }
  // Create the checkers
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{11};
  RowChecker row_checker = [&num_output_rows, num_expected_rows](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    auto col2 = static_cast<sql::Integer *>(vals[1]);
    auto col3 = static_cast<sql::Integer *>(vals[2]);
    auto col4 = static_cast<sql::Integer *>(vals[3]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_ || col3->is_null_ || col4->is_null_);
    int32_t col1_val = (-495 - num_output_rows) * 1;
    ASSERT_EQ(col1->val_, col1_val);

    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  // Execute Table Scan
  {
    num_output_rows = 0;
    GenericChecker checker(row_checker, correcteness_fn);
    OutputStore store{&checker, seq_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get());
    CompileAndRun(seq_scan.get(), exec_ctx.get());
    checker.CheckCorrectness();
  }

  // Do an index scan to look for inserted value
  std::unique_ptr<planner::AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema1.GetColumn("colA").Oid();
    auto colb_oid = table_schema1.GetColumn("colB").Oid();
    auto colc_oid = table_schema1.GetColumn("colC").Oid();
    auto cold_oid = table_schema1.GetColumn("colD").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    auto col3 = expr_maker.CVE(colc_oid, type::TypeId::INTEGER);
    auto col4 = expr_maker.CVE(cold_oid, type::TypeId::INTEGER);

    index_scan_out.AddOutput("col1", col1);
    index_scan_out.AddOutput("col2", col2);
    index_scan_out.AddOutput("col3", col3);
    index_scan_out.AddOutput("col4", col4);
    auto schema = index_scan_out.MakeSchema();
    planner::IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid1)
                     .SetColumnOids({cola_oid, colb_oid, colc_oid, cold_oid})
                     .SetIndexOid(index_oid1)
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(-1000))
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.Constant(-1))
                     .SetScanPredicate(nullptr)
                     .SetNamespaceOid(NSOid())
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::Descending)
                     .SetScanLimit(0)
                     .Build();
  }
  // Execute index scan
  {
    num_output_rows = 0;
    GenericChecker checker(row_checker, correcteness_fn);
    OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get());
    CompileAndRun(index_scan.get(), exec_ctx.get());
    checker.CheckCorrectness();
  }
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleInsertWithParamsTest) {
  // INSERT INTO all_types_table (string_col, date_col, real_col, int_col) VALUES (param1, param2, param3, param4),
  // (param5, param6, param7, param8) Where the parameter values are: ("37 Strings", 1937-3-7, 37.73, 37), (73 String,
  // 1973-7-3, 73.37, 73) Then check that the following finds the new tuples: SELECT colA, colB, colC, colD FROM test_1.
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid1 = accessor->GetTableOid(NSOid(), "all_types_table");
  auto index_oid1 = accessor->GetIndexOid(NSOid(), "varchar_index");
  auto table_schema1 = accessor->GetSchema(table_oid1);

  // Make the parameter values.
  std::string str1("37 Strings");
  std::string str2("73 Strings");
  sql::Date date1(1937, 3, 7);
  sql::Date date2(1973, 7, 3);
  double real1 = 37.73;
  double real2 = 73.37;
  int32_t int1 = 37;
  int32_t int2 = 73;

  // make InsertPlanNode
  std::unique_ptr<planner::AbstractPlanNode> insert;
  {
    std::vector<ExpressionMaker::ManagedExpression> values1;
    std::vector<ExpressionMaker::ManagedExpression> values2;

    values1.push_back(expr_maker.PVE(type::TypeId::VARCHAR, 0));
    values1.push_back(expr_maker.PVE(type::TypeId::DATE, 1));
    values1.push_back(expr_maker.PVE(type::TypeId::DECIMAL, 2));
    values1.push_back(expr_maker.PVE(type::TypeId::INTEGER, 3));
    values2.push_back(expr_maker.PVE(type::TypeId::VARCHAR, 4));
    values2.push_back(expr_maker.PVE(type::TypeId::DATE, 5));
    values2.push_back(expr_maker.PVE(type::TypeId::DECIMAL, 6));
    values2.push_back(expr_maker.PVE(type::TypeId::INTEGER, 7));
    planner::InsertPlanNode::Builder builder;
    insert = builder.AddParameterInfo(table_schema1.GetColumn("varchar_col").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("date_col").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("real_col").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("int_col").Oid())
                 .SetIndexOids({index_oid1})
                 .AddValues(std::move(values1))
                 .AddValues(std::move(values2))
                 .SetNamespaceOid(NSOid())
                 .SetTableOid(table_oid1)
                 .Build();
  }
  // Execute insert
  {
    // Make Exec Ctx
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{}};
    auto exec_ctx = MakeExecCtx(std::move(callback), insert->GetOutputSchema().Get());
    std::vector<type::TransientValue> params;
    params.emplace_back(type::TransientValueFactory::GetVarChar(str1));
    params.emplace_back(type::TransientValueFactory::GetDate(type::date_t(date1.int_val_)));
    params.emplace_back(type::TransientValueFactory::GetDecimal(real1));
    params.emplace_back(type::TransientValueFactory::GetInteger(int1));
    params.emplace_back(type::TransientValueFactory::GetVarChar(str2));
    params.emplace_back(type::TransientValueFactory::GetDate(type::date_t(date2.int_val_)));
    params.emplace_back(type::TransientValueFactory::GetDecimal(real2));
    params.emplace_back(type::TransientValueFactory::GetInteger(int2));
    exec_ctx->SetParams(std::move(params));
    CompileAndRun(insert.get(), exec_ctx.get());
  }

  // Now scan through table to check content.
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    auto col1_oid = table_schema1.GetColumn("varchar_col").Oid();
    auto col2_oid = table_schema1.GetColumn("date_col").Oid();
    auto col3_oid = table_schema1.GetColumn("real_col").Oid();
    auto col4_oid = table_schema1.GetColumn("int_col").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(col1_oid, type::TypeId::VARCHAR);
    auto col2 = expr_maker.CVE(col2_oid, type::TypeId::DATE);
    auto col3 = expr_maker.CVE(col3_oid, type::TypeId::DECIMAL);
    auto col4 = expr_maker.CVE(col4_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    seq_scan_out.AddOutput("col3", col3);
    seq_scan_out.AddOutput("col4", col4);
    // Make predicate
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({col1_oid, col2_oid, col3_oid, col4_oid})
                   .SetScanPredicate(nullptr)
                   .SetIsForUpdateFlag(false)
                   .SetNamespaceOid(NSOid())
                   .SetTableOid(table_oid1)
                   .Build();
  }
  // Create the checkers
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{2};
  RowChecker row_checker = [&](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<sql::StringVal *>(vals[0]);
    auto col2 = static_cast<sql::Date *>(vals[1]);
    auto col3 = static_cast<sql::Real *>(vals[2]);
    auto col4 = static_cast<sql::Integer *>(vals[3]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_ || col3->is_null_ || col4->is_null_);
    if (num_output_rows == 0) {
      ASSERT_EQ(col1->len_, str1.size());
      ASSERT_EQ(std::memcmp(col1->Content(), str1.data(), col1->len_), 0);
      ASSERT_EQ(col2->ymd_, date1.ymd_);
      ASSERT_EQ(col3->val_, real1);
      ASSERT_EQ(col4->val_, int1);
    } else {
      ASSERT_TRUE(col1->len_ == str2.size());
      ASSERT_EQ(std::memcmp(col1->Content(), str2.data(), col1->len_), 0);
      ASSERT_EQ(col2->ymd_, date2.ymd_);
      ASSERT_EQ(col3->val_, real2);
      ASSERT_EQ(col4->val_, int2);
    }
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
  };
  CorrectnessFn correcteness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  // Execute Table Scan
  {
    GenericChecker checker(row_checker, correcteness_fn);
    OutputStore store{&checker, seq_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get());
    CompileAndRun(seq_scan.get(), exec_ctx.get());
    checker.CheckCorrectness();
  }

  // Do an index scan to look for inserted value
  std::unique_ptr<planner::AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema1.GetColumn("varchar_col").Oid();
    auto colb_oid = table_schema1.GetColumn("date_col").Oid();
    auto colc_oid = table_schema1.GetColumn("real_col").Oid();
    auto cold_oid = table_schema1.GetColumn("int_col").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::VARCHAR);
    auto col2 = expr_maker.CVE(colb_oid, type::TypeId::DATE);
    auto col3 = expr_maker.CVE(colc_oid, type::TypeId::DECIMAL);
    auto col4 = expr_maker.CVE(cold_oid, type::TypeId::INTEGER);

    index_scan_out.AddOutput("col1", col1);
    index_scan_out.AddOutput("col2", col2);
    index_scan_out.AddOutput("col3", col3);
    index_scan_out.AddOutput("col4", col4);
    auto schema = index_scan_out.MakeSchema();
    planner::IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid1)
                     .SetColumnOids({cola_oid, colb_oid, colc_oid, cold_oid})
                     .SetIndexOid(index_oid1)
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.PVE(type::TypeId::VARCHAR, 0))
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.PVE(type::TypeId::VARCHAR, 1))
                     .SetScanPredicate(nullptr)
                     .SetNamespaceOid(NSOid())
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::Ascending)
                     .SetScanLimit(0)
                     .Build();
  }

  // Execute index scan
  {
    num_output_rows = 0;
    GenericChecker checker(row_checker, correcteness_fn);
    OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get());
    std::vector<type::TransientValue> params;
    params.emplace_back(type::TransientValueFactory::GetVarChar(str1));
    params.emplace_back(type::TransientValueFactory::GetVarChar(str2));
    exec_ctx->SetParams(std::move(params));
    CompileAndRun(index_scan.get(), exec_ctx.get());
    checker.CheckCorrectness();
  }
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
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
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
    planner::SeqScanPlanNode::Builder builder;
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
  std::unique_ptr<planner::AbstractPlanNode> agg;
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
    planner::AggregatePlanNode::Builder builder;
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
