#include "execution/compiler/compiler.h"

#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "execution/ast/ast_dump.h"
#include "execution/ast/context.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/compiler/expression_maker.h"
#include "execution/compiler/output_checker.h"
#include "execution/compiler/output_schema_util.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/output.h"
#include "execution/execution_util.h"
#include "execution/sema/sema.h"
#include "execution/sql/value.h"
#include "execution/sql/value_util.h"
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
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
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

  bool CheckFeatureVectorEquality(const std::vector<brain::ExecutionOperatingUnitFeature> &vec_a,
                                  const std::vector<brain::ExecutionOperatingUnitType> &vec_b) {
    std::unordered_set<brain::ExecutionOperatingUnitType> set_a;
    std::unordered_set<brain::ExecutionOperatingUnitType> set_b;
    for (const auto &e : vec_a) {
      set_a.insert(e.GetExecutionOperatingUnitType());
    }

    for (auto e : vec_b) {
      set_b.insert(e);
    }

    return set_a == set_b;
  }

  static constexpr vm::ExecutionMode MODE = vm::ExecutionMode::Interpret;
};

// NOLINTNEXTLINE
TEST_F(CompilerTest, CompileFromSource) {
  util::Region region{"compiler_test"};
  sema::ErrorReporter error_reporter{&region};
  ast::Context context{&region, &error_reporter};

  // Should be able to compile multiple TPL programs from source, including
  // functions that potentially collide in name.

  for (uint32_t i = 1; i < 4; i++) {
    auto src = std::string("fun test() -> int32 { return " + std::to_string(i * 10) + " }");
    auto input = Compiler::Input("Simple Test", &context, &src);
    auto module = Compiler::RunCompilationSimple(input);

    // The module should be valid since the input source is valid
    EXPECT_FALSE(module == nullptr);

    // The function should exist
    std::function<int64_t()> test_fn;
    EXPECT_TRUE(module->GetFunction("test", vm::ExecutionMode::Interpret, &test_fn));

    // And should return what we expect
    EXPECT_EQ(i * 10, test_fn());
  }
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, CompileToAst) {
  util::Region region{"compiler_test"};
  sema::ErrorReporter error_reporter{&region};
  ast::Context context{&region, &error_reporter};

  // Check compilation only to AST.
  struct CompileToAstCallback : public Compiler::Callbacks {
    ast::AstNode *root_;
    bool BeginPhase(Compiler::Phase phase, Compiler *compiler) override { return phase == Compiler::Phase::Parsing; }
    void EndPhase(Compiler::Phase phase, Compiler *compiler) override {
      if (phase == Compiler::Phase::Parsing) {
        root_ = compiler->GetAST();
      }
    }
    void OnError(Compiler::Phase phase, Compiler *compiler) override { FAIL(); }
    void TakeOwnership(std::unique_ptr<vm::Module> module) override { FAIL(); }
  };

  auto src = std::string("fun BLAH() -> int32 { return 10 }");
  auto input = Compiler::Input("Simple Test", &context, &src);
  auto callback = CompileToAstCallback();
  Compiler::RunCompilation(input, &callback);

  EXPECT_NE(nullptr, callback.root_);
  EXPECT_TRUE(callback.root_->IsFile());
  EXPECT_TRUE(callback.root_->As<ast::File>()->Declarations()[0]->IsFunctionDecl());
  auto *decl = callback.root_->As<ast::File>()->Declarations()[0]->As<ast::FunctionDecl>();

  // Check name
  EXPECT_EQ(context.GetIdentifier("BLAH"), decl->Name());

  // Check type isn't set, since we didn't do type checking
  EXPECT_EQ(nullptr, decl->Function()->TypeRepr()->ReturnType()->GetType());
}

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
  auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*seq_scan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  multi_checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 1);

  auto feature_vec = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto exp_vec = std::vector<brain::ExecutionOperatingUnitType>{
      brain::ExecutionOperatingUnitType::SEQ_SCAN, brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
      brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY, brain::ExecutionOperatingUnitType::OUTPUT};

  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec, exp_vec));
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleSeqScanNonVecFilterTest) {
  // SELECT col1, col2, col1 * col2, col1 >= 100*col2 FROM test_1
  // WHERE (col1 < 500 AND col2 >= 5) OR (500 <= col1 <= 1000 AND (col2 = 3 OR col2 = 7));
  // The filter is not in DNF form and can't be vectorized.
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
    auto comp2 = expr_maker.ComparisonGe(col2, expr_maker.Constant(5));
    auto clause1 = expr_maker.ConjunctionAnd(comp1, comp2);
    auto comp3 = expr_maker.ComparisonGe(col1, expr_maker.Constant(500));
    auto comp4 = expr_maker.ComparisonLt(col1, expr_maker.Constant(1000));
    auto comp5 = expr_maker.ComparisonEq(col2, expr_maker.Constant(3));
    auto comp6 = expr_maker.ComparisonEq(col2, expr_maker.Constant(7));
    auto clause2 =
        expr_maker.ConjunctionAnd(expr_maker.ConjunctionAnd(comp3, comp4), expr_maker.ConjunctionOr(comp5, comp6));
    auto predicate = expr_maker.ConjunctionOr(clause1, clause2);
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Make the output checkers
  std::function<void(const std::vector<sql::Val *> &)> row_checker = [](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<const sql::Integer *>(vals[0]);
    auto col2 = static_cast<const sql::Integer *>(vals[1]);
    ASSERT_FALSE(col1->is_null_ || col2->is_null_);
    // Check predicate
    ASSERT_TRUE((col1->val_ < 500 && col2->val_ >= 5) ||
                (col1->val_ >= 500 && col1->val_ < 1000 && (col2->val_ == 7 || col2->val_ == 3)));
  };
  GenericChecker checker(row_checker, {});

  MultiChecker multi_checker{std::vector<OutputChecker *>{&checker}};

  // Create the execution context
  OutputStore store{&multi_checker, seq_scan->GetOutputSchema().Get()};
  exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*seq_scan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  multi_checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 1);

  auto feature_vec = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto exp_vec = std::vector<brain::ExecutionOperatingUnitType>{
      brain::ExecutionOperatingUnitType::SEQ_SCAN, brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
      brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY, brain::ExecutionOperatingUnitType::OUTPUT};

  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec, exp_vec));
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleSeqScanWithProjectionTest) {
  // SELECT col1, col2, col1 * col2, col1 >= 100*col2 FROM test_1 WHERE col1 < 500;
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
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(500));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid, colb_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
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
  NumChecker num_checker(500);
  SingleIntComparisonChecker col1_checker(std::less<>(), 0, 500);

  MultiChecker multi_checker{std::vector<OutputChecker *>{&num_checker, &col1_checker}};

  // Create the execution context
  OutputStore store{&multi_checker, proj->GetOutputSchema().Get()};
  exec::OutputPrinter printer(proj->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), proj->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*proj, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  multi_checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 1);

  auto feature_vec = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto exp_vec = std::vector<brain::ExecutionOperatingUnitType>{
      brain::ExecutionOperatingUnitType::SEQ_SCAN, brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
      brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY, brain::ExecutionOperatingUnitType::OUTPUT};

  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec, exp_vec));
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
  auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get(), true);
  std::vector<parser::ConstantValueExpression> params;
  params.emplace_back(type::TypeId::INTEGER, execution::sql::Integer(100));
  params.emplace_back(type::TypeId::INTEGER, execution::sql::Integer(500));
  params.emplace_back(type::TypeId::INTEGER, execution::sql::Integer(3));
  exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&params));

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*seq_scan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  multi_checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 1);

  auto feature_vec = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto exp_vec = std::vector<brain::ExecutionOperatingUnitType>{
      brain::ExecutionOperatingUnitType::SEQ_SCAN, brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
      brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY, brain::ExecutionOperatingUnitType::OUTPUT};

  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec, exp_vec));
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
  auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*index_scan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  multi_checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 1);

  auto feature_vec = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto exp_vec = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::IDX_SCAN,
                                                                brain::ExecutionOperatingUnitType::OUTPUT};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec, exp_vec));
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleIndexScanAscendingTest) {
  // SELECT colA, colB FROM test_1 WHERE colA BETWEEN 495 AND 505 ORDER BY colA;
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
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::AscendingClosed)
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
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  GenericChecker checker(row_checker, correctness_fn);
  // Create the execution context
  OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*index_scan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 1);

  auto feature_vec = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto exp_vec = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::IDX_SCAN,
                                                                brain::ExecutionOperatingUnitType::OUTPUT};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec, exp_vec));
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleIndexScanLimitAscendingTest) {
  // SELECT colA, colB FROM test_1 WHERE colA BETWEEN 495 AND 505 ORDER BY colA LIMIT 5;
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
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::AscendingClosed)
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
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  GenericChecker checker(row_checker, correctness_fn);
  // Create the execution context
  OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*index_scan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 1);

  auto feature_vec = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto exp_vec = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::IDX_SCAN,
                                                                brain::ExecutionOperatingUnitType::OUTPUT};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec, exp_vec));
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleIndexScanDescendingTest) {
  // SELECT colA, colB FROM test_1 WHERE colA BETWEEN 495 AND 505 ORDER BY colA DESC;
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
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  GenericChecker checker(row_checker, correctness_fn);
  // Create the execution context
  OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*index_scan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 1);

  auto feature_vec = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto exp_vec = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::IDX_SCAN,
                                                                brain::ExecutionOperatingUnitType::OUTPUT};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec, exp_vec));
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleIndexScanLimitDescendingTest) {
  // SELECT colA, colB FROM test_1 WHERE colA BETWEEN 495 AND 505 ORDER BY colA DESC LIMIT 5;
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
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  GenericChecker checker(row_checker, correctness_fn);
  // Create the execution context
  OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*index_scan, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 1);

  auto feature_vec = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto exp_vec = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::IDX_SCAN,
                                                                brain::ExecutionOperatingUnitType::OUTPUT};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec, exp_vec));
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
              .AddAggregateTerm(agg_out.GetAggTerm("sum_col1"))
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
  auto exec_ctx = MakeExecCtx(std::move(callback), agg->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable =
      execution::compiler::CompilationContext::Compile(*agg, exec_ctx->GetExecutionSettings(), exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  multi_checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 2);

  auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto feature_vec1 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(2));
  auto exp_vec0 = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::AGGREGATE_ITERATE,
                                                                 brain::ExecutionOperatingUnitType::OUTPUT};
  auto exp_vec1 = std::vector<brain::ExecutionOperatingUnitType>{
      brain::ExecutionOperatingUnitType::SEQ_SCAN, brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
      brain::ExecutionOperatingUnitType::AGGREGATE_BUILD, brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec1, exp_vec1));
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, DISABLED_AggregateWithDistinctAndGroupByTest) {
  // TODO(WAN): distinct doesn't work yet in TPL2
  // SELECT col2, SUM(col1), COUNT(DISTINCT col2), SUM(DISTINCT col1) FROM test_1 WHERE col1 < 1000 GROUP BY col2;
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
    auto count_distinct_col2 = expr_maker.AggCount(col2, true);
    agg_out.AddAggTerm("count_distinct_col2", count_distinct_col2);
    auto sum_distinct_col1 = expr_maker.AggSum(col1, true);
    agg_out.AddAggTerm("sum_distinct_col1", sum_distinct_col1);

    // Make the output expressions
    agg_out.AddOutput("col2", agg_out.GetGroupByTermForOutput("col2"));
    agg_out.AddOutput("sum_col1", agg_out.GetAggTermForOutput("sum_col1"));
    agg_out.AddOutput("count_distinct_col2", agg_out.GetAggTermForOutput("count_distinct_col2"));
    agg_out.AddOutput("sum_distinct_col1", agg_out.GetAggTermForOutput("sum_distinct_col1"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddGroupByTerm(agg_out.GetGroupByTerm("col2"))
              .AddAggregateTerm(agg_out.GetAggTerm("sum_col1"))
              .AddAggregateTerm(agg_out.GetAggTerm("count_distinct_col2"))
              .AddAggregateTerm(agg_out.GetAggTerm("sum_distinct_col1"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }
  // Make the checkers
  // There should be 10 output rows.
  NumChecker num_checker{10};
  // The sum should be from 0 to 1000.
  SingleIntSumChecker sum_checker{1, (1000 * 999) / 2};
  // Group by key and count distinct aggregate are the same. So the output shouls always be one.
  SingleIntComparisonChecker distinct_count_checker{std::equal_to<>(), 2, 1};
  // Every key in col1 is unique, the sum and the sum(distinct) should be the same.
  SingleIntSumChecker distinct_sum_checker{3, (1000 * 999) / 2};
  MultiChecker multi_checker{
      std::vector<OutputChecker *>{&num_checker, &sum_checker, &distinct_count_checker, &distinct_sum_checker}};

  // Compile and Run
  OutputStore store{&multi_checker, agg->GetOutputSchema().Get()};
  exec::OutputPrinter printer(agg->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), agg->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable =
      execution::compiler::CompilationContext::Compile(*agg, exec_ctx->GetExecutionSettings(), exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  multi_checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, CountStarTest) {
  // SELECT COUNT(*) FROM test_1;
  // Get accessor
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    auto schema = seq_scan_out.MakeSchema();
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid})
                   .SetScanPredicate(nullptr)
                   .SetIsForUpdateFlag(false)
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Add aggregates
    agg_out.AddAggTerm("count_star", expr_maker.AggCount(expr_maker.Star()));
    // Make the output expressions
    agg_out.AddOutput("count_star", agg_out.GetAggTermForOutput("count_star"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddAggregateTerm(agg_out.GetAggTerm("count_star"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }
  // Make the checkers
  // There should be only one output
  NumChecker num_checker{1};
  // The count should be the size of the table.
  SingleIntComparisonChecker count_checker{std::equal_to<>(), 0, sql::TEST1_SIZE};
  MultiChecker multi_checker{std::vector<OutputChecker *>{&num_checker, &count_checker}};

  // Compile and Run
  OutputStore store{&multi_checker, agg->GetOutputSchema().Get()};
  exec::OutputPrinter printer(agg->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), agg->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable =
      execution::compiler::CompilationContext::Compile(*agg, exec_ctx->GetExecutionSettings(), exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  multi_checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, StaticAggregateTest) {
  // SELECT COUNT(*), SUM(cola) FROM test_1;
  // Get accessor
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({})
                   .SetScanPredicate(nullptr)
                   .SetIsForUpdateFlag(false)
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous output
    auto col1 = seq_scan_out.GetOutput("col1");
    // Add aggregates
    agg_out.AddAggTerm("count_star", expr_maker.AggCount(expr_maker.Star()));
    auto sum_col1 = expr_maker.AggSum(col1);
    agg_out.AddAggTerm("sum_col1", sum_col1);
    // Make the output expressions
    agg_out.AddOutput("count_star", agg_out.GetAggTermForOutput("count_star"));
    agg_out.AddOutput("sum_col1", agg_out.GetAggTermForOutput("sum_col1"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddAggregateTerm(agg_out.GetAggTerm("count_star"))
              .AddAggregateTerm(agg_out.GetAggTerm("sum_col1"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }
  // Make the checkers
  // There should be only one output
  NumChecker num_checker{1};
  // The count should be the size of the table.
  SingleIntComparisonChecker count_checker{std::equal_to<>(), 0, sql::TEST1_SIZE};
  // The sum should be from 1 to the size of the table.
  SingleIntComparisonChecker sum_checker{std::equal_to<>(), 1, sql::TEST1_SIZE * (sql::TEST1_SIZE - 1) / 2};
  MultiChecker multi_checker{std::vector<OutputChecker *>{&num_checker, &count_checker, &sum_checker}};

  // Compile and Run
  OutputStore store{&multi_checker, agg->GetOutputSchema().Get()};
  exec::OutputPrinter printer(agg->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), agg->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable =
      execution::compiler::CompilationContext::Compile(*agg, exec_ctx->GetExecutionSettings(), exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  multi_checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, DISABLED_StaticDistinctAggregateTest) {
  // TODO(WAN): distinct doesn't work yet in TPL2
  // SELECT COUNT(DISTINCT colb), SUM(DISTINCT colb), COUNT(*) FROM test_1;
  // Get accessor
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    auto colb_oid = table_schema.GetColumn("colB").Oid();
    auto col1 = expr_maker.CVE(colb_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({colb_oid})
                   .SetScanPredicate(nullptr)
                   .SetIsForUpdateFlag(false)
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Read previous output
    auto col1 = seq_scan_out.GetOutput("col1");
    // Add aggregates
    auto distinct_count1 = expr_maker.AggCount(col1, true);
    auto distinct_sum1 = expr_maker.AggSum(col1, true);
    agg_out.AddAggTerm("distinct_count1", distinct_count1);
    agg_out.AddAggTerm("distinct_sum1", distinct_sum1);
    agg_out.AddAggTerm("count_star", expr_maker.AggCount(expr_maker.Star()));
    // Make the output expressions
    agg_out.AddOutput("distinct_count1", agg_out.GetAggTermForOutput("distinct_count1"));
    agg_out.AddOutput("distinct_sum1", agg_out.GetAggTermForOutput("distinct_sum1"));
    agg_out.AddOutput("count_star", agg_out.GetAggTermForOutput("count_star"));
    auto schema = agg_out.MakeSchema();
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddAggregateTerm(agg_out.GetAggTerm("distinct_count1"))
              .AddAggregateTerm(agg_out.GetAggTerm("distinct_sum1"))
              .AddAggregateTerm(agg_out.GetAggTerm("count_star"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(nullptr)
              .Build();
  }
  // Make the checkers
  // There should be only one output
  NumChecker num_checker{1};
  // The count distinct should be the number of distinct elements (10).
  SingleIntComparisonChecker distinct_count_checker{std::equal_to<>(), 0, 10};
  // The sum  should be from 1 to 10, which is 45.
  SingleIntComparisonChecker distinct_sum_checker{std::equal_to<>(), 1, 45};
  // The count star should be the size of the table
  SingleIntComparisonChecker count_star_checker{std::equal_to<>(), 2, sql::TEST1_SIZE};
  MultiChecker multi_checker{
      std::vector<OutputChecker *>{&num_checker, &distinct_count_checker, &distinct_sum_checker, &count_star_checker}};

  // Compile and Run
  OutputStore store{&multi_checker, agg->GetOutputSchema().Get()};
  exec::OutputPrinter printer(agg->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), agg->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable =
      execution::compiler::CompilationContext::Compile(*agg, exec_ctx->GetExecutionSettings(), exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  multi_checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_FALSE(true);
  // TODO(WAN): re-enable when distinct works EXPECT_EQ(pipeline->units_.size(), 2);

  auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto feature_vec1 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(2));
  auto exp_vec0 = std::vector<brain::ExecutionOperatingUnitType>{
      brain::ExecutionOperatingUnitType::AGGREGATE_BUILD, brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS,
      brain::ExecutionOperatingUnitType::SEQ_SCAN};
  auto exp_vec1 = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::AGGREGATE_ITERATE,
                                                                 brain::ExecutionOperatingUnitType::OUTPUT};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec1, exp_vec1));
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
              .AddAggregateTerm(agg_out.GetAggTerm("sum_col1"))
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
  CorrectnessFn correctness_fn;
  GenericChecker checker(row_checker, correctness_fn);

  // Compile and Run
  OutputStore store{&checker, agg->GetOutputSchema().Get()};
  exec::OutputPrinter printer(agg->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), agg->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable =
      execution::compiler::CompilationContext::Compile(*agg, exec_ctx->GetExecutionSettings(), exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 2);

  auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto feature_vec1 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(2));
  auto exp_vec0 = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::AGGREGATE_ITERATE,
                                                                 brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
                                                                 brain::ExecutionOperatingUnitType::OUTPUT};
  auto exp_vec1 = std::vector<brain::ExecutionOperatingUnitType>{
      brain::ExecutionOperatingUnitType::SEQ_SCAN, brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
      brain::ExecutionOperatingUnitType::AGGREGATE_BUILD, brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec1, exp_vec1));
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, StaticAggregateHavingTest) {
  // SELECT COUNT(*) FROM test_1 HAVING COUNT(*) < 0;
  // Get accessor
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({})
                   .SetScanPredicate(nullptr)
                   .SetIsForUpdateFlag(false)
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Make the aggregate
  std::unique_ptr<planner::AbstractPlanNode> agg;
  OutputSchemaHelper agg_out{0, &expr_maker};
  {
    // Add aggregates
    agg_out.AddAggTerm("count_star", expr_maker.AggCount(expr_maker.Star()));
    // Make the output expressions
    agg_out.AddOutput("count_star", agg_out.GetAggTermForOutput("count_star"));
    auto schema = agg_out.MakeSchema();
    // Having
    auto having = expr_maker.ComparisonLt(agg_out.GetAggTermForOutput("count_star"), expr_maker.Constant(0));
    // Build
    planner::AggregatePlanNode::Builder builder;
    agg = builder.SetOutputSchema(std::move(schema))
              .AddAggregateTerm(agg_out.GetAggTerm("count_star"))
              .AddChild(std::move(seq_scan))
              .SetAggregateStrategyType(planner::AggregateStrategyType::HASH)
              .SetHavingClausePredicate(having)
              .Build();
  }
  // Make the checkers
  // Should not output anything
  NumChecker num_checker{0};
  // The count should be the size of the table.
  MultiChecker multi_checker{std::vector<OutputChecker *>{&num_checker}};

  // Compile and Run
  OutputStore store{&multi_checker, agg->GetOutputSchema().Get()};
  exec::OutputPrinter printer(agg->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), agg->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable =
      execution::compiler::CompilationContext::Compile(*agg, exec_ctx->GetExecutionSettings(), exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  multi_checker.CheckCorrectness();
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
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  GenericChecker checker(row_checker, correctness_fn);

  OutputStore store{&checker, hash_join->GetOutputSchema().Get()};
  exec::OutputPrinter printer(hash_join->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), hash_join->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*hash_join, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 2);

  auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto feature_vec1 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(2));
  auto exp_vec0 = std::vector<brain::ExecutionOperatingUnitType>{
      brain::ExecutionOperatingUnitType::SEQ_SCAN, brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
      brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS, brain::ExecutionOperatingUnitType::HASHJOIN_PROBE,
      brain::ExecutionOperatingUnitType::OUTPUT};
  auto exp_vec1 = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::SEQ_SCAN,
                                                                 brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
                                                                 brain::ExecutionOperatingUnitType::HASHJOIN_BUILD};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec1, exp_vec1));
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, MultiWayHashJoinTest) {
  // SELECT t1.col1, t2.col1, t3.col1, t1.col1 + t2.col1 + t3.col1
  // FROM test_1 AS t1, test_2 AS t2, test_2 AS t3
  // WHERE t1.col1 = t2.col1 AND t1.col1 = t3.col1 AND t1.col1 < 100 AND t2.col1 < 100 AND t2.col1 < 100
  // The sum is to make sure that codegen can traverse the query tree correctly.
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
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    seq_scan_out1.AddOutput("col1", col1);
    auto schema = seq_scan_out1.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(100));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan1 = builder.SetOutputSchema(std::move(schema))
                    .SetColumnOids({cola_oid})
                    .SetScanPredicate(predicate)
                    .SetIsForUpdateFlag(false)
                    .SetTableOid(table_oid1)
                    .Build();
  }
  // Make the second seq scan
  std::unique_ptr<planner::AbstractPlanNode> seq_scan2;
  OutputSchemaHelper seq_scan_out2{1, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema2.GetColumn("col1").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::SMALLINT);
    seq_scan_out2.AddOutput("col1", col1);
    auto schema = seq_scan_out2.MakeSchema();
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(100));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan2 = builder.SetOutputSchema(std::move(schema))
                    .SetColumnOids({cola_oid})
                    .SetScanPredicate(predicate)
                    .SetIsForUpdateFlag(false)
                    .SetTableOid(table_oid2)
                    .Build();
  }
  std::unique_ptr<planner::AbstractPlanNode> seq_scan3;
  OutputSchemaHelper seq_scan_out3{0, &expr_maker};
  {
    // OIDs
    auto cola_oid = table_schema1.GetColumn("colA").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    seq_scan_out3.AddOutput("col1", col1);
    auto schema = seq_scan_out3.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(100));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan3 = builder.SetOutputSchema(std::move(schema))
                    .SetColumnOids({cola_oid})
                    .SetScanPredicate(predicate)
                    .SetIsForUpdateFlag(false)
                    .SetTableOid(table_oid1)
                    .Build();
  }
  // Make first hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join1;
  OutputSchemaHelper hash_join_out1{1, &expr_maker};
  {
    // Left column
    auto t1_col1 = seq_scan_out1.GetOutput("col1");
    // Right column
    auto t2_col1 = seq_scan_out2.GetOutput("col1");
    // Output Schema
    hash_join_out1.AddOutput("t1.col1", t1_col1);
    hash_join_out1.AddOutput("t2.col1", t2_col1);
    auto schema = hash_join_out1.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(t1_col1, t2_col1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join1 = builder.AddChild(std::move(seq_scan1))
                     .AddChild(std::move(seq_scan2))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(t1_col1)
                     .AddRightHashKey(t2_col1)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Make second hash join
  std::unique_ptr<planner::AbstractPlanNode> hash_join2;
  OutputSchemaHelper hash_join_out2{0, &expr_maker};
  {
    // Left column
    auto t3_col1 = seq_scan_out3.GetOutput("col1");
    // Right columns
    auto t1_col1 = hash_join_out1.GetOutput("t1.col1");
    auto t2_col1 = hash_join_out1.GetOutput("t2.col1");
    // Make sum
    auto sum = expr_maker.OpSum(t1_col1, expr_maker.OpSum(t2_col1, t3_col1));
    // Output Schema
    hash_join_out2.AddOutput("t1.col1", t1_col1);
    hash_join_out2.AddOutput("t2.col1", t2_col1);
    hash_join_out2.AddOutput("t3.col1", t3_col1);
    hash_join_out2.AddOutput("sum", sum);
    auto schema = hash_join_out2.MakeSchema();
    // Predicate
    auto predicate = expr_maker.ComparisonEq(t3_col1, t1_col1);
    // Build
    planner::HashJoinPlanNode::Builder builder;
    hash_join2 = builder.AddChild(std::move(seq_scan3))
                     .AddChild(std::move(hash_join1))
                     .SetOutputSchema(std::move(schema))
                     .AddLeftHashKey(t3_col1)
                     .AddRightHashKey(t1_col1)
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .Build();
  }
  // Compile and Run
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{100};
  RowChecker row_checker = [&num_output_rows, num_expected_rows](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto t1_col1 = static_cast<sql::Integer *>(vals[0]);
    auto t2_col1 = static_cast<sql::Integer *>(vals[1]);
    auto t3_col1 = static_cast<sql::Integer *>(vals[2]);
    auto sum = static_cast<sql::Integer *>(vals[3]);
    ASSERT_FALSE(t1_col1->is_null_ || t2_col1->is_null_ || t3_col1->is_null_ || sum->is_null_);
    // Check join cols
    ASSERT_EQ(t1_col1->val_, t2_col1->val_);
    ASSERT_EQ(t1_col1->val_, t3_col1->val_);
    // Check sum
    ASSERT_EQ(sum->val_, 3 * t1_col1->val_);
    // Check the number of output row
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
  };
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  GenericChecker checker(row_checker, correctness_fn);

  OutputStore store{&checker, hash_join2->GetOutputSchema().Get()};
  exec::OutputPrinter printer(hash_join2->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), hash_join2->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*hash_join2, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 3);

  auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto feature_vec1 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(2));
  auto feature_vec2 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(3));
  auto exp_vec0 =
      std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::SEQ_SCAN,
                                                     brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
                                                     brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS,
                                                     brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS,
                                                     brain::ExecutionOperatingUnitType::HASHJOIN_PROBE,
                                                     brain::ExecutionOperatingUnitType::HASHJOIN_PROBE,
                                                     brain::ExecutionOperatingUnitType::OUTPUT};
  auto exp_vec1 = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::SEQ_SCAN,
                                                                 brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
                                                                 brain::ExecutionOperatingUnitType::HASHJOIN_BUILD};
  auto exp_vec2 = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::SEQ_SCAN,
                                                                 brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
                                                                 brain::ExecutionOperatingUnitType::HASHJOIN_BUILD};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec1, exp_vec1));
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec2, exp_vec2));
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
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  GenericChecker checker(row_checker, correctness_fn);

  // Create exec ctx
  OutputStore store{&checker, order_by->GetOutputSchema().Get()};
  exec::OutputPrinter printer(order_by->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), order_by->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*order_by, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 2);

  auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto feature_vec1 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(2));
  auto exp_vec0 = std::vector<brain::ExecutionOperatingUnitType>{
      brain::ExecutionOperatingUnitType::SORT_ITERATE, brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS,
      brain::ExecutionOperatingUnitType::OUTPUT};
  auto exp_vec1 = std::vector<brain::ExecutionOperatingUnitType>{
      brain::ExecutionOperatingUnitType::SEQ_SCAN, brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
      brain::ExecutionOperatingUnitType::SORT_BUILD, brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec1, exp_vec1));
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SortWithLimitTest) {
  // SELECT col1, col2, col1 + col2 FROM test_1 WHERE col1 < 500 ORDER BY col2 ASC, col1 - col2 DESC LIMIT 10
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
                   .SetLimit(10)
                   .Build();
  }
  // Checkers:
  // There should be 10 output rows because of the limit.
  // The output should be sorted by col2 ASC, then col1 DESC.
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{10};
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
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  GenericChecker checker(row_checker, correctness_fn);

  // Create exec ctx
  OutputStore store{&checker, order_by->GetOutputSchema().Get()};
  exec::OutputPrinter printer(order_by->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), order_by->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*order_by, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SortWithLimitAndOffsetTest) {
  // SELECT col1 FROM test_1 WHERE col1 < 500 ORDER BY col1 DESC LIMIT 100 OFFSET 100
  // Get accessor
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table_schema = accessor->GetSchema(table_oid);
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  int32_t upper = 500;
  {
    // OIDs
    auto cola_oid = table_schema.GetColumn("colA").Oid();
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(upper));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Order By
  std::unique_ptr<planner::AbstractPlanNode> order_by;
  OutputSchemaHelper order_by_out{0, &expr_maker};
  {
    // Output Colums col1
    auto col1 = seq_scan_out.GetOutput("col1");
    order_by_out.AddOutput("col1", col1);
    auto schema = order_by_out.MakeSchema();
    // Order By Clause
    planner::SortKey clause1{col1, optimizer::OrderByOrderingType::DESC};
    // Build
    planner::OrderByPlanNode::Builder builder;
    order_by = builder.SetOutputSchema(std::move(schema))
                   .AddChild(std::move(seq_scan))
                   .AddSortKey(clause1.first, clause1.second)
                   .SetLimit(100)
                   .SetOffset(100)
                   .Build();
  }
  // Checkers:
  // There should be 100 output rows because of the limit.
  // The output should be sorted by col1 DESC.
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{100};
  int64_t curr_col1{std::numeric_limits<int64_t>::max()};
  RowChecker row_checker = [&num_output_rows, &curr_col1, num_expected_rows,
                            upper](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    ASSERT_FALSE(col1->is_null_);
    // Check col1 and number of outputs
    ASSERT_LT(col1->val_, upper);
    // This equality holds because of the offset of 100 and the descending order.
    ASSERT_EQ(col1->val_, (upper - 1) - (num_output_rows + 100));
    // Check that output is sorted by col1
    ASSERT_GT(curr_col1, col1->val_);
    curr_col1 = col1->val_;

    // Check limit
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
  };
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  GenericChecker checker(row_checker, correctness_fn);

  // Create exec ctx
  OutputStore store{&checker, order_by->GetOutputSchema().Get()};
  exec::OutputPrinter printer(order_by->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), order_by->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*order_by, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, LimitAndOffsetTest) {
  // SELECT col1 FROM test_1 WHERE col1 < 500 LIMIT 100 OFFSET 100
  // This test uses an explicit limit node.
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
    // Get Table columns
    auto col1 = expr_maker.CVE(cola_oid, type::TypeId::INTEGER);
    seq_scan_out.AddOutput("col1", col1);
    auto schema = seq_scan_out.MakeSchema();
    // Make predicate
    auto predicate = expr_maker.ComparisonLt(col1, expr_maker.Constant(500));
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({cola_oid})
                   .SetScanPredicate(predicate)
                   .SetIsForUpdateFlag(false)
                   .SetTableOid(table_oid)
                   .Build();
  }
  // Limit
  std::unique_ptr<planner::AbstractPlanNode> limit;
  OutputSchemaHelper limit_out{0, &expr_maker};
  {
    // Output Colums col1
    auto col1 = seq_scan_out.GetOutput("col1");
    limit_out.AddOutput("col1", col1);
    auto schema = limit_out.MakeSchema();
    // Build
    planner::LimitPlanNode::Builder builder;
    limit =
        builder.SetOutputSchema(std::move(schema)).AddChild(std::move(seq_scan)).SetLimit(100).SetOffset(100).Build();
  }
  // Checkers:
  // There should be 100 output rows because of the limit.
  // The output should be sorted by col1.
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{100};
  int64_t curr_col1{std::numeric_limits<int64_t>::min()};
  RowChecker row_checker = [&num_output_rows, &curr_col1, num_expected_rows](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<sql::Integer *>(vals[0]);
    ASSERT_FALSE(col1->is_null_);
    // Check col1 and number of outputs
    ASSERT_LT(col1->val_, 500);
    // This equality holds because of the offset of 100.
    // The output should be (399, ..., 300).
    ASSERT_EQ(col1->val_, num_output_rows + 100);
    // Check that output is sorted by col1
    ASSERT_LT(curr_col1, col1->val_);
    curr_col1 = col1->val_;

    // Check limit
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
  };
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  GenericChecker checker(row_checker, correctness_fn);

  // Create exec ctx
  OutputStore store{&checker, limit->GetOutputSchema().Get()};
  exec::OutputPrinter printer(limit->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), limit->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*limit, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
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
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  GenericChecker checker(row_checker, correctness_fn);

  // Make Exec Ctx
  OutputStore store{&checker, nl_join->GetOutputSchema().Get()};
  exec::OutputPrinter printer(nl_join->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), nl_join->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*nl_join, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 1);

  // NLJOIN left and right are in same pipeline
  // But NLJOIN left/right features do not exist
  auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto exp_vec0 =
      std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::SEQ_SCAN,
                                                     brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
                                                     brain::ExecutionOperatingUnitType::SEQ_SCAN,
                                                     brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS,
                                                     brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
                                                     brain::ExecutionOperatingUnitType::OUTPUT};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
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
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(1), t2_col1)
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(1), t2_col1)
                     .SetOutputSchema(std::move(schema))
                     .SetJoinType(planner::LogicalJoinType::INNER)
                     .SetJoinPredicate(predicate)
                     .SetScanType(planner::IndexScanType::AscendingClosed)
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
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };
  GenericChecker checker(row_checker, correctness_fn);

  // Make Exec Ctx
  OutputStore store{&checker, index_join->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_join->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_join->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*index_join, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 1);

  auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto exp_vec0 = std::vector<brain::ExecutionOperatingUnitType>{
      brain::ExecutionOperatingUnitType::OUTPUT, brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
      brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS, brain::ExecutionOperatingUnitType::IDX_SCAN,
      brain::ExecutionOperatingUnitType::SEQ_SCAN};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
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
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(1), t1_col1)
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(1), t1_col1)
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(2), t1_col2)
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(2), t1_col2)
                     .SetScanType(planner::IndexScanType::AscendingClosed)
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
  CorrectnessFn correctness_fn;
  GenericChecker checker(row_checker, correctness_fn);

  // Make Exec Ctx
  OutputStore store{&checker, index_join->GetOutputSchema().Get()};
  exec::OutputPrinter printer(index_join->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  auto exec_ctx = MakeExecCtx(std::move(callback), index_join->GetOutputSchema().Get(), true);

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*index_join, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE);
  checker.CheckCorrectness();

  // Pipeline Units
  auto pipeline = executable->GetPipelineOperatingUnits();
  EXPECT_EQ(pipeline->units_.size(), 1);

  auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
  auto exp_vec0 = std::vector<brain::ExecutionOperatingUnitType>{
      brain::ExecutionOperatingUnitType::OUTPUT, brain::ExecutionOperatingUnitType::IDX_SCAN,
      brain::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS, brain::ExecutionOperatingUnitType::SEQ_SCAN};
  EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
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
    auto exec_ctx = MakeExecCtx(std::move(callback), delete_node->GetOutputSchema().Get(), true);
    auto executable = execution::compiler::CompilationContext::Compile(*delete_node, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);

    // Pipeline Units
    auto pipeline = executable->GetPipelineOperatingUnits();
    EXPECT_EQ(pipeline->units_.size(), 1);

    auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
    auto exp_vec0 = std::vector<brain::ExecutionOperatingUnitType>{
        brain::ExecutionOperatingUnitType::DELETE, brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
        brain::ExecutionOperatingUnitType::SEQ_SCAN};
    EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
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
                   .SetTableOid(table_oid1)
                   .Build();
  }
  // Execute Table Scan
  {
    NumChecker checker{0};
    OutputStore store{&checker, seq_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get(), true);
    auto executable = execution::compiler::CompilationContext::Compile(*seq_scan, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);
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
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::AscendingClosed)
                     .SetScanLimit(0)
                     .Build();
  }

  // Execute index scan
  {
    NumChecker checker{0};
    OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get(), true);
    auto executable = execution::compiler::CompilationContext::Compile(*index_scan, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);
    checker.CheckCorrectness();
  }
}

// NOLINTNEXTLINE
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
                    .SetTableOid(table_oid1)
                    .Build();
  }

  // make UpdatePlanNode
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
  // Execute update
  {
    // Make Exec Ctx
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{}};
    auto exec_ctx = MakeExecCtx(std::move(callback), update_node->GetOutputSchema().Get(), true);
    auto executable = execution::compiler::CompilationContext::Compile(*update_node, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);

    // Pipeline Units
    auto pipeline = executable->GetPipelineOperatingUnits();
    EXPECT_EQ(pipeline->units_.size(), 1);

    auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
    auto exp_vec0 = std::vector<brain::ExecutionOperatingUnitType>{
        brain::ExecutionOperatingUnitType::UPDATE, brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY,
        brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE, brain::ExecutionOperatingUnitType::SEQ_SCAN};
    EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
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
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  // Execute Table Scan
  {
    GenericChecker checker(row_checker, correctness_fn);
    OutputStore store{&checker, seq_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get(), true);
    auto executable = execution::compiler::CompilationContext::Compile(*seq_scan, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);
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
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::Descending)
                     .SetScanLimit(0)
                     .Build();
  }

  // Execute index scan
  {
    num_output_rows = 0;
    GenericChecker checker(row_checker, correctness_fn);
    OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get(), true);
    auto executable = execution::compiler::CompilationContext::Compile(*index_scan, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);
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

    auto output_schema = std::make_unique<planner::OutputSchema>();

    insert = builder.AddParameterInfo(table_schema1.GetColumn("colA").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("colB").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("colC").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("colD").Oid())
                 .SetIndexOids({index_oid1})
                 .AddValues(std::move(values1))
                 .AddValues(std::move(values2))
                 .SetTableOid(table_oid1)
                 .SetOutputSchema(std::move(output_schema))
                 .Build();
  }
  // Execute insert
  {
    // Make Exec Ctx
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{}};
    auto exec_ctx = MakeExecCtx(std::move(callback), insert->GetOutputSchema().Get(), true);
    auto executable = execution::compiler::CompilationContext::Compile(*insert, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);

    // Pipeline Units
    auto pipeline = executable->GetPipelineOperatingUnits();
    EXPECT_EQ(pipeline->units_.size(), 1);

    auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
    auto exp_vec0 = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::INSERT};
    EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
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
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  // Execute Table Scan
  {
    GenericChecker checker(row_checker, correctness_fn);
    OutputStore store{&checker, seq_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get(), true);
    auto executable = execution::compiler::CompilationContext::Compile(*seq_scan, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);
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
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::Descending)
                     .SetScanLimit(0)
                     .Build();
  }

  // Execute index scan
  {
    num_output_rows = 0;
    GenericChecker checker(row_checker, correctness_fn);
    OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get(), true);
    auto executable = execution::compiler::CompilationContext::Compile(*index_scan, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);
    checker.CheckCorrectness();
  }
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, DISABLED_InsertIntoSelectWithParamTest) {
  // TODO(WAN): insert into select doesn't work yet in TPL2
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
                    .SetTableOid(table_oid1)
                    .Build();
  }

  // Insertion node
  std::unique_ptr<planner::AbstractPlanNode> insert;
  {
    auto output_schema = std::make_unique<planner::OutputSchema>();

    planner::InsertPlanNode::Builder builder;
    insert = builder.AddParameterInfo(table_schema1.GetColumn("colA").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("colB").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("colC").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("colD").Oid())
                 .SetIndexOids({index_oid1})
                 .SetTableOid(table_oid1)
                 .AddChild(std::move(seq_scan1))
                 .SetOutputSchema(std::move(output_schema))
                 .Build();
  }

  // Execute insert
  {
    // Make Exec Ctx
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{}};
    auto exec_ctx = MakeExecCtx(std::move(callback), insert->GetOutputSchema().Get(), true);
    std::vector<parser::ConstantValueExpression> params;
    params.emplace_back(type::TypeId::INTEGER, execution::sql::Integer(495));
    params.emplace_back(type::TypeId::INTEGER, execution::sql::Integer(505));
    exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&params));
    auto executable = execution::compiler::CompilationContext::Compile(*insert, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);

    // Pipeline Units
    auto pipeline = executable->GetPipelineOperatingUnits();
    EXPECT_FALSE(true);
    // TODO(WAN): re-enable when distinct works EXPECT_EQ(pipeline->units_.size(), 1);

    auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
    auto exp_vec0 = std::vector<brain::ExecutionOperatingUnitType>{
        brain::ExecutionOperatingUnitType::INSERT, brain::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
        brain::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY, brain::ExecutionOperatingUnitType::SEQ_SCAN};
    EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
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
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  // Execute Table Scan
  {
    num_output_rows = 0;
    GenericChecker checker(row_checker, correctness_fn);
    OutputStore store{&checker, seq_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get(), true);
    auto executable = execution::compiler::CompilationContext::Compile(*seq_scan, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);
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
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::Descending)
                     .SetScanLimit(0)
                     .Build();
  }
  // Execute index scan
  {
    num_output_rows = 0;
    GenericChecker checker(row_checker, correctness_fn);
    OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get(), true);
    auto executable = execution::compiler::CompilationContext::Compile(*index_scan, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);
    checker.CheckCorrectness();
  }
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleInsertWithParamsTest) {
  // INSERT INTO all_types_empty_table
  //    (string_col, date_col, real_col, bool_col, tinyint_col, smallint_col, int_col, bigint_col)
  // VALUES
  //    (param1, param2, param3, param4, param5, param6, param7, param8),
  //    (param9, param10, param11, param12, param13, param14, param15, param16);
  //
  // Where the parameter values are:
  //    ("I am a long string with 37 characters", 1937-3-7, 37.73, true, 37, 37, 37, 37),
  //    ("I am a long string with 73 characters", 1973-7-3, 73.37, false, 73, 73, 73, 73)
  //
  // Then check that the following finds the
  // new tuples: SELECT colA, colB, colC, colD FROM test_1.
  auto accessor = MakeAccessor();
  ExpressionMaker expr_maker;
  auto table_oid1 = accessor->GetTableOid(NSOid(), "all_types_empty_table");
  auto index_oid1 = accessor->GetIndexOid(NSOid(), "varchar_index");
  auto table_schema1 = accessor->GetSchema(table_oid1);

  // Make the parameter values.
  // Keep string longs to avoid inlining
  std::string str1("I am a long string with 37 characters");
  std::string str2("I am a long string with 73 characters");
  sql::DateVal date1(sql::Date::FromYMD(1937, 3, 7));
  sql::DateVal date2(sql::Date::FromYMD(1973, 7, 3));
  double real1 = 37.73;
  double real2 = 73.37;
  bool bool1 = true;
  bool bool2 = false;
  int8_t tinyint1 = 37;
  int8_t tinyint2 = 73;
  int16_t smallint1 = 37;
  int16_t smallint2 = 73;
  int32_t int1 = 37;
  int32_t int2 = 73;
  int64_t bigint1 = 37;
  int64_t bigint2 = 73;

  // make InsertPlanNode
  std::unique_ptr<planner::AbstractPlanNode> insert;
  {
    std::vector<ExpressionMaker::ManagedExpression> values1;
    std::vector<ExpressionMaker::ManagedExpression> values2;

    int param_idx = 0;
    values1.push_back(expr_maker.PVE(type::TypeId::VARCHAR, param_idx++));
    values1.push_back(expr_maker.PVE(type::TypeId::DATE, param_idx++));
    values1.push_back(expr_maker.PVE(type::TypeId::DECIMAL, param_idx++));
    values1.push_back(expr_maker.PVE(type::TypeId::BOOLEAN, param_idx++));
    values1.push_back(expr_maker.PVE(type::TypeId::TINYINT, param_idx++));
    values1.push_back(expr_maker.PVE(type::TypeId::SMALLINT, param_idx++));
    values1.push_back(expr_maker.PVE(type::TypeId::INTEGER, param_idx++));
    values1.push_back(expr_maker.PVE(type::TypeId::BIGINT, param_idx++));

    values2.push_back(expr_maker.PVE(type::TypeId::VARCHAR, param_idx++));
    values2.push_back(expr_maker.PVE(type::TypeId::DATE, param_idx++));
    values2.push_back(expr_maker.PVE(type::TypeId::DECIMAL, param_idx++));
    values2.push_back(expr_maker.PVE(type::TypeId::BOOLEAN, param_idx++));
    values2.push_back(expr_maker.PVE(type::TypeId::TINYINT, param_idx++));
    values2.push_back(expr_maker.PVE(type::TypeId::SMALLINT, param_idx++));
    values2.push_back(expr_maker.PVE(type::TypeId::INTEGER, param_idx++));
    values2.push_back(expr_maker.PVE(type::TypeId::BIGINT, param_idx++));

    auto output_schema = std::make_unique<planner::OutputSchema>();

    planner::InsertPlanNode::Builder builder;
    insert = builder.AddParameterInfo(table_schema1.GetColumn("varchar_col").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("date_col").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("real_col").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("bool_col").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("tinyint_col").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("smallint_col").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("int_col").Oid())
                 .AddParameterInfo(table_schema1.GetColumn("bigint_col").Oid())
                 .SetIndexOids({index_oid1})
                 .AddValues(std::move(values1))
                 .AddValues(std::move(values2))
                 .SetTableOid(table_oid1)
                 .SetOutputSchema(std::move(output_schema))
                 .Build();
  }
  // Execute insert
  {
    // Make Exec Ctx
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{}};
    auto exec_ctx = MakeExecCtx(std::move(callback), insert->GetOutputSchema().Get(), true);
    std::vector<parser::ConstantValueExpression> params;
    // First parameter list
    auto str1_val = sql::ValueUtil::CreateStringVal(str1);
    params.emplace_back(type::TypeId::VARCHAR, str1_val.first, std::move(str1_val.second));
    params.emplace_back(type::TypeId::DATE, sql::DateVal(date1.val_));
    params.emplace_back(type::TypeId::DECIMAL, sql::Real(real1));
    params.emplace_back(type::TypeId::BOOLEAN, sql::BoolVal(bool1));
    params.emplace_back(type::TypeId::TINYINT, sql::Integer(tinyint1));
    params.emplace_back(type::TypeId::SMALLINT, sql::Integer(smallint1));
    params.emplace_back(type::TypeId::INTEGER, sql::Integer(int1));
    params.emplace_back(type::TypeId::BIGINT, sql::Integer(bigint1));
    // Second parameter list
    auto str2_val = sql::ValueUtil::CreateStringVal(str2);
    params.emplace_back(type::TypeId::VARCHAR, str2_val.first, std::move(str2_val.second));
    params.emplace_back(type::TypeId::DATE, sql::DateVal(date2.val_));
    params.emplace_back(type::TypeId::DECIMAL, sql::Real(real2));
    params.emplace_back(type::TypeId::BOOLEAN, sql::BoolVal(bool2));
    params.emplace_back(type::TypeId::TINYINT, sql::Integer(tinyint2));
    params.emplace_back(type::TypeId::SMALLINT, sql::Integer(smallint2));
    params.emplace_back(type::TypeId::INTEGER, sql::Integer(int2));
    params.emplace_back(type::TypeId::BIGINT, sql::Integer(bigint2));
    exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&params));
    auto executable = execution::compiler::CompilationContext::Compile(*insert, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);

    // Pipeline Units
    auto pipeline = executable->GetPipelineOperatingUnits();
    EXPECT_EQ(pipeline->units_.size(), 1);

    auto feature_vec0 = pipeline->GetPipelineFeatures(execution::pipeline_id_t(1));
    auto exp_vec0 = std::vector<brain::ExecutionOperatingUnitType>{brain::ExecutionOperatingUnitType::INSERT};
    EXPECT_TRUE(CheckFeatureVectorEquality(feature_vec0, exp_vec0));
  }

  // Now scan through table to check content.
  std::unique_ptr<planner::AbstractPlanNode> seq_scan;
  OutputSchemaHelper seq_scan_out{0, &expr_maker};
  {
    auto col1_oid = table_schema1.GetColumn("varchar_col").Oid();
    auto col2_oid = table_schema1.GetColumn("date_col").Oid();
    auto col3_oid = table_schema1.GetColumn("real_col").Oid();
    auto col4_oid = table_schema1.GetColumn("bool_col").Oid();
    auto col5_oid = table_schema1.GetColumn("tinyint_col").Oid();
    auto col6_oid = table_schema1.GetColumn("smallint_col").Oid();
    auto col7_oid = table_schema1.GetColumn("int_col").Oid();
    auto col8_oid = table_schema1.GetColumn("bigint_col").Oid();

    // Get Table columns
    auto col1 = expr_maker.CVE(col1_oid, type::TypeId::VARCHAR);
    auto col2 = expr_maker.CVE(col2_oid, type::TypeId::DATE);
    auto col3 = expr_maker.CVE(col3_oid, type::TypeId::DECIMAL);
    auto col4 = expr_maker.CVE(col4_oid, type::TypeId::BOOLEAN);
    auto col5 = expr_maker.CVE(col5_oid, type::TypeId::TINYINT);
    auto col6 = expr_maker.CVE(col6_oid, type::TypeId::SMALLINT);
    auto col7 = expr_maker.CVE(col7_oid, type::TypeId::INTEGER);
    auto col8 = expr_maker.CVE(col8_oid, type::TypeId::BIGINT);

    seq_scan_out.AddOutput("col1", col1);
    seq_scan_out.AddOutput("col2", col2);
    seq_scan_out.AddOutput("col3", col3);
    seq_scan_out.AddOutput("col4", col4);
    seq_scan_out.AddOutput("col5", col5);
    seq_scan_out.AddOutput("col6", col6);
    seq_scan_out.AddOutput("col7", col7);
    seq_scan_out.AddOutput("col8", col8);
    // Make predicate
    auto schema = seq_scan_out.MakeSchema();
    // Build
    planner::SeqScanPlanNode::Builder builder;
    seq_scan = builder.SetOutputSchema(std::move(schema))
                   .SetColumnOids({col1_oid, col2_oid, col3_oid, col4_oid, col5_oid, col6_oid, col7_oid, col8_oid})
                   .SetScanPredicate(nullptr)
                   .SetIsForUpdateFlag(false)
                   .SetTableOid(table_oid1)
                   .Build();
  }
  // Create the checkers
  uint32_t num_output_rows{0};
  uint32_t num_expected_rows{2};
  RowChecker row_checker = [&](const std::vector<sql::Val *> &vals) {
    // Read cols
    auto col1 = static_cast<sql::StringVal *>(vals[0]);
    auto col2 = static_cast<sql::DateVal *>(vals[1]);
    auto col3 = static_cast<sql::Real *>(vals[2]);
    auto col4 = static_cast<sql::BoolVal *>(vals[3]);
    auto col5 = static_cast<sql::Integer *>(vals[4]);
    auto col6 = static_cast<sql::Integer *>(vals[5]);
    auto col7 = static_cast<sql::Integer *>(vals[6]);
    auto col8 = static_cast<sql::Integer *>(vals[7]);

    // Nobody should be null here!
    ASSERT_FALSE(col1->is_null_);
    ASSERT_FALSE(col2->is_null_);
    ASSERT_FALSE(col3->is_null_);
    ASSERT_FALSE(col4->is_null_);
    ASSERT_FALSE(col5->is_null_);
    ASSERT_FALSE(col6->is_null_);
    ASSERT_FALSE(col7->is_null_);
    ASSERT_FALSE(col8->is_null_);

    // Make sure all of our values match what we inserted into the table
    if (num_output_rows == 0) {
      ASSERT_EQ(col1->GetLength(), str1.size());
      ASSERT_EQ(std::memcmp(col1->GetContent(), str1.data(), col1->GetLength()), 0);
      ASSERT_EQ(col2->val_, date1.val_);
      ASSERT_EQ(col3->val_, real1);
      ASSERT_EQ(col4->val_, bool1);
      ASSERT_EQ(col5->val_, tinyint1);
      ASSERT_EQ(col6->val_, smallint1);
      ASSERT_EQ(col7->val_, int1);
      ASSERT_EQ(col8->val_, bigint1);
    } else {
      ASSERT_TRUE(col1->GetLength() == str2.size());
      ASSERT_EQ(std::memcmp(col1->GetContent(), str2.data(), col1->GetLength()), 0);
      ASSERT_EQ(col2->val_, date2.val_);
      ASSERT_EQ(col3->val_, real2);
      ASSERT_EQ(col4->val_, bool2);
      ASSERT_EQ(col5->val_, tinyint2);
      ASSERT_EQ(col6->val_, smallint2);
      ASSERT_EQ(col7->val_, int2);
      ASSERT_EQ(col8->val_, bigint2);
    }
    num_output_rows++;
    ASSERT_LE(num_output_rows, num_expected_rows);
  };
  CorrectnessFn correctness_fn = [&num_output_rows, num_expected_rows]() {
    ASSERT_EQ(num_output_rows, num_expected_rows);
  };

  // Execute Table Scan
  {
    GenericChecker checker(row_checker, correctness_fn);
    OutputStore store{&checker, seq_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(seq_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), seq_scan->GetOutputSchema().Get(), true);
    auto executable = execution::compiler::CompilationContext::Compile(*seq_scan, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);
    checker.CheckCorrectness();
  }

  // Do an index scan to look for inserted value
  std::unique_ptr<planner::AbstractPlanNode> index_scan;
  OutputSchemaHelper index_scan_out{0, &expr_maker};
  {
    // OIDs
    auto col1_oid = table_schema1.GetColumn("varchar_col").Oid();
    auto col2_oid = table_schema1.GetColumn("date_col").Oid();
    auto col3_oid = table_schema1.GetColumn("real_col").Oid();
    auto col4_oid = table_schema1.GetColumn("bool_col").Oid();
    auto col5_oid = table_schema1.GetColumn("tinyint_col").Oid();
    auto col6_oid = table_schema1.GetColumn("smallint_col").Oid();
    auto col7_oid = table_schema1.GetColumn("int_col").Oid();
    auto col8_oid = table_schema1.GetColumn("bigint_col").Oid();

    // Get Table columns
    auto col1 = expr_maker.CVE(col1_oid, type::TypeId::VARCHAR);
    auto col2 = expr_maker.CVE(col2_oid, type::TypeId::DATE);
    auto col3 = expr_maker.CVE(col3_oid, type::TypeId::DECIMAL);
    auto col4 = expr_maker.CVE(col4_oid, type::TypeId::BOOLEAN);
    auto col5 = expr_maker.CVE(col5_oid, type::TypeId::TINYINT);
    auto col6 = expr_maker.CVE(col6_oid, type::TypeId::SMALLINT);
    auto col7 = expr_maker.CVE(col7_oid, type::TypeId::INTEGER);
    auto col8 = expr_maker.CVE(col8_oid, type::TypeId::BIGINT);

    index_scan_out.AddOutput("col1", col1);
    index_scan_out.AddOutput("col2", col2);
    index_scan_out.AddOutput("col3", col3);
    index_scan_out.AddOutput("col4", col4);
    index_scan_out.AddOutput("col5", col5);
    index_scan_out.AddOutput("col6", col6);
    index_scan_out.AddOutput("col7", col7);
    index_scan_out.AddOutput("col8", col8);

    auto schema = index_scan_out.MakeSchema();
    planner::IndexScanPlanNode::Builder builder;
    index_scan = builder.SetTableOid(table_oid1)
                     .SetColumnOids({col1_oid, col2_oid, col3_oid, col4_oid, col5_oid, col6_oid, col7_oid, col8_oid})
                     .SetIndexOid(index_oid1)
                     .AddLoIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.PVE(type::TypeId::VARCHAR, 0))
                     .AddHiIndexColumn(catalog::indexkeycol_oid_t(1), expr_maker.PVE(type::TypeId::VARCHAR, 1))
                     .SetScanPredicate(nullptr)
                     .SetOutputSchema(std::move(schema))
                     .SetScanType(planner::IndexScanType::AscendingClosed)
                     .SetScanLimit(0)
                     .Build();
  }

  // Execute index scan
  {
    num_output_rows = 0;
    GenericChecker checker(row_checker, correctness_fn);
    OutputStore store{&checker, index_scan->GetOutputSchema().Get()};
    exec::OutputPrinter printer(index_scan->GetOutputSchema().Get());
    MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
    auto exec_ctx = MakeExecCtx(std::move(callback), index_scan->GetOutputSchema().Get(), true);
    std::vector<parser::ConstantValueExpression> params;
    auto str1_val = sql::ValueUtil::CreateStringVal(str1);
    auto str2_val = sql::ValueUtil::CreateStringVal(str2);
    params.emplace_back(type::TypeId::VARCHAR, str1_val.first, std::move(str1_val.second));
    params.emplace_back(type::TypeId::VARCHAR, str2_val.first, std::move(str2_val.second));
    exec_ctx->SetParams(common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&params));
    auto executable = execution::compiler::CompilationContext::Compile(*index_scan, exec_ctx->GetExecutionSettings(),
                                                                       exec_ctx->GetAccessor());
    executable->Run(common::ManagedPointer(exec_ctx), MODE);
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
  CorrectnessFn correctness_fn;
  RowChecker row_checker;
  GenericChecker checker(row_checker, correctness_fn);
  // Make Exec Ctx
  OutputStore store{&checker, agg->GetOutputSchema().Get()};
  exec::OutputPrinter printer(agg->GetOutputSchema().Get());
  MultiOutputCallback callback{std::vector<exec::OutputCallback>{store, printer}};
  exec_ctx = MakeExecCtx(std::move(callback), agg->GetOutputSchema().Get());

  // Run & Check
  auto executable = execution::compiler::CompilationContext::Compile(*agg, exec_ctx->GetExecutionSettings(),
                                                                     exec_ctx->GetAccessor());
  executable->Run(common::ManagedPointer(exec_ctx), MODE); checker.CheckCorrectness();
}
*/
}  // namespace terrier::execution::compiler::test

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  terrier::execution::ExecutionUtil::InitTPL();
  int ret = RUN_ALL_TESTS();
  terrier::execution::ExecutionUtil::ShutdownTPL();
  return ret;
}
