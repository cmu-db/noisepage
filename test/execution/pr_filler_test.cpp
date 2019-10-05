#include "execution/compiler/storage/pr_filler.h"
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

#include "execution/compiler/expression_util.h"
#include "execution/compiler/output_checker.h"
#include "execution/compiler/output_schema_util.h"


namespace terrier::execution::compiler::test {

class PRFillerTest : public SqlBasedTest {
 public:
  void SetUp() override {
    SqlBasedTest::SetUp();
    // Make the test tables
    auto exec_ctx = MakeExecCtx();
    sql::TableGenerator table_generator{exec_ctx.get(), BlockStore(), NSOid()};
    table_generator.GenerateTestTables();
  }


  static std::unique_ptr<vm::Module> MakeModule(CodeGen *codegen, ast::File* root, exec::ExecutionContext *exec_ctx) {
    // Create the query object, whose region must outlive all the processing.
    // Compile and check for errors
    EXECUTION_LOG_INFO("Generated File");
    sema::Sema type_checker{codegen->Context()};
    type_checker.Run(root);
    if (codegen->Reporter()->HasErrors()) {
      EXECUTION_LOG_ERROR("Type-checking error! \n {}", codegen->Reporter()->SerializeErrors());
    }

    EXECUTION_LOG_INFO("Converted: \n {}", execution::ast::AstDump::Dump(root));


    // Convert to bytecode

    auto bytecode_module = vm::BytecodeGenerator::Compile(root, exec_ctx, "tmp-tpl");
    bytecode_module->PrettyPrint(&std::cout);
    return std::make_unique<vm::Module>(std::move(bytecode_module));
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
TEST_F(PRFillerTest, SimpleIndexFillerTest) {
  // Get Table Info
  auto exec_ctx = MakeExecCtx();
  auto accessor = exec_ctx->GetAccessor();
  auto table_oid = accessor->GetTableOid(NSOid(), "test_1");
  auto table = accessor->GetTable(table_oid);
  auto table_schema = accessor->GetSchema(table_oid);
  std::vector<catalog::col_oid_t> col_oids;
  for (const auto & col : table_schema.GetColumns()) {
    col_oids.emplace_back(col.Oid());
  }
  storage::ProjectionMap table_pm(table->ProjectionMapForOids(col_oids));

  // Create pr filler
  CodeGen codegen(accessor);
  PRFiller filler(&codegen, table_schema, table_pm);

  // Get the index
  auto index_oid = accessor->GetIndexOid(NSOid(), "index_1");
  auto index = accessor->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  auto index_schema = accessor->GetIndexSchema(index_oid);

  // Compile the function
  auto [root, fn_name]= filler.GenFiller(index_pm, index_schema);
  auto module = MakeModule(&codegen, root, exec_ctx.get());

  // Now get the compiled function
  std::function<void(sql::ProjectedRowWrapper*, sql::ProjectedRowWrapper*)> filler_fn;
  ASSERT_TRUE(module->GetFunction(fn_name, vm::ExecutionMode::Compiled, &filler_fn));

  // Try it out.
  auto table_init = table->InitializerForProjectedRow(col_oids);
  auto table_buffer = common::AllocationUtil::AllocateAligned(table_init.ProjectedRowSize());
  auto table_pr = sql::ProjectedRowWrapper(table_init.InitializeRow(table_buffer));
  auto index_init = index->GetProjectedRowInitializer();
  auto index_buffer = common::AllocationUtil::AllocateAligned(index_init.ProjectedRowSize());
  auto index_pr = sql::ProjectedRowWrapper(index_init.InitializeRow(index_buffer));

  table_pr.Set<int32_t, false>(0, 500, false);
  filler_fn(&table_pr, &index_pr);
  auto val = index_pr.Get<int32_t, false>(0, nullptr);
  ASSERT_EQ(*val, 500);

  table_pr.Set<int32_t, false>(0, 651, false);
  filler_fn(&table_pr, &index_pr);
  val = index_pr.Get<int32_t, false>(0, nullptr);
  ASSERT_EQ(*val, 651);


  delete [] table_buffer;
  delete [] index_buffer;
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  terrier::execution::compiler::test::PRFillerTest::InitTPL();
  int ret = RUN_ALL_TESTS();
  terrier::execution::compiler::test::PRFillerTest::ShutdownTPL();
  return ret;
}

