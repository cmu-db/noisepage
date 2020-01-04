#include "execution/sql/index_creator.h"
#include <memory>
#include "catalog/catalog_defs.h"
#include "execution/compiler/expression/pr_filler.h"
#include "execution/exec/execution_context.h"
#include "storage/sql_table.h"
#include "storage/storage_defs.h"

namespace terrier::execution::sql {

std::pair<std::function<void(storage::ProjectedRow *, storage::ProjectedRow *)>, std::unique_ptr<vm::Module>>
IndexCreator::CompileBuildKeyFunction(terrier::execution::exec::ExecutionContext *exec_ctx,
                                      terrier::catalog::table_oid_t table_oid,
                                      terrier::catalog::index_oid_t index_oid) {
  auto accessor = exec_ctx->GetAccessor();
  auto table = accessor->GetTable(table_oid);
  auto &table_schema = accessor->GetSchema(table_oid);
  std::vector<catalog::col_oid_t> col_oids;
  for (const auto &col : table_schema.GetColumns()) {
    col_oids.emplace_back(col.Oid());
  }
  storage::ProjectionMap table_pm(table->ProjectionMapForOids(col_oids));

  // Create pr filler
  terrier::execution::compiler::CodeGen codegen(exec_ctx);
  terrier::execution::compiler::PRFiller filler(&codegen, table_schema, table_pm);

  // Get the index
  auto index = accessor->GetIndex(index_oid);
  const auto &index_pm = index->GetKeyOidToOffsetMap();
  auto &index_schema = accessor->GetIndexSchema(index_oid);

  // Compile the function
  auto [root, fn_name] = filler.GenFiller(index_pm, index_schema);

  // Create the query object, whose region must outlive all the processing.
  // Compile and check for errors
  EXECUTION_LOG_INFO("Generated File");
  sema::Sema type_checker{codegen.Context()};
  type_checker.Run(root);
  if (codegen.Reporter()->HasErrors()) {
    EXECUTION_LOG_ERROR("Type-checking error! \n {}", codegen.Reporter()->SerializeErrors());
  }

  EXECUTION_LOG_INFO("Converted: \n {}", execution::ast::AstDump::Dump(root));

  // Convert to bytecode

  auto bytecode_module = vm::BytecodeGenerator::Compile(root, exec_ctx, "tmp-tpl");
  auto module = std::make_unique<vm::Module>(std::move(bytecode_module));

  // Now get the compiled function
  std::function<void(storage::ProjectedRow *, storage::ProjectedRow *)> filler_fn;
  TERRIER_ASSERT(module->GetFunction(fn_name, vm::ExecutionMode::Compiled, &filler_fn), "");
  return {filler_fn, std::move(module)};
}

IndexCreator::IndexCreator(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid,
                           catalog::index_oid_t index_oid)
    : exec_ctx_{exec_ctx} {
  index_ = exec_ctx->GetAccessor()->GetIndex(index_oid);
  auto index_pri = index_->GetProjectedRowInitializer();
  index_pr_size_ = index_pri.ProjectedRowSize();
  index_pr_buffer_ = exec_ctx->GetMemoryPool()->AllocateAligned(index_pr_size_, sizeof(uint64_t), true);
  index_pr_ = index_pri.InitializeRow(index_pr_buffer_);

  table_ = exec_ctx->GetAccessor()->GetTable(table_oid);
  std::vector<catalog::col_oid_t> col_oids;
  auto table_schema = exec_ctx->GetAccessor()->GetSchema(table_oid);
  for (const auto &col : table_schema.GetColumns()) {
    col_oids.emplace_back(col.Oid());
  }
  auto table_pri = table_->InitializerForProjectedRow(col_oids);
  table_pr_size_ = table_pri.ProjectedRowSize();
  table_pr_buffer_ = exec_ctx->GetMemoryPool()->AllocateAligned(table_pr_size_, sizeof(uint64_t), true);
  table_pr_ = table_pri.InitializeRow(table_pr_buffer_);

  auto res = CompileBuildKeyFunction(exec_ctx, table_oid, index_oid);
  build_key_fn_ = res.first;
  module_ = std::move(res.second);
}

/**
 * Destructor
 */
IndexCreator::~IndexCreator() {
  exec_ctx_->GetMemoryPool()->Deallocate(table_pr_buffer_, table_pr_size_);
  exec_ctx_->GetMemoryPool()->Deallocate(index_pr_buffer_, index_pr_size_);
}

/**
 * @return The projected row of the index.
 */
storage::ProjectedRow *IndexCreator::GetIndexPR() { return index_pr_; }

/**
 * @return The projected row of the index.
 */
storage::ProjectedRow *IndexCreator::GetTablePR() { return table_pr_; }

std::function<void(storage::ProjectedRow *, storage::ProjectedRow *)> IndexCreator::GetBuildKeyFn() {
  return build_key_fn_;
}

/**
 * Insert into the index
 * @return Whether insertion was successful.
 */
bool IndexCreator::IndexInsert(storage::ProjectedRow *index_pr, storage::TupleSlot ts) {
  return index_->Insert(exec_ctx_->GetTxn(), *index_pr, ts);
}

bool IndexCreator::IndexInsertUnique(storage::ProjectedRow *index_pr, storage::TupleSlot ts) {
  return index_->InsertUnique(exec_ctx_->GetTxn(), *index_pr, ts);
}

}  // namespace terrier::execution::sql