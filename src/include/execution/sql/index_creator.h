#pragma once

#include <execution/sema/sema.h>
#include <planner/plannodes/create_index_plan_node.h>
#include <map>
#include <memory>
#include <vector>
#include "catalog/catalog_defs.h"
#include "execution/ast/ast_dump.h"
#include "execution/compiler/expression/pr_filler.h"
#include "execution/compiler/function_builder.h"
#include "execution/compiler/operator/operator_translator.h"
#include "execution/compiler/translator_factory.h"
#include "execution/exec/execution_context.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/module.h"
#include "loggers/execution_logger.h"
#include "storage/storage_defs.h"

namespace terrier::execution::sql {

/**
 * Allows insertion from TPL.
 */
class EXPORT IndexCreator {
 public:
  static std::pair<std::function<void(storage::ProjectedRow *, storage::ProjectedRow *)>, std::unique_ptr<vm::Module>>
  CompileBuildKeyFunction(terrier::execution::exec::ExecutionContext *exec_ctx, terrier::catalog::table_oid_t table_oid,
                          terrier::catalog::index_oid_t index_oid);

 public:
  /**
   * Constructor
   * @param exec_ctx The execution context
   * @param table_oid The oid of the table to insert into
   */
  explicit IndexCreator(exec::ExecutionContext *exec_ctx, catalog::table_oid_t table_oid,
                        catalog::index_oid_t index_oid);
  /**
   * Destructor
   */
  ~IndexCreator();

  /**
   * @return The projected row of the index.
   */
  storage::ProjectedRow *GetIndexPR();

  storage::ProjectedRow *GetTablePR();

  std::function<void(storage::ProjectedRow *, storage::ProjectedRow *)> GetBuildKeyFn();

  /**
   * Insert into the index
   * @return Whether insertion was successful.
   */
  bool IndexInsert(storage::ProjectedRow *index_pr, storage::TupleSlot ts);

  /**
   * Insert into the index uniquelly
   * @return Whether insertion was successful.
   */
  bool IndexInsertUnique(storage::ProjectedRow *index_pr, storage::TupleSlot ts);

 private:
  exec::ExecutionContext *exec_ctx_;

  std::unique_ptr<vm::Module> module_;
  std::function<void(storage::ProjectedRow *, storage::ProjectedRow *)> build_key_fn_;

  common::ManagedPointer<terrier::storage::SqlTable> table_;
  void *table_pr_buffer_;
  uint32_t table_pr_size_;
  storage::ProjectedRow *table_pr_;

  common::ManagedPointer<terrier::storage::index::Index> index_;
  void *index_pr_buffer_;
  uint32_t index_pr_size_;
  storage::ProjectedRow *index_pr_;
};

}  // namespace terrier::execution::sql