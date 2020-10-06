#pragma once

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/IR/Module.h>

#include <memory>

#include "storage/block_layout.h"
#include "storage/projected_row.h"
#include "storage/storage_defs.h"

namespace terrier::storage {

class TupleAccessor {
 public:
  explicit TupleAccessor(DataTable *datatable);

 private:
  llvm::LLVMContext context_;
  std::unique_ptr<llvm::ExecutionEngine> engine_;

  void BuildInsertInto(llvm::Module *module, DataTable *datatable);
};
}  // namespace terrier::storage
