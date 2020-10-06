#include "storage/tuple_accessor.h"

#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Module.h>
#include <llvm/IR/PassManager.h>
#include <llvm/IR/Verifier.h>

#include "common/constants.h"
#include "common/llvm.h"
#include "storage/data_table.h"
#include "storage/storage_util.h"
#include "transaction/transaction_context.h"

namespace terrier::storage {

static const char INSERT_INTO_NAME[] = "insert_into";

TupleAccessor::TupleAccessor(DataTable *datatable) {
  auto module = std::make_unique<llvm::Module>("tuple_accessor", context_);

  BuildInsertInto(module.get(), datatable);

  llvm::ModuleAnalysisManager mam;
  llvm::VerifierAnalysis verifier;
  auto UNUSED_ATTRIBUTE result = verifier.run(*module, mam);
  TERRIER_ASSERT(!result.IRBroken, "Compilation must not fail");

  std::string err;
  engine_ = std::unique_ptr<llvm::ExecutionEngine>(llvm::EngineBuilder(std::move(module))
                                                       .setErrorStr(&err)
                                                       .setOptLevel(llvm::CodeGenOpt::Aggressive)
                                                       .setEngineKind(llvm::EngineKind::JIT)
                                                       .create());
  if (engine_ == nullptr) {
    throw std::runtime_error(err);
  }
  TERRIER_ASSERT(engine_ != nullptr, "Engine initialization should not fail");

  datatable->InsertInto = (void (*)(terrier::common::ManagedPointer<terrier::transaction::TransactionContext>,
                                    const terrier::storage::ProjectedRow &,
                                    terrier::storage::TupleSlot))engine_->getFunctionAddress(INSERT_INTO_NAME);
}

void TupleAccessor::BuildInsertInto(llvm::Module *module, DataTable *datatable) {
  auto const &layout = datatable->GetBlockLayout();

  /* Define the LLVM prototype of 'TransactionContext::UndoRecordForInsert' function */
  llvm::FunctionType *insert_undo_fn_type =
      llvm::FunctionType::get(/* returns opaque pointer */ llvm::IntegerType::getInt64Ty(context_),
                              {/* TransactionContext pointer (implicit) */ llvm::IntegerType::getInt64Ty(context_),
                               /* DataTable pointer */ llvm::IntegerType::getInt64Ty(context_),
                               /* TupleSlot */ llvm::IntegerType::getInt64Ty(context_)},
                              /* varargs */ false);

  llvm::FunctionType *insert_into_fn_type =
      llvm::FunctionType::get(/* returns void */ llvm::IntegerType::getVoidTy(context_),
                              {/* TransactionContext pointer */ llvm::IntegerType::getInt64Ty(context_),
                               /* ProjectedRow pointer */ llvm::IntegerType::getInt64Ty(context_),
                               /* TupleSlot */ llvm::IntegerType::getInt64Ty(context_)},
                              /* varargs */ false);

  /* Define the LLVM prototype of the 'InsertInto' function */
  module->getOrInsertFunction(INSERT_INTO_NAME, insert_into_fn_type);
  llvm::Function *fn = module->getFunction(INSERT_INTO_NAME);

  /* Set the calling convention */
  fn->setCallingConv(llvm::CallingConv::C);

  /* Get handles to the arguments */
  llvm::Function::arg_iterator args = fn->arg_begin();
  llvm::Value *txn = args++;
  llvm::Value *pr = args++;
  llvm::Value *tuple = args++;
  TERRIER_ASSERT(args == fn->arg_end(), "Unexpected extra arguments in 'insert_into'");

  llvm::BasicBlock *entry = llvm::BasicBlock::Create(context_, "entry", fn);
  llvm::IRBuilder<> builder(entry);

  // Separate tuple slot into block and index parts
  auto mask = static_cast<uintptr_t>(common::Constants::BLOCK_SIZE) - 1;
  auto block_addr = builder.CreateAnd(tuple, ~mask);
  auto index = builder.CreateAnd(tuple, mask);

  // Get the undo record
  auto datatable_addr = builder.getInt64(reinterpret_cast<uintptr_t>(datatable));
  auto undo_fn_addr =
      builder.getInt64(reinterpret_cast<uintptr_t>(&transaction::TransactionContext::UndoRecordForInsert));
  auto undo_fn = builder.CreateIntToPtr(
      undo_fn_addr, llvm::PointerType::get(insert_undo_fn_type, common::LLVM::DEFAULT_ADDRESS_SPACE));
  auto undo_addr = builder.CreateCall(insert_undo_fn_type, undo_fn, {txn, datatable_addr, tuple});

  // Update the version pointer
  //     1. Get version pointer's address
  auto version_col_offset = builder.getInt64(layout.ColumnDataOffset(VERSION_POINTER_COLUMN_ID));
  auto version_slot_width = builder.getInt64(sizeof(uintptr_t));
  auto version_slot_offset = builder.CreateMul(index, version_slot_width);
  auto version_ptr_offset = builder.CreateAdd(version_col_offset, version_slot_offset);
  auto version_ptr_addr = builder.CreateAdd(block_addr, version_ptr_offset);
  auto version_ptr = builder.CreateIntToPtr(
      version_ptr_addr, llvm::PointerType::get(builder.getInt64Ty(), common::LLVM::DEFAULT_ADDRESS_SPACE));
  //     2. Store undo_addr at that address
  builder.CreateStore(undo_addr, version_ptr);

  auto pri = ProjectedRowInitializer::Create(layout, layout.AllColumns());
  // Copy over the data [looped over columns]
  uint32_t pr_index = 0;
  for (auto col_id : layout.AllColumns()) {
    //     0. Determine the column's data width
    auto value_width = AttrSizeBytes(layout.AttrSize(col_id));
    llvm::IntegerType *data_type;
    switch (value_width) {
      case 1:
        data_type = builder.getInt8Ty();
        break;
      case 2:
        data_type = builder.getInt16Ty();
        break;
      case 4:
        data_type = builder.getInt32Ty();
        break;
      case 8:
        data_type = builder.getInt64Ty();
        break;
      case 16:
        data_type = builder.getInt128Ty();
        break;
      default:
        throw std::runtime_error("unexpected type size");
    }
    //     1. Get source address
    auto data_ptr_offset = builder.getInt64(pri.offsets_[pr_index]);
    auto data_ptr_addr = builder.CreateAdd(pr, data_ptr_offset);
    auto data_ptr =
        builder.CreateIntToPtr(data_ptr_addr, llvm::PointerType::get(data_type, common::LLVM::DEFAULT_ADDRESS_SPACE));
    //     2. Load (with correct width)
    auto data = builder.CreateLoad(data_ptr);
    //     3. Get destination address
    auto col_offset = builder.getInt64(layout.ColumnDataOffset(col_id));
    auto col_val_width = builder.getInt64(value_width);
    auto col_slot_array_offset = builder.CreateMul(index, col_val_width);
    auto col_slot_offset = builder.CreateAdd(col_offset, col_slot_array_offset);
    auto col_slot_addr = builder.CreateAdd(block_addr, col_slot_offset);
    auto col_slot_ptr =
        builder.CreateIntToPtr(col_slot_addr, llvm::PointerType::get(data_type, common::LLVM::DEFAULT_ADDRESS_SPACE));
    //     4. Store
    builder.CreateStore(data, col_slot_ptr);
    pr_index++;
  }

  // Create bitmap masks
  auto bitmap_byte_index = builder.CreateLShr(index, 3);           // Divide by 8
  auto bitmap_bit_index = builder.CreateAnd(index, (1 << 3) - 1);  // Remainder 8
  auto bitmap_bit_index_i8 = builder.CreateTrunc(bitmap_bit_index, builder.getInt8Ty());
  auto bitmap_set_mask = builder.CreateShl(builder.getInt8(1), bitmap_bit_index_i8);
  auto bitmap_clear_mask = builder.CreateXor(bitmap_set_mask, builder.getInt8(0xFF));

  // Set the null bits [if nullable] (CAS) [looped over columns]
  uint32_t num_cols = layout.AllColumns().size();
  uint32_t pr_bitmap_offset =
      StorageUtil::PadUpToSize(sizeof(uint32_t), sizeof(uint32_t) /* size */ + sizeof(uint16_t) /* num_cols */ +
                                                     num_cols * sizeof(uint16_t) /* column IDs */) +
      num_cols * sizeof(uint32_t) /* column offsets */;
  pr_index = 0;
  for (auto col_id : layout.AllColumns()) {
    //     1. Get source addr
    auto pr_bm_byte_index_raw = builder.getInt64(pr_index);
    auto pr_bm_offset = builder.getInt64(pr_bitmap_offset);
    auto pr_bm_byte_index = builder.CreateLShr(pr_bm_byte_index_raw, 3);  // Divide by 8
    auto pr_null_byte_offset = builder.CreateAdd(pr_bm_offset, pr_bm_byte_index);
    auto pr_null_byte_addr = builder.CreateAdd(pr, pr_null_byte_offset);
    auto pr_null_byte_ptr = builder.CreateIntToPtr(
        pr_null_byte_addr, llvm::PointerType::get(builder.getInt8Ty(), common::LLVM::DEFAULT_ADDRESS_SPACE));
    //     2. Load data
    auto pr_null_byte = builder.CreateLoad(pr_null_byte_ptr);
    //     3. Mask and normalize
    auto pr_bm_bit_index = builder.CreateAnd(pr_bm_byte_index_raw, (1 << 3) - 1);  // Remainder 8
    auto pr_null_mask = builder.CreateShl(builder.getInt8(1), pr_bm_bit_index);
    auto null_val = builder.CreateAnd(pr_null_byte, pr_null_mask);
    //     5. Get dest addr
    auto nullmap_offset = builder.getInt64(layout.ColumnBitmapOffset(col_id));
    auto null_byte_offset = builder.CreateAdd(nullmap_offset, bitmap_byte_index);
    auto null_byte_addr = builder.CreateAdd(block_addr, null_byte_offset);
    auto null_byte_ptr = builder.CreateIntToPtr(
        null_byte_addr, llvm::PointerType::get(builder.getInt8Ty(), common::LLVM::DEFAULT_ADDRESS_SPACE));
    //     6. Branch on non-zero
    auto null_block = llvm::BasicBlock::Create(context_, "null", fn);
    auto not_null_block = llvm::BasicBlock::Create(context_, "not_null", fn);
    auto common_block = llvm::BasicBlock::Create(context_, "common", fn);
    auto is_null = builder.CreateICmpEQ(null_val, builder.getInt8(0));
    builder.CreateCondBr(is_null, null_block, not_null_block);
    //     7. If null
    builder.SetInsertPoint(null_block);
    builder.CreateAtomicRMW(llvm::AtomicRMWInst::BinOp::And, null_byte_ptr, bitmap_clear_mask,
                            llvm::AtomicOrdering::Monotonic);
    builder.CreateBr(common_block);
    //     8. If not null
    builder.SetInsertPoint(not_null_block);
    builder.CreateAtomicRMW(llvm::AtomicRMWInst::BinOp::Or, null_byte_ptr, bitmap_set_mask,
                            llvm::AtomicOrdering::Monotonic);
    builder.CreateBr(common_block);
    //     9. Set to common insertion point
    builder.SetInsertPoint(common_block);
    pr_index++;
  }

  // Mark as present (CAS)
  //     1. Get dest addr
  auto alloc_map_offset = builder.getInt64(layout.ColumnBitmapOffset(VERSION_POINTER_COLUMN_ID));
  auto alloc_byte_offset = builder.CreateAdd(alloc_map_offset, bitmap_byte_index);
  auto alloc_byte_addr = builder.CreateAdd(block_addr, alloc_byte_offset);
  auto alloc_byte_ptr = builder.CreateIntToPtr(
      alloc_byte_addr, llvm::PointerType::get(builder.getInt8Ty(), common::LLVM::DEFAULT_ADDRESS_SPACE));
  //     2. Atomic OR (release consistency - occurs after all prior mem ops)
  builder.CreateAtomicRMW(llvm::AtomicRMWInst::BinOp::Or, alloc_byte_ptr, bitmap_set_mask,
                          llvm::AtomicOrdering::Release);
  builder.CreateRetVoid();
}

}  // namespace terrier::storage
