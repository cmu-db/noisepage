#include "execution/vm/bytecode_emitter.h"

#include <limits>
#include <vector>

#include "execution/vm/bytecode_label.h"

namespace terrier::vm {

void BytecodeEmitter::EmitDeref(Bytecode bytecode, LocalVar dest, LocalVar src) {
  TPL_ASSERT(bytecode == Bytecode::Deref1 || bytecode == Bytecode::Deref2 || bytecode == Bytecode::Deref4 ||
                 bytecode == Bytecode::Deref8,
             "Bytecode is not a Deref code");
  EmitAll(bytecode, dest, src);
}

void BytecodeEmitter::EmitDerefN(LocalVar dest, LocalVar src, u32 len) { EmitAll(Bytecode::DerefN, dest, src, len); }

void BytecodeEmitter::EmitAssign(Bytecode bytecode, LocalVar dest, LocalVar src) {
  TPL_ASSERT(bytecode == Bytecode::Assign1 || bytecode == Bytecode::Assign2 || bytecode == Bytecode::Assign4 ||
                 bytecode == Bytecode::Assign8,
             "Bytecode is not an Assign code");
  EmitAll(bytecode, dest, src);
}

void BytecodeEmitter::EmitAssignImm1(LocalVar dest, i8 val) { EmitAll(Bytecode::AssignImm1, dest, val); }

void BytecodeEmitter::EmitAssignImm2(LocalVar dest, i16 val) { EmitAll(Bytecode::AssignImm2, dest, val); }

void BytecodeEmitter::EmitAssignImm4(LocalVar dest, i32 val) { EmitAll(Bytecode::AssignImm4, dest, val); }

void BytecodeEmitter::EmitAssignImm4F(LocalVar dest, f32 val) { EmitAll(Bytecode::AssignImm4F, dest, val); }

void BytecodeEmitter::EmitAssignImm8F(LocalVar dest, f64 val) { EmitAll(Bytecode::AssignImm8F, dest, val); }

void BytecodeEmitter::EmitAssignImm8(LocalVar dest, i64 val) { EmitAll(Bytecode::AssignImm8, dest, val); }

void BytecodeEmitter::EmitUnaryOp(Bytecode bytecode, LocalVar dest, LocalVar input) { EmitAll(bytecode, dest, input); }

void BytecodeEmitter::EmitBinaryOp(Bytecode bytecode, LocalVar dest, LocalVar lhs, LocalVar rhs) {
  EmitAll(bytecode, dest, lhs, rhs);
}

void BytecodeEmitter::EmitLea(LocalVar dest, LocalVar src, u32 offset) { EmitAll(Bytecode::Lea, dest, src, offset); }

void BytecodeEmitter::EmitLeaScaled(LocalVar dest, LocalVar src, LocalVar index, u32 scale, u32 offset) {
  EmitAll(Bytecode::LeaScaled, dest, src, index, scale, offset);
}

void BytecodeEmitter::EmitCall(FunctionId func_id, const std::vector<LocalVar> &params) {
  TPL_ASSERT(Bytecodes::GetNthOperandSize(Bytecode::Call, 1) == OperandSize::Short,
             "Expected argument count to be 2-byte short");
  TPL_ASSERT(params.size() < std::numeric_limits<u16>::max(), "Too many parameters!");

  EmitAll(Bytecode::Call, static_cast<u16>(func_id), static_cast<u16>(params.size()));
  for (LocalVar local : params) {
    EmitImpl(local);
  }
}

void BytecodeEmitter::EmitReturn() { EmitImpl(Bytecode::Return); }

void BytecodeEmitter::Bind(BytecodeLabel *label) {
  TPL_ASSERT(!label->is_bound(), "Cannot rebind labels");

  std::size_t curr_offset = position();

  if (label->IsForwardTarget()) {
    // We need to patch all locations in the bytecode that forward jump to the
    // given bytecode label. Each referrer is stored in the bytecode label ...
    auto &jump_locations = label->referrer_offsets();

    for (const auto &jump_location : jump_locations) {
      TPL_ASSERT((curr_offset - jump_location) < std::numeric_limits<i32>::max(),
                 "Jump delta exceeds 32-bit value for jump offsets!");

      auto delta = static_cast<i32>(curr_offset - jump_location);
      auto *raw_delta = reinterpret_cast<u8 *>(&delta);
      bytecode_->at(jump_location) = raw_delta[0];
      bytecode_->at(jump_location + 1) = raw_delta[1];
      bytecode_->at(jump_location + 2) = raw_delta[2];
      bytecode_->at(jump_location + 3) = raw_delta[3];
    }
  }

  label->BindTo(curr_offset);
}

void BytecodeEmitter::EmitJump(BytecodeLabel *label) {
  static const i32 kJumpPlaceholder = std::numeric_limits<i32>::max() - 1;

  std::size_t curr_offset = position();

  if (label->is_bound()) {
    // The label is already bound so this must be a backwards jump. We just need
    // to emit the delta offset directly into the bytestream.
    TPL_ASSERT(label->offset() <= curr_offset, "Label for backwards jump cannot be beyond current bytecode position");
    std::size_t delta = curr_offset - label->offset();
    TPL_ASSERT(delta < std::numeric_limits<i32>::max(), "Jump delta exceeds 32-bit value for jump offsets!");

    // Immediately emit the delta
    EmitScalarValue(-static_cast<i32>(delta));
  } else {
    // The label is not bound yet so this must be a forward jump. We set the
    // reference position in the label and use a placeholder offset in the
    // byte stream for now. We'll update the placeholder when the label is bound
    label->set_referrer(curr_offset);
    EmitScalarValue(kJumpPlaceholder);
  }
}

void BytecodeEmitter::EmitJump(Bytecode bytecode, BytecodeLabel *label) {
  TPL_ASSERT(Bytecodes::IsJump(bytecode), "Provided bytecode is not a jump");
  EmitAll(bytecode);
  EmitJump(label);
}

void BytecodeEmitter::EmitConditionalJump(Bytecode bytecode, LocalVar cond, BytecodeLabel *label) {
  TPL_ASSERT(Bytecodes::IsJump(bytecode), "Provided bytecode is not a jump");
  EmitAll(bytecode, cond);
  EmitJump(label);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1) {
  TPL_ASSERT(Bytecodes::NumOperands(bytecode) == 1, "Incorrect operand count for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
             "Incorrect operand type at index 0 for bytecode");
  EmitAll(bytecode, operand_1);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2) {
  TPL_ASSERT(Bytecodes::NumOperands(bytecode) == 2, "Incorrect operand count for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
             "Incorrect operand type at index 0 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
             "Incorrect operand type at index 1 for bytecode");
  EmitAll(bytecode, operand_1, operand_2);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3) {
  TPL_ASSERT(Bytecodes::NumOperands(bytecode) == 3, "Incorrect operand count for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
             "Incorrect operand type at index 0 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
             "Incorrect operand type at index 1 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 2) == OperandType::Local,
             "Incorrect operand type at index 2 for bytecode");
  EmitAll(bytecode, operand_1, operand_2, operand_3);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4) {
  TPL_ASSERT(Bytecodes::NumOperands(bytecode) == 4, "Incorrect operand count for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
             "Incorrect operand type at index 0 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
             "Incorrect operand type at index 1 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 2) == OperandType::Local,
             "Incorrect operand type at index 2 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 3) == OperandType::Local,
             "Incorrect operand type at index 3 for bytecode");
  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4, LocalVar operand_5) {
  TPL_ASSERT(Bytecodes::NumOperands(bytecode) == 5, "Incorrect operand count for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
             "Incorrect operand type at index 0 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
             "Incorrect operand type at index 1 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 2) == OperandType::Local,
             "Incorrect operand type at index 2 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 3) == OperandType::Local,
             "Incorrect operand type at index 3 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 4) == OperandType::Local,
             "Incorrect operand type at index 4 for bytecode");

  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4, operand_5);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4, LocalVar operand_5, LocalVar operand_6) {
  TPL_ASSERT(Bytecodes::NumOperands(bytecode) == 6, "Incorrect operand count for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
             "Incorrect operand type at index 0 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
             "Incorrect operand type at index 1 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 2) == OperandType::Local,
             "Incorrect operand type at index 2 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 3) == OperandType::Local,
             "Incorrect operand type at index 3 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 4) == OperandType::Local,
             "Incorrect operand type at index 4 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 5) == OperandType::Local,
             "Incorrect operand type at index 5 for bytecode");
  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4, operand_5, operand_6);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4, LocalVar operand_5, LocalVar operand_6, LocalVar operand_7) {
  TPL_ASSERT(Bytecodes::NumOperands(bytecode) == 7, "Incorrect operand count for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
             "Incorrect operand type at index 0 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
             "Incorrect operand type at index 1 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 2) == OperandType::Local,
             "Incorrect operand type at index 2 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 3) == OperandType::Local,
             "Incorrect operand type at index 3 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 4) == OperandType::Local,
             "Incorrect operand type at index 4 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 5) == OperandType::Local,
             "Incorrect operand type at index 5 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 6) == OperandType::Local,
             "Incorrect operand type at index 6 for bytecode");
  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4, operand_5, operand_6, operand_7);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4, LocalVar operand_5, LocalVar operand_6, LocalVar operand_7,
                           LocalVar operand_8) {
  TPL_ASSERT(Bytecodes::NumOperands(bytecode) == 8, "Incorrect operand count for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
             "Incorrect operand type at index 0 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
             "Incorrect operand type at index 1 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 2) == OperandType::Local,
             "Incorrect operand type at index 2 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 3) == OperandType::Local,
             "Incorrect operand type at index 3 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 4) == OperandType::Local,
             "Incorrect operand type at index 4 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 5) == OperandType::Local,
             "Incorrect operand type at index 5 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 6) == OperandType::Local,
             "Incorrect operand type at index 6 for bytecode");
  TPL_ASSERT(Bytecodes::GetNthOperandType(bytecode, 7) == OperandType::Local,
             "Incorrect operand type at index 7 for bytecode");
  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4, operand_5, operand_6, operand_7, operand_8);
}

void BytecodeEmitter::EmitThreadStateContainerIterate(LocalVar tls, LocalVar ctx, FunctionId iterate_fn) {
  EmitAll(Bytecode::ThreadStateContainerIterate, tls, ctx, iterate_fn);
}

void BytecodeEmitter::EmitThreadStateContainerReset(LocalVar tls, LocalVar state_size, FunctionId init_fn,
                                                    FunctionId destroy_fn, LocalVar ctx) {
  EmitAll(Bytecode::ThreadStateContainerReset, tls, state_size, init_fn, destroy_fn, ctx);
}

void BytecodeEmitter::EmitTableIterConstruct(Bytecode bytecode, LocalVar iter, u32 table_oid, LocalVar exec_ctx) {
  EmitAll(bytecode, iter, table_oid, exec_ctx);
}

void BytecodeEmitter::EmitAddCol(Bytecode bytecode, LocalVar iter, u32 col_oid) { EmitAll(bytecode, iter, col_oid); }

void BytecodeEmitter::EmitParallelTableScan(u32 db_oid, u32 table_oid, LocalVar ctx, LocalVar thread_states,
                                            FunctionId scan_fn) {
  EmitAll(Bytecode::ParallelScanTable, db_oid, table_oid, ctx, thread_states, scan_fn);
}

void BytecodeEmitter::EmitPCIGet(Bytecode bytecode, LocalVar out, LocalVar pci, u16 col_idx) {
  EmitAll(bytecode, out, pci, col_idx);
}

void BytecodeEmitter::EmitPCIVectorFilter(Bytecode bytecode, LocalVar selected, LocalVar pci, u32 col_idx, i8 type,
                                          i64 val) {
  EmitAll(bytecode, selected, pci, col_idx, type, val);
}

void BytecodeEmitter::EmitFilterManagerInsertFlavor(LocalVar fmb, FunctionId func) {
  EmitAll(Bytecode::FilterManagerInsertFlavor, fmb, func);
}

void BytecodeEmitter::EmitAggHashTableLookup(LocalVar dest, LocalVar agg_ht, LocalVar hash, FunctionId key_eq_fn,
                                             LocalVar arg) {
  EmitAll(Bytecode::AggregationHashTableLookup, dest, agg_ht, hash, key_eq_fn, arg);
}

void BytecodeEmitter::EmitAggHashTableProcessBatch(LocalVar agg_ht, LocalVar iters, FunctionId hash_fn,
                                                   FunctionId key_eq_fn, FunctionId init_agg_fn,
                                                   FunctionId merge_agg_fn) {
  EmitAll(Bytecode::AggregationHashTableProcessBatch, agg_ht, iters, hash_fn, key_eq_fn, init_agg_fn, merge_agg_fn);
}

void BytecodeEmitter::EmitAggHashTableMovePartitions(LocalVar agg_ht, LocalVar tls, LocalVar aht_offset,
                                                     FunctionId merge_part_fn) {
  EmitAll(Bytecode::AggregationHashTableTransferPartitions, agg_ht, tls, aht_offset, merge_part_fn);
}

void BytecodeEmitter::EmitAggHashTableParallelPartitionedScan(LocalVar agg_ht, LocalVar context, LocalVar tls,
                                                              FunctionId scan_part_fn) {
  EmitAll(Bytecode::AggregationHashTableParallelPartitionedScan, agg_ht, context, tls, scan_part_fn);
}

void BytecodeEmitter::EmitJoinHashTableIterHasNext(LocalVar has_more, LocalVar iterator, FunctionId key_eq,
                                                   LocalVar opaque_ctx, LocalVar probe_tuple) {
  EmitAll(Bytecode::JoinHashTableIterHasNext, has_more, iterator, key_eq, opaque_ctx, probe_tuple);
}

void BytecodeEmitter::EmitSorterInit(Bytecode bytecode, LocalVar sorter, LocalVar region, FunctionId cmp_fn,
                                     LocalVar tuple_size) {
  EmitAll(bytecode, sorter, region, cmp_fn, tuple_size);
}

void BytecodeEmitter::EmitOutputAlloc(Bytecode bytecode, LocalVar exec_ctx, LocalVar dest) {
  EmitAll(bytecode, exec_ctx, dest);
}

void BytecodeEmitter::EmitOutputCall(Bytecode bytecode, LocalVar exec_ctx) { EmitAll(bytecode, exec_ctx); }

void BytecodeEmitter::EmitOutputSetNull(Bytecode bytecode, LocalVar exec_ctx, LocalVar idx) {
  EmitAll(bytecode, exec_ctx, idx);
}

void BytecodeEmitter::EmitIndexIteratorConstruct(Bytecode bytecode, LocalVar iter, uint32_t table_oid,
                                                 uint32_t index_oid, LocalVar exec_ctx) {
  EmitAll(bytecode, iter, table_oid, index_oid, exec_ctx);
}

void BytecodeEmitter::EmitIndexIteratorFree(Bytecode bytecode, LocalVar iter) { EmitAll(bytecode, iter); }

void BytecodeEmitter::EmitIndexIteratorGet(Bytecode bytecode, LocalVar out, LocalVar iter, u16 col_idx) {
  EmitAll(bytecode, out, iter, col_idx);
}

void BytecodeEmitter::EmitIndexIteratorSetKey(Bytecode bytecode, LocalVar iter, u16 col_idx, LocalVar val) {
  EmitAll(bytecode, iter, col_idx, val);
}

void BytecodeEmitter::EmitInitString(Bytecode bytecode, LocalVar out, u64 length, uintptr_t data) {
  EmitAll(bytecode, out, length, data);
}

}  // namespace terrier::vm
