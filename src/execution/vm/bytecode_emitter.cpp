#include "execution/vm/bytecode_emitter.h"

#include <limits>
#include <vector>

#include "execution/vm/bytecode_label.h"

namespace terrier::execution::vm {

void BytecodeEmitter::EmitDeref(Bytecode bytecode, LocalVar dest, LocalVar src) {
  TERRIER_ASSERT(bytecode == Bytecode::Deref1 || bytecode == Bytecode::Deref2 || bytecode == Bytecode::Deref4 ||
                     bytecode == Bytecode::Deref8,
                 "Bytecode is not a Deref code");
  EmitAll(bytecode, dest, src);
}

void BytecodeEmitter::EmitDerefN(LocalVar dest, LocalVar src, uint32_t len) {
  EmitAll(Bytecode::DerefN, dest, src, len);
}

void BytecodeEmitter::EmitAssign(Bytecode bytecode, LocalVar dest, LocalVar src) {
  TERRIER_ASSERT(bytecode == Bytecode::Assign1 || bytecode == Bytecode::Assign2 || bytecode == Bytecode::Assign4 ||
                     bytecode == Bytecode::Assign8,
                 "Bytecode is not an Assign code");
  EmitAll(bytecode, dest, src);
}

void BytecodeEmitter::EmitAssignImm1(LocalVar dest, int8_t val) { EmitAll(Bytecode::AssignImm1, dest, val); }

void BytecodeEmitter::EmitAssignImm2(LocalVar dest, int16_t val) { EmitAll(Bytecode::AssignImm2, dest, val); }

void BytecodeEmitter::EmitAssignImm4(LocalVar dest, int32_t val) { EmitAll(Bytecode::AssignImm4, dest, val); }

void BytecodeEmitter::EmitAssignImm4F(LocalVar dest, float val) { EmitAll(Bytecode::AssignImm4F, dest, val); }

void BytecodeEmitter::EmitAssignImm8F(LocalVar dest, double val) { EmitAll(Bytecode::AssignImm8F, dest, val); }

void BytecodeEmitter::EmitAssignImm8(LocalVar dest, int64_t val) { EmitAll(Bytecode::AssignImm8, dest, val); }

void BytecodeEmitter::EmitUnaryOp(Bytecode bytecode, LocalVar dest, LocalVar input) { EmitAll(bytecode, dest, input); }

void BytecodeEmitter::EmitBinaryOp(Bytecode bytecode, LocalVar dest, LocalVar lhs, LocalVar rhs) {
  EmitAll(bytecode, dest, lhs, rhs);
}

void BytecodeEmitter::EmitLea(LocalVar dest, LocalVar src, uint32_t offset) {
  EmitAll(Bytecode::Lea, dest, src, offset);
}

void BytecodeEmitter::EmitLeaScaled(LocalVar dest, LocalVar src, LocalVar index, uint32_t scale, uint32_t offset) {
  EmitAll(Bytecode::LeaScaled, dest, src, index, scale, offset);
}

void BytecodeEmitter::EmitCall(FunctionId func_id, const std::vector<LocalVar> &params) {
  TERRIER_ASSERT(Bytecodes::GetNthOperandSize(Bytecode::Call, 1) == OperandSize::Short,
                 "Expected argument count to be 2-byte short");
  TERRIER_ASSERT(params.size() < std::numeric_limits<uint16_t>::max(), "Too many parameters!");

  EmitAll(Bytecode::Call, static_cast<uint16_t>(func_id), static_cast<uint16_t>(params.size()));
  for (LocalVar local : params) {
    EmitImpl(local);
  }
}

void BytecodeEmitter::EmitReturn() { EmitImpl(Bytecode::Return); }

void BytecodeEmitter::Bind(BytecodeLabel *label) {
  TERRIER_ASSERT(!label->IsBound(), "Cannot rebind labels");

  std::size_t curr_offset = Position();

  if (label->IsForwardTarget()) {
    // We need to patch all locations in the bytecode that forward jump to the
    // given bytecode label. Each referrer is stored in the bytecode label ...
    auto &jump_locations = label->ReferrerOffsets();

    for (const auto &jump_location : jump_locations) {
      TERRIER_ASSERT((curr_offset - jump_location) < std::numeric_limits<int32_t>::max(),
                     "Jump delta exceeds 32-bit value for jump offsets!");

      auto delta = static_cast<int32_t>(curr_offset - jump_location);
      auto *raw_delta = reinterpret_cast<uint8_t *>(&delta);
      bytecode_->at(jump_location) = raw_delta[0];
      bytecode_->at(jump_location + 1) = raw_delta[1];
      bytecode_->at(jump_location + 2) = raw_delta[2];
      bytecode_->at(jump_location + 3) = raw_delta[3];
    }
  }

  label->BindTo(curr_offset);
}

void BytecodeEmitter::EmitJump(BytecodeLabel *label) {
  static const int32_t k_jump_placeholder = std::numeric_limits<int32_t>::max() - 1;

  std::size_t curr_offset = Position();

  if (label->IsBound()) {
    // The label is already bound so this must be a backwards jump. We just need
    // to emit the delta offset directly into the bytestream.
    TERRIER_ASSERT(label->Offset() <= curr_offset,
                   "Label for backwards jump cannot be beyond current bytecode position");
    std::size_t delta = curr_offset - label->Offset();
    TERRIER_ASSERT(delta < std::numeric_limits<int32_t>::max(), "Jump delta exceeds 32-bit value for jump offsets!");

    // Immediately emit the delta
    EmitScalarValue(-static_cast<int32_t>(delta));
  } else {
    // The label is not bound yet so this must be a forward jump. We set the
    // reference position in the label and use a placeholder offset in the
    // byte stream for now. We'll update the placeholder when the label is bound
    label->SetReferrer(curr_offset);
    EmitScalarValue(k_jump_placeholder);
  }
}

void BytecodeEmitter::EmitJump(Bytecode bytecode, BytecodeLabel *label) {
  TERRIER_ASSERT(Bytecodes::IsJump(bytecode), "Provided bytecode is not a jump");
  EmitAll(bytecode);
  EmitJump(label);
}

void BytecodeEmitter::EmitConditionalJump(Bytecode bytecode, LocalVar cond, BytecodeLabel *label) {
  TERRIER_ASSERT(Bytecodes::IsJump(bytecode), "Provided bytecode is not a jump");
  EmitAll(bytecode, cond);
  EmitJump(label);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1) {
  TERRIER_ASSERT(Bytecodes::NumOperands(bytecode) == 1, "Incorrect operand count for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
                 "Incorrect operand type at index 0 for bytecode");
  EmitAll(bytecode, operand_1);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2) {
  TERRIER_ASSERT(Bytecodes::NumOperands(bytecode) == 2, "Incorrect operand count for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
                 "Incorrect operand type at index 0 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
                 "Incorrect operand type at index 1 for bytecode");
  EmitAll(bytecode, operand_1, operand_2);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3) {
  TERRIER_ASSERT(Bytecodes::NumOperands(bytecode) == 3, "Incorrect operand count for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
                 "Incorrect operand type at index 0 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
                 "Incorrect operand type at index 1 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 2) == OperandType::Local,
                 "Incorrect operand type at index 2 for bytecode");
  EmitAll(bytecode, operand_1, operand_2, operand_3);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4) {
  TERRIER_ASSERT(Bytecodes::NumOperands(bytecode) == 4, "Incorrect operand count for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
                 "Incorrect operand type at index 0 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
                 "Incorrect operand type at index 1 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 2) == OperandType::Local,
                 "Incorrect operand type at index 2 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 3) == OperandType::Local,
                 "Incorrect operand type at index 3 for bytecode");
  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4, LocalVar operand_5) {
  TERRIER_ASSERT(Bytecodes::NumOperands(bytecode) == 5, "Incorrect operand count for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
                 "Incorrect operand type at index 0 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
                 "Incorrect operand type at index 1 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 2) == OperandType::Local,
                 "Incorrect operand type at index 2 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 3) == OperandType::Local,
                 "Incorrect operand type at index 3 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 4) == OperandType::Local,
                 "Incorrect operand type at index 4 for bytecode");

  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4, operand_5);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4, LocalVar operand_5, LocalVar operand_6) {
  TERRIER_ASSERT(Bytecodes::NumOperands(bytecode) == 6, "Incorrect operand count for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
                 "Incorrect operand type at index 0 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
                 "Incorrect operand type at index 1 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 2) == OperandType::Local,
                 "Incorrect operand type at index 2 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 3) == OperandType::Local,
                 "Incorrect operand type at index 3 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 4) == OperandType::Local,
                 "Incorrect operand type at index 4 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 5) == OperandType::Local,
                 "Incorrect operand type at index 5 for bytecode");
  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4, operand_5, operand_6);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4, LocalVar operand_5, LocalVar operand_6, LocalVar operand_7) {
  TERRIER_ASSERT(Bytecodes::NumOperands(bytecode) == 7, "Incorrect operand count for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
                 "Incorrect operand type at index 0 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
                 "Incorrect operand type at index 1 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 2) == OperandType::Local,
                 "Incorrect operand type at index 2 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 3) == OperandType::Local,
                 "Incorrect operand type at index 3 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 4) == OperandType::Local,
                 "Incorrect operand type at index 4 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 5) == OperandType::Local,
                 "Incorrect operand type at index 5 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 6) == OperandType::Local,
                 "Incorrect operand type at index 6 for bytecode");
  EmitAll(bytecode, operand_1, operand_2, operand_3, operand_4, operand_5, operand_6, operand_7);
}

void BytecodeEmitter::Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3,
                           LocalVar operand_4, LocalVar operand_5, LocalVar operand_6, LocalVar operand_7,
                           LocalVar operand_8) {
  TERRIER_ASSERT(Bytecodes::NumOperands(bytecode) == 8, "Incorrect operand count for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 0) == OperandType::Local,
                 "Incorrect operand type at index 0 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 1) == OperandType::Local,
                 "Incorrect operand type at index 1 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 2) == OperandType::Local,
                 "Incorrect operand type at index 2 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 3) == OperandType::Local,
                 "Incorrect operand type at index 3 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 4) == OperandType::Local,
                 "Incorrect operand type at index 4 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 5) == OperandType::Local,
                 "Incorrect operand type at index 5 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 6) == OperandType::Local,
                 "Incorrect operand type at index 6 for bytecode");
  TERRIER_ASSERT(Bytecodes::GetNthOperandType(bytecode, 7) == OperandType::Local,
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

void BytecodeEmitter::EmitTableIterInit(Bytecode bytecode, LocalVar iter, LocalVar exec_ctx, uint32_t table_oid,
                                        LocalVar col_oids, uint32_t num_oids) {
  EmitAll(bytecode, iter, exec_ctx, table_oid, col_oids, num_oids);
}

void BytecodeEmitter::EmitAddCol(Bytecode bytecode, LocalVar iter, uint32_t col_oid) {
  EmitAll(bytecode, iter, col_oid);
}

void BytecodeEmitter::EmitParallelTableScan(uint32_t db_oid, uint32_t table_oid, LocalVar ctx, LocalVar thread_states,
                                            FunctionId scan_fn) {
  EmitAll(Bytecode::ParallelScanTable, db_oid, table_oid, ctx, thread_states, scan_fn);
}

void BytecodeEmitter::EmitPCIGet(Bytecode bytecode, LocalVar out, LocalVar pci, uint16_t col_idx) {
  EmitAll(bytecode, out, pci, col_idx);
}

void BytecodeEmitter::EmitPCIVectorFilter(Bytecode bytecode, LocalVar selected, LocalVar pci, uint32_t col_idx,
                                          int8_t type, int64_t val) {
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

void BytecodeEmitter::EmitIndexIteratorInit(Bytecode bytecode, LocalVar iter, LocalVar exec_ctx, uint32_t table_oid,
                                            uint32_t index_oid, LocalVar col_oids, uint32_t num_oids) {
  EmitAll(bytecode, iter, exec_ctx, table_oid, index_oid, col_oids, num_oids);
}

void BytecodeEmitter::EmitIndexIteratorFree(Bytecode bytecode, LocalVar iter) { EmitAll(bytecode, iter); }

void BytecodeEmitter::EmitIndexIteratorGet(Bytecode bytecode, LocalVar out, LocalVar iter, uint16_t col_idx) {
  EmitAll(bytecode, out, iter, col_idx);
}

void BytecodeEmitter::EmitIndexIteratorSetKey(Bytecode bytecode, LocalVar iter, uint16_t col_idx, LocalVar val) {
  EmitAll(bytecode, iter, col_idx, val);
}

void BytecodeEmitter::EmitInitString(Bytecode bytecode, LocalVar out, uint64_t length, uintptr_t data) {
  EmitAll(bytecode, out, length, data);
}

void BytecodeEmitter::EmitPRGet(Bytecode bytecode, LocalVar out, LocalVar pr, uint16_t col_idx) {
  EmitAll(bytecode, out, pr, col_idx);
}

void BytecodeEmitter::EmitPRSet(Bytecode bytecode, LocalVar pr, uint16_t col_idx, LocalVar val) {
  EmitAll(bytecode, pr, col_idx, val);
}

}  // namespace terrier::execution::vm
