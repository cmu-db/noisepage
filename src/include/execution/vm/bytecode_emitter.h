#pragma once

#include <cstdint>
#include <vector>

#include "execution/util/common.h"
#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecodes.h"

namespace tpl::vm {

class BytecodeLabel;

class BytecodeEmitter {
 public:
  /// Construct a bytecode emitter instance that emits bytecode operations into
  /// the provided bytecode vector
  explicit BytecodeEmitter(std::vector<u8> *bytecode) : bytecode_(bytecode) {}

  /// Cannot copy or move this class
  DISALLOW_COPY_AND_MOVE(BytecodeEmitter);

  /// Access the current position of the emitter in the bytecode stream
  std::size_t position() const { return bytecode_->size(); }

  // -------------------------------------------------------
  // Derefs
  // -------------------------------------------------------

  void EmitDeref(Bytecode bytecode, LocalVar dest, LocalVar src);
  void EmitDerefN(LocalVar dest, LocalVar src, u32 len);

  // -------------------------------------------------------
  // Assignment
  // -------------------------------------------------------

  void EmitAssign(Bytecode bytecode, LocalVar dest, LocalVar src);
  void EmitAssignImm1(LocalVar dest, i8 val);
  void EmitAssignImm2(LocalVar dest, i16 val);
  void EmitAssignImm4(LocalVar dest, i32 val);
  void EmitAssignImm8(LocalVar dest, i64 val);

  // -------------------------------------------------------
  // Jumps
  // -------------------------------------------------------

  // Bind the given label to the current bytecode position
  void Bind(BytecodeLabel *label);

  void EmitJump(Bytecode bytecode, BytecodeLabel *label);
  void EmitConditionalJump(Bytecode bytecode, LocalVar cond,
                           BytecodeLabel *label);

  // -------------------------------------------------------
  // Load-effective-address
  // -------------------------------------------------------

  void EmitLea(LocalVar dest, LocalVar src, u32 offset);
  void EmitLeaScaled(LocalVar dest, LocalVar src, LocalVar index, u32 scale,
                     u32 offset);

  // -------------------------------------------------------
  // Calls and returns
  // -------------------------------------------------------

  void EmitCall(FunctionId func_id, const std::vector<LocalVar> &params);
  void EmitReturn();

  // -------------------------------------------------------
  // Generic unary and binary operations
  // -------------------------------------------------------

  void EmitUnaryOp(Bytecode bytecode, LocalVar dest, LocalVar input);
  void EmitBinaryOp(Bytecode bytecode, LocalVar dest, LocalVar lhs,
                    LocalVar rhs);

  // -------------------------------------------------------
  // Generic emissions
  // -------------------------------------------------------

  void Emit(Bytecode bytecode, LocalVar operand_1);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2,
            LocalVar operand_3);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2,
            LocalVar operand_3, LocalVar operand_4);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2,
            LocalVar operand_3, LocalVar operand_4, LocalVar operand_5);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2,
            LocalVar operand_3, LocalVar operand_4, LocalVar operand_5,
            LocalVar operand_6);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2,
            LocalVar operand_3, LocalVar operand_4, LocalVar operand_5,
            LocalVar operand_6, LocalVar operand_7);
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2,
            LocalVar operand_3, LocalVar operand_4, LocalVar operand_5,
            LocalVar operand_6, LocalVar operand_7, LocalVar operand_8);

  // -------------------------------------------------------
  // Tables
  // -------------------------------------------------------

  void EmitTableIteratorInit(Bytecode bytecode, LocalVar iter, u32 db_oid,
                             u32 table_id, uintptr_t exec_context);

  // Reading integer values from an iterator
  void EmitPCIGet(Bytecode bytecode, LocalVar out, LocalVar pci, u32 col_idx);

  // Filter a column in the iterator by a constant value
  void EmitPCIVectorFilter(Bytecode bytecode, LocalVar selected, LocalVar pci,
                           u32 col_idx, i8 type, i64 val);

  // --------------------------------------------
  // Output calls
  // --------------------------------------------
  void EmitOutputAlloc(Bytecode bytecode, uintptr_t ptr, LocalVar dest);
  void EmitOutputCall(Bytecode bytecode, uintptr_t ptr);
  void EmitOutputSetNull(Bytecode bytecode, uintptr_t ptr, LocalVar idx);
  void EmitIndexIteratorInit(Bytecode bytecode, LocalVar iter,
                             uint32_t index_oid, uintptr_t ptr);
  void EmitIndexIteratorScanKey(Bytecode bytecode, LocalVar iter, LocalVar key);
  void EmitIndexIteratorFree(Bytecode bytecode, LocalVar iter);
  void EmitIndexIteratorGet(Bytecode bytecode, LocalVar out, LocalVar iter,
                            u32 col_idx);

 private:
  // Copy a scalar immediate value into the bytecode stream
  template <typename T>
  auto EmitScalarValue(T val) -> std::enable_if_t<std::is_integral_v<T>> {
    bytecode_->insert(bytecode_->end(), sizeof(T), 0);
    *reinterpret_cast<T *>(&*(bytecode_->end() - sizeof(T))) = val;
  }

  // Emit a bytecode
  void EmitImpl(Bytecode bytecode) {
    EmitScalarValue(Bytecodes::ToByte(bytecode));
  }

  // Emit a local variable reference by encoding it into the bytecode stream
  void EmitImpl(LocalVar local) { EmitScalarValue(local.Encode()); }

  // Emit an integer immediate value
  template <typename T>
  auto EmitImpl(T val) -> std::enable_if_t<std::is_integral_v<T>> {
    EmitScalarValue(val);
  }

  // Emit all arguments in sequence
  template <typename... ArgTypes>
  void EmitAll(ArgTypes... args) {
    (EmitImpl(args), ...);
  }

  //
  void EmitJump(BytecodeLabel *label);

 private:
  std::vector<u8> *bytecode_;
};

}  // namespace tpl::vm
