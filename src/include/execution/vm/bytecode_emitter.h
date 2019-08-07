#pragma once

#include <cstdint>
#include <vector>

#include "execution/util/common.h"
#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecodes.h"

namespace terrier::execution::vm {

class BytecodeLabel;

/**
 * Defines functions that allow bytecode emission.
 */
class BytecodeEmitter {
 public:
  /**
   * Construct a bytecode emitter instance that emits bytecode operations into
   * the provided bytecode vector
   * @param bytecode vector to emit bytecodes into
   */
  explicit BytecodeEmitter(std::vector<u8> *bytecode) : bytecode_(bytecode) {}

  /**
   * Cannot copy or move this class
   */
  DISALLOW_COPY_AND_MOVE(BytecodeEmitter);

  /**
   * @return the current position of the emitter in the bytecode stream
   */
  std::size_t position() const { return bytecode_->size(); }

  // -------------------------------------------------------
  // Derefs
  // -------------------------------------------------------

  /**
   * Emit fixed (1 byte, 2 bytes, 4 bytes or 8 bytes) dereference code
   * @param bytecode dereference bytecode
   * @param dest where to store dereference
   * @param src what to dereference
   */
  void EmitDeref(Bytecode bytecode, LocalVar dest, LocalVar src);

  /**
   * Emits arbitrary dereference code.
   * @param dest where to store dereference
   * @param src what to dereference
   * @param len length of the source object
   */
  void EmitDerefN(LocalVar dest, LocalVar src, u32 len);

  // -------------------------------------------------------
  // Assignment
  // -------------------------------------------------------

  /**
   * Emit arbitrary assignment code
   * @param bytecode assignment bytecode
   * @param dest destination variable
   * @param src source variable
   */
  void EmitAssign(Bytecode bytecode, LocalVar dest, LocalVar src);

  /**
   * Emit assignment code for 1 byte values.
   * @param dest destination variable
   * @param val value to assign
   */
  void EmitAssignImm1(LocalVar dest, i8 val);

  /**
   * Emit assignment code for 2 byte values.
   * @param dest destination variable
   * @param val value to assign
   */
  void EmitAssignImm2(LocalVar dest, i16 val);

  /**
   * Emit assignment code for 4 byte values.
   * @param dest destination variable
   * @param val value to assign
   */
  void EmitAssignImm4(LocalVar dest, i32 val);

  /**
   * Emit assignment code for 4 byte float values.
   * @param dest destination variable
   * @param val value to assign
   */
  void EmitAssignImm4F(LocalVar dest, f32 val);

  /**
   * Emit assignment code for 8 byte float values.
   * @param dest destination variable
   * @param val value to assign
   */
  void EmitAssignImm8F(LocalVar dest, f64 val);

  /**
   * Emit assignment code for 8 byte values.
   * @param dest destination variable
   * @param val value to assign
   */
  void EmitAssignImm8(LocalVar dest, i64 val);

  // -------------------------------------------------------
  // Jumps
  // -------------------------------------------------------

  /**
   * Bind the given label to the current bytecode position
   * @param label label to bind
   */
  void Bind(BytecodeLabel *label);

  /**
   * Emits jump code
   * @param bytecode jump bytecode to emit
   * @param label label to jump to
   */
  void EmitJump(Bytecode bytecode, BytecodeLabel *label);

  /**
   * Emits a conditional jump code. The jump is performed when the given condition holds.
   * @param bytecode jump bytecode to emit
   * @param cond jump condition
   * @param label label to jump to
   */
  void EmitConditionalJump(Bytecode bytecode, LocalVar cond, BytecodeLabel *label);

  // -------------------------------------------------------
  // Load-effective-address
  // -------------------------------------------------------

  /**
   * Emit Lea code
   * @param dest destination variable
   * @param src source variable
   * @param offset offset to load starting from the source variable
   */
  void EmitLea(LocalVar dest, LocalVar src, u32 offset);

  /**
   * Emits scaled Lea code.
   * The loaded address is found using src + (scale * index) + offset
   * @param dest destination variable
   * @param src source variable
   * @param index index of the element to access
   * @param scale element size
   * @param offset additional offset of the element to load
   */
  void EmitLeaScaled(LocalVar dest, LocalVar src, LocalVar index, u32 scale, u32 offset);

  // -------------------------------------------------------
  // Calls and returns
  // -------------------------------------------------------

  /**
   * Emit a function call
   * @param func_id id of the function to call
   * @param params parameters of the function
   */
  void EmitCall(FunctionId func_id, const std::vector<LocalVar> &params);

  /**
   * Emit a return bytecode
   */
  void EmitReturn();

  // -------------------------------------------------------
  // Generic unary and binary operations
  // -------------------------------------------------------

  /**
   * Emit unary operator code
   * @param bytecode bytecode of the unary operation
   * @param dest destination variable
   * @param input input of the iterator
   */
  void EmitUnaryOp(Bytecode bytecode, LocalVar dest, LocalVar input);

  /**
   * Emit binary operator code
   * @param bytecode bytecode of the binary operation
   * @param dest destination variable
   * @param lhs lhs of the operator
   * @param rhs rhs of the operator
   */
  void EmitBinaryOp(Bytecode bytecode, LocalVar dest, LocalVar lhs, LocalVar rhs);

  // -------------------------------------------------------
  // Generic emissions
  // -------------------------------------------------------

  /**
   * Emit arbitrary bytecode with one operand
   * @param bytecode bytecode to emit
   * @param operand_1 first operand
   */
  void Emit(Bytecode bytecode, LocalVar operand_1);

  /**
   * Emit arbitrary bytecode with two operand
   * @param bytecode bytecode to emit
   * @param operand_1 first operand
   * @param operand_2 second operand
   */
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2);

  /**
   * Emit arbitrary bytecode with three operand
   * @param bytecode bytecode to emit
   * @param operand_1 first operand
   * @param operand_2 second operand
   * @param operand_3 third operand
   */
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3);

  /**
   * Emit arbitrary bytecode with three operand
   * @param bytecode bytecode to emit
   * @param operand_1 first operand
   * @param operand_2 second operand
   * @param operand_3 third operand
   * @param operand_4 fourth operand
   */
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3, LocalVar operand_4);

  /**
   * Emit arbitrary bytecode with three operand
   * @param bytecode bytecode to emit
   * @param operand_1 first operand
   * @param operand_2 second operand
   * @param operand_3 third operand
   * @param operand_4 fourth operand
   * @param operand_5 fifth operand
   */
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3, LocalVar operand_4,
            LocalVar operand_5);

  /**
   * Emit arbitrary bytecode with three operand
   * @param bytecode bytecode to emit
   * @param operand_1 first operand
   * @param operand_2 second operand
   * @param operand_3 third operand
   * @param operand_4 fourth operand
   * @param operand_5 fifth operand
   * @param operand_6 sixth operand
   */
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3, LocalVar operand_4,
            LocalVar operand_5, LocalVar operand_6);

  /**
   * Emit arbitrary bytecode with three operand
   * @param bytecode bytecode to emit
   * @param operand_1 first operand
   * @param operand_2 second operand
   * @param operand_3 third operand
   * @param operand_4 fourth operand
   * @param operand_5 fifth operand
   * @param operand_6 sixth operand
   * @param operand_7 seventh operand
   */
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3, LocalVar operand_4,
            LocalVar operand_5, LocalVar operand_6, LocalVar operand_7);

  /**
   * Emit arbitrary bytecode with three operand
   * @param bytecode bytecode to emit
   * @param operand_1 first operand
   * @param operand_2 second operand
   * @param operand_3 third operand
   * @param operand_4 fourth operand
   * @param operand_5 fifth operand
   * @param operand_6 sixth operand
   * @param operand_7 seventh operand
   * @param operand_8 eighth operand
   */
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3, LocalVar operand_4,
            LocalVar operand_5, LocalVar operand_6, LocalVar operand_7, LocalVar operand_8);

  // -------------------------------------------------------
  // Special
  // -------------------------------------------------------

  /**
   * Iterate over all the states in the container
   */
  void EmitThreadStateContainerIterate(LocalVar tls, LocalVar ctx, FunctionId iterate_fn);

  /**
   * Reset a thread state container with init and destroy functions
   */
  void EmitThreadStateContainerReset(LocalVar tls, LocalVar state_size, FunctionId init_fn, FunctionId destroy_fn,
                                     LocalVar ctx);

  /**
   * Emit TVI init code
   * @param bytecode init bytecode
   * @param iter TVI to initialize
   * @param table_oid oid of the sql table
   * @param exec_ctx execution context
   */
  void EmitTableIterConstruct(Bytecode bytecode, LocalVar iter, u32 table_oid, LocalVar exec_ctx);

  /**
   * Emit bytecode to add a column for scanning
   * @param bytecode bytecode to emit
   * @param iter TVI and index iterator
   * @param col_oid oid of the column
   */
  void EmitAddCol(Bytecode bytecode, LocalVar iter, u32 col_oid);

  /**
   * Emit a parallel table scan
   */
  void EmitParallelTableScan(u32 db_oid, u32 table_oid, LocalVar ctx, LocalVar thread_states, FunctionId scan_fn);

  // Reading integer values from an iterator
  /**
   * Emit bytecode to read from a PCI
   * @param bytecode PCIGet bytecode
   * @param out destination variable
   * @param pci PCI to read
   * @param col_idx index of the column to read
   */
  void EmitPCIGet(Bytecode bytecode, LocalVar out, LocalVar pci, u16 col_idx);

  /**
   * Filter a column in the iterator by a constant value
   * @param bytecode filter bytecode to emit
   * @param selected output variable for the number of selected values
   * @param pci PCI to filter
   * @param col_idx index of the iterator to filter
   * @param type type of the column
   * @param val filter value
   */
  void EmitPCIVectorFilter(Bytecode bytecode, LocalVar selected, LocalVar pci, u32 col_idx, i8 type, i64 val);

  /**
   * Insert a filter flavor into the filter manager builder
   */
  void EmitFilterManagerInsertFlavor(LocalVar fmb, FunctionId func);

  /**
   * Lookup a single entry in the aggregation hash table
   */
  void EmitAggHashTableLookup(LocalVar dest, LocalVar agg_ht, LocalVar hash, FunctionId key_eq_fn, LocalVar arg);

  /**
   * Process a batch of input into the aggregation hash table
   */
  void EmitAggHashTableProcessBatch(LocalVar agg_ht, LocalVar iters, FunctionId hash_fn, FunctionId key_eq_fn,
                                    FunctionId init_agg_fn, FunctionId merge_agg_fn);

  /**
   * Emit move partition code
   */
  void EmitAggHashTableMovePartitions(LocalVar agg_ht, LocalVar tls, LocalVar aht_offset, FunctionId merge_part_fn);

  /**
   * Emit parallel partition scan code
   */
  void EmitAggHashTableParallelPartitionedScan(LocalVar agg_ht, LocalVar context, LocalVar tls,
                                               FunctionId scan_part_fn);

  /**
   * Emit join table iteration code
   */
  void EmitJoinHashTableIterHasNext(LocalVar has_more, LocalVar iterator, FunctionId key_eq, LocalVar opaque_ctx,
                                    LocalVar probe_tuple);

  /**
   * Initialize a sorter instance
   */
  void EmitSorterInit(Bytecode bytecode, LocalVar sorter, LocalVar region, FunctionId cmp_fn, LocalVar tuple_size);

  // --------------------------------------------
  // Output calls
  // --------------------------------------------
  /**
   * Emit output slot allocation code.
   * @param bytecode output allocation bytecode
   * @param exec_ctx to execution context
   * @param dest destination variable
   */
  void EmitOutputAlloc(Bytecode bytecode, LocalVar exec_ctx, LocalVar dest);

  /**
   * Generic helper method to emit output calls.
   * @param bytecode bytecode to emit
   * @param exec_ctx the execution context
   */
  void EmitOutputCall(Bytecode bytecode, LocalVar exec_ctx);

  /**
   * Emit code to set a column to null
   * TODO(Amadou): Remove this if it's indeed unneeded.
   * @param bytecode bytecode to set to null
   * @param exec_ctx the execution_context
   * @param idx index of the columns
   */
  void EmitOutputSetNull(Bytecode bytecode, LocalVar exec_ctx, LocalVar idx);

  // -------------------------------------------
  // Index Calls
  // -------------------------------------------

  /**
   * Emit code to initialize an index iterator
   * @param bytecode index initialization bytecode
   * @param iter iterator in initialize
   * @param table_oid oid of the table owning the index
   * @param index_oid oid of the index to use
   * @param exec_ctx the execution context
   */
  void EmitIndexIteratorConstruct(Bytecode bytecode, LocalVar iter, uint32_t table_oid, uint32_t index_oid,
                                  LocalVar exec_ctx);

  /**
   * Emit code to free an index iterator
   * @param bytecode bytecode used to free an index iterator
   * @param iter index iterator to free
   */
  void EmitIndexIteratorFree(Bytecode bytecode, LocalVar iter);

  /**
   * Emit code to get columns of tuples with are iterator over
   * @param bytecode bytecode corresponding to the column type
   * @param out destination variable
   * @param iter index iterator to use
   * @param col_idx index of the column to access
   */
  void EmitIndexIteratorGet(Bytecode bytecode, LocalVar out, LocalVar iter, u16 col_idx);

  /**
   * Emit code to set the index's scan key
   * @param bytecode bytecode corresponding to the column type
   * @param iter index iterator to use
   * @param col_idx index of the column to set
   * @param val value to write
   */
  void EmitIndexIteratorSetKey(Bytecode bytecode, LocalVar iter, u16 col_idx, LocalVar val);

  /**
   * Initialize a StringVal from a char array
   * @param bytecode bytecode to emit
   * @param out where to store the result
   * @param length length of the string
   * @param data pointer to the char array
   */
  void EmitInitString(Bytecode bytecode, LocalVar out, u64 length, uintptr_t data);

  /**
   * Copy a scalar immediate value into the bytecode stream
   * @tparam T type of the value
   * @param val value to copy
   * @return nothing
   */
  template <typename T>
  auto EmitScalarValue(T val) -> std::enable_if_t<std::is_arithmetic_v<T>> {
    bytecode_->insert(bytecode_->end(), sizeof(T), 0);
    *reinterpret_cast<T *>(&*(bytecode_->end() - sizeof(T))) = val;
  }

  /**
   * Emit a bytecode
   * @param bytecode bytecode to emit
   */
  void EmitImpl(Bytecode bytecode) { EmitScalarValue(Bytecodes::ToByte(bytecode)); }

  /**
   * Emit a local variable reference by encoding it into the bytecode stream
   * @param local local variable to referenece
   */
  void EmitImpl(LocalVar local) { EmitScalarValue(local.Encode()); }

  /**
   * Emit an integer immediate value
   * @tparam T type of the value
   * @param val value to emit
   * @return nothing
   */
  template <typename T>
  auto EmitImpl(T val) -> std::enable_if_t<std::is_arithmetic_v<T>> {
    EmitScalarValue(val);
  }

  /**
   * Emit all arguments in sequence
   * @tparam ArgTypes types of the arguments
   * @param args list of arguments
   */
  template <typename... ArgTypes>
  void EmitAll(ArgTypes... args) {
    (EmitImpl(args), ...);
  }

  /**
   * Emits jump code
   * @param label label to jump
   */
  void EmitJump(BytecodeLabel *label);

 private:
  std::vector<u8> *bytecode_;
};

}  // namespace terrier::execution::vm
