#pragma once

#include <cstdint>
#include <vector>

#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecodes.h"

namespace noisepage::execution::vm {

class BytecodeLabel;

/**
 * Defines functions that allow bytecode emission.
 */
class BytecodeEmitter {
 public:
  /**
   * Construct a bytecode emitter instance that encodes and writes bytecode instructions into the
   * provided bytecode vector.
   * @param bytecode The bytecode array to emit bytecode into.
   */
  explicit BytecodeEmitter(std::vector<uint8_t> *bytecode) : bytecode_(bytecode) {
    NOISEPAGE_ASSERT(bytecode_ != nullptr, "NULL bytecode pointer provided to emitter");
  }

  /**
   * This class cannot be copied or moved.
   */
  DISALLOW_COPY_AND_MOVE(BytecodeEmitter);

  /**
   * @return The current position of the emitter in the bytecode stream.
   */
  std::size_t GetPosition() const { return bytecode_->size(); }

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
  void EmitDerefN(LocalVar dest, LocalVar src, uint32_t len);

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
  void EmitAssignImm1(LocalVar dest, int8_t val);

  /**
   * Emit assignment code for 2 byte values.
   * @param dest destination variable
   * @param val value to assign
   */
  void EmitAssignImm2(LocalVar dest, int16_t val);

  /**
   * Emit assignment code for 4 byte values.
   * @param dest destination variable
   * @param val value to assign
   */
  void EmitAssignImm4(LocalVar dest, int32_t val);

  /**
   * Emit assignment code for 8 byte values.
   * @param dest destination variable
   * @param val value to assign
   */
  void EmitAssignImm8(LocalVar dest, int64_t val);

  /**
   * Emit assignment code for 4 byte float values.
   * @param dest destination variable
   * @param val value to assign
   */
  void EmitAssignImm4F(LocalVar dest, float val);

  /**
   * Emit assignment code for 8 byte float values.
   * @param dest destination variable
   * @param val value to assign
   */
  void EmitAssignImm8F(LocalVar dest, double val);

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
  void EmitLea(LocalVar dest, LocalVar src, uint32_t offset);

  /**
   * Emits scaled Lea code.
   * The loaded address is found using src + (scale * index) + offset
   * @param dest destination variable
   * @param src source variable
   * @param index index of the element to access
   * @param scale element size
   * @param offset additional offset of the element to load
   */
  void EmitLeaScaled(LocalVar dest, LocalVar src, LocalVar index, uint32_t scale, uint32_t offset);

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
   * Emit arbitrary bytecode with two operands
   * @param bytecode bytecode to emit
   * @param operand_1 first operand
   * @param operand_2 second operand
   */
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2);

  /**
   * Emit arbitrary bytecode with three operands
   * @param bytecode bytecode to emit
   * @param operand_1 first operand
   * @param operand_2 second operand
   * @param operand_3 third operand
   */
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3);

  /**
   * Emit arbitrary bytecode with four operands
   * @param bytecode bytecode to emit
   * @param operand_1 first operand
   * @param operand_2 second operand
   * @param operand_3 third operand
   * @param operand_4 fourth operand
   */
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3, LocalVar operand_4);

  /**
   * Emit arbitrary bytecode with five operands
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
   * Emit arbitrary bytecode with six operands
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
   * Emit arbitrary bytecode with eight operands
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

  /**
   * Emit arbitrary bytecode with nine operands
   * @param bytecode bytecode to emit
   * @param operand_1 first operand
   * @param operand_2 second operand
   * @param operand_3 third operand
   * @param operand_4 fourth operand
   * @param operand_5 fifth operand
   * @param operand_6 sixth operand
   * @param operand_7 seventh operand
   * @param operand_8 eighth operand
   * @param operand_9 ninth operand
   */
  void Emit(Bytecode bytecode, LocalVar operand_1, LocalVar operand_2, LocalVar operand_3, LocalVar operand_4,
            LocalVar operand_5, LocalVar operand_6, LocalVar operand_7, LocalVar operand_8, LocalVar operand_9);

  // -------------------------------------------------------
  // Special
  // -------------------------------------------------------

  /** Initialize a SQL string from a raw string. */
  void EmitInitString(LocalVar dest, LocalVar static_local_string, uint32_t string_len);

  /** Iterate over all the states in the container. */
  void EmitThreadStateContainerIterate(LocalVar tls, LocalVar ctx, FunctionId iterate_fn);

  /** Reset a thread state container with init and destroy functions. */
  void EmitThreadStateContainerReset(LocalVar tls, LocalVar state_size, FunctionId init_fn, FunctionId destroy_fn,
                                     LocalVar ctx);

  /** Initialize a table iterator. */
  void EmitTableIterInit(Bytecode bytecode, LocalVar iter, LocalVar exec_ctx, LocalVar table_oid, LocalVar col_oids,
                         uint32_t num_oids);

  /** Emit a parallel table scan. */
  void EmitParallelTableScan(LocalVar table_oid, LocalVar col_oids, uint32_t num_oids, LocalVar query_state,
                             LocalVar exec_ctx, FunctionId scan_fn);

  /** Emit a register hook function. */
  void EmitRegisterHook(LocalVar exec_ctx, LocalVar hook_idx, FunctionId hook_fn);

  /** Reading values from an iterator. */
  void EmitVPIGet(Bytecode bytecode, LocalVar out, LocalVar vpi, uint32_t col_idx);

  /** Setting values in an iterator. */
  void EmitVPISet(Bytecode bytecode, LocalVar vpi, LocalVar input, uint32_t col_idx);

  /** Insert a filter flavor into the filter manager builder. */
  void EmitFilterManagerInsertFilter(LocalVar filter_manager, FunctionId func);

  /** Lookup a single entry in the aggregation hash table. */
  void EmitAggHashTableLookup(LocalVar dest, LocalVar agg_ht, LocalVar hash, FunctionId key_eq_fn, LocalVar arg);

  /** Emit code to process a batch of input into the aggregation hash table. */
  void EmitAggHashTableProcessBatch(LocalVar agg_ht, LocalVar vpi, uint32_t num_keys, LocalVar key_cols,
                                    FunctionId init_agg_fn, FunctionId merge_agg_fn, LocalVar partitioned);

  /** Emit code to move thread-local data into main agg table. */
  void EmitAggHashTableMovePartitions(LocalVar agg_ht, LocalVar tls, LocalVar aht_offset, FunctionId merge_part_fn);

  /** Emit code to scan an agg table in parallel. */
  void EmitAggHashTableParallelPartitionedScan(LocalVar agg_ht, LocalVar context, LocalVar tls,
                                               FunctionId scan_part_fn);

  /** Initialize a sorter instance. */
  void EmitSorterInit(Bytecode bytecode, LocalVar sorter, LocalVar exec_ctx, FunctionId cmp_fn, LocalVar tuple_size);

  /** Initialize a CSV reader. */
  // void EmitCSVReaderInit(LocalVar creader, LocalVar file_name, uint32_t file_name_len);

  /** ONLY FOR TESTING! */
  void EmitTestCatalogLookup(LocalVar oid_var, LocalVar exec_ctx, LocalVar table_name, uint32_t table_name_len,
                             LocalVar col_name, uint32_t col_name_len);

  /** ONLY FOR TESTING! */
  void EmitTestCatalogIndexLookup(LocalVar oid_var, LocalVar exec_ctx, LocalVar table_name, uint32_t table_name_len);

  /**
   * Emit code to initialize an index iterator
   * @param bytecode index initialization bytecode
   * @param iter iterator in initialize
   * @param exec_ctx the execution context
   * @param num_attrs number attributes of key set
   * @param table_oid oid of the table owning the index
   * @param index_oid oid of the index to use
   * @param col_oids array of oids
   * @param num_oids length of the array
   */
  void EmitIndexIteratorInit(Bytecode bytecode, LocalVar iter, LocalVar exec_ctx, uint32_t num_attrs,
                             LocalVar table_oid, LocalVar index_oid, LocalVar col_oids, uint32_t num_oids);

  /**
   * Emit bytecode to set value within a PR
   */
  void EmitPRSet(Bytecode bytecode, LocalVar pr, uint16_t col_idx, LocalVar val);

  /**
   * Emit bytecode to set a varlen within a PR
   */
  void EmitPRSetVarlen(Bytecode bytecode, LocalVar pr, uint16_t col_idx, LocalVar val, LocalVar own);

  /**
   * Emit bytecode to get a value from a PR
   */
  void EmitPRGet(Bytecode bytecode, LocalVar out, LocalVar pr, uint16_t col_idx);

  /**
   * Emit bytecode to init an Storage Interface
   */
  void EmitStorageInterfaceInit(Bytecode bytecode, LocalVar storage_interface, LocalVar exec_ctx, LocalVar table_oid,
                                LocalVar col_oids, uint32_t num_oids, LocalVar need_indexes);

  /**
   * Emit bytecode to get an index PR for the storage_interface.
   */
  void EmitStorageInterfaceGetIndexPR(Bytecode bytecode, LocalVar pr, LocalVar storage_interface, LocalVar index_oid);

  /**
   * Emits bytecode to abort the current transaction
   */
  void EmitAbortTxn(Bytecode bytecode, LocalVar exec_ctx);

  /**
   * @brief Emits a concat instruction
   * @param ret where to store the result of concat instruction
   * @param exec_ctx execution context
   * @param inputs string inputs to concat
   * @param num_inputs length of inputs
   */
  void EmitConcat(LocalVar ret, LocalVar exec_ctx, LocalVar inputs, uint32_t num_inputs);

 private:
  /** Copy a scalar immediate value into the bytecode stream */
  template <typename T>
  auto EmitScalarValue(const T val) -> std::enable_if_t<std::is_arithmetic_v<T>> {
    bytecode_->insert(bytecode_->end(), sizeof(T), 0);
    *reinterpret_cast<T *>(&*(bytecode_->end() - sizeof(T))) = val;
  }

  /** Emit a bytecode */
  void EmitImpl(const Bytecode bytecode) { EmitScalarValue(Bytecodes::ToByte(bytecode)); }

  /** Emit a local variable reference by encoding it into the bytecode stream */
  void EmitImpl(const LocalVar local) { EmitScalarValue(local.Encode()); }

  /** Emit an integer immediate value */
  template <typename T>
  auto EmitImpl(const T val) -> std::enable_if_t<std::is_arithmetic_v<T>> {
    EmitScalarValue(val);
  }

  /** Emit all arguments in sequence */
  template <typename... ArgTypes>
  void EmitAll(const ArgTypes... args) {
    (EmitImpl(args), ...);
  }

  /** Emit a jump instruction to the given label */
  void EmitJump(BytecodeLabel *label);

 private:
  std::vector<uint8_t> *bytecode_;
};

}  // namespace noisepage::execution::vm
