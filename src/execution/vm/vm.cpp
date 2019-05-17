#include "execution/vm/vm.h"

#include <numeric>
#include <string>
#include <vector>

#include <iostream>
#include "execution/exec/execution_context.h"
#include "execution/sql/value.h"
#include "type/type_id.h"
#include "execution/util/common.h"
#include "execution/util/timer.h"
#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecode_handlers.h"
#include "execution/vm/bytecode_module.h"

namespace tpl::vm {

// ---------------------------------------------------------
// Virtual Machine Frame
// ---------------------------------------------------------

/// An execution frame where all function's local variables and parameters live
/// for the duration of the function's lifetime.
class VM::Frame {
  friend class VM;

 public:
  /// Constructor
  Frame(u8 *frame_data, std::size_t frame_size)
      : frame_data_(frame_data), frame_size_(frame_size) {
    TPL_ASSERT(frame_data_ != nullptr, "Frame data cannot be null");
    TPL_ASSERT(frame_size_ >= 0, "Frame size must be >= 0");
  }

  /// Access the local variable at the given index in the fame. The \ref 'index'
  /// attribute is encoded and indicates whether the local variable is accessed
  /// through an indirection (i.e., if the variable has to be dereferenced or
  /// loaded)
  /// \tparam T The type of the variable the user expects
  /// \param index The encoded index into the frame where the variable is
  /// \return The value of the variable. Note that this is copied!
  template <typename T>
  T LocalAt(u32 index) const {
    LocalVar local = LocalVar::Decode(index);

    EnsureInFrame(local);

    auto val = reinterpret_cast<uintptr_t>(&frame_data_[local.GetOffset()]);

    if (local.GetAddressMode() == LocalVar::AddressMode::Value) {
      return *reinterpret_cast<T *>(val);
    }

    return (T)(val);  // NOLINT (both static/reinterpret cast semantics)
  }

 private:
#ifndef NDEBUG
  // Ensure the local variable is valid
  void EnsureInFrame(LocalVar var) const {
    if (var.GetOffset() >= frame_size_) {
      std::string error_msg =
          fmt::format("Accessing local at offset {}, beyond frame of size {}",
                      var.GetOffset(), frame_size_);
      EXECUTION_LOG_ERROR("{}", error_msg);
      throw std::runtime_error(error_msg);
    }
  }
#else
  void EnsureInFrame(UNUSED LocalVar var) const {}
#endif

 private:
  u8 *frame_data_;
  std::size_t frame_size_;
};

// ---------------------------------------------------------
// Virtual Machine
// ---------------------------------------------------------

// The maximum amount of stack to use. If the function requires more than 16K
// bytes, acquire space from the heap.
static constexpr const u32 kMaxStackAllocSize = 1ull << 14ull;
// A soft-maximum amount of stack to use. If a function's frame requires more
// than 4K (the soft max), try the stack and fallback to heap. If the function
// requires less, use the stack.
static constexpr const u32 kSoftMaxStackAllocSize = 1ull << 12ull;

VM::VM(const BytecodeModule &module) : module_(module) {}

// static
void VM::InvokeFunction(const BytecodeModule &module, const FunctionId func_id,
                        const u8 args[]) {
  // The function's info
  const FunctionInfo *const func_info = module.GetFuncInfoById(func_id);
  TPL_ASSERT(func_info != nullptr, "Function doesn't exist in module!");
  const std::size_t frame_size = func_info->frame_size();

  // Let's try to get some space
  bool used_heap = false;
  u8 *raw_frame = nullptr;
  if (frame_size > kMaxStackAllocSize) {
    used_heap = true;
    posix_memalign(reinterpret_cast<void **>(&raw_frame), alignof(u64), frame_size);
  } else if (frame_size > kSoftMaxStackAllocSize) {
    // TODO(pmenon): Check stack before allocation
    raw_frame = static_cast<u8 *>(alloca(frame_size));
  } else {
    raw_frame = static_cast<u8 *>(alloca(frame_size));
  }

  // Copy args into frame
  std::memcpy(raw_frame + func_info->params_start_pos(), args,
              func_info->params_size());

  // Let's go. First, create the virtual machine instance.
  VM vm(module);

  // Now get the bytecode for the function and fire it off
  const u8 *const bytecode = module.GetBytecodeForFunction(*func_info);
  TPL_ASSERT(bytecode != nullptr, "Bytecode cannot be null");
  Frame frame(raw_frame, frame_size);
  vm.Interpret(bytecode, &frame);

  // Cleanup
  if (used_heap) {
    std::free(raw_frame);
  }
}

namespace {

template <typename T>
inline ALWAYS_INLINE T Read(const u8 **ip) {
  static_assert(std::is_integral_v<T>,
                "Read() should only be used to read primitive integer types "
                "directly from the bytecode instruction stream");
  auto ret = *reinterpret_cast<const T *>(*ip);
  (*ip) += sizeof(T);
  return ret;
}

template <typename T>
inline ALWAYS_INLINE T Peek(const u8 **ip) {
  static_assert(std::is_integral_v<T>,
                "Peek() should only be used to read primitive integer types "
                "directly from the bytecode instruction stream");
  return *reinterpret_cast<const T *>(*ip);
}

}  // namespace

// NOLINTNEXTLINE(google-readability-function-size,readability-function-size)
void VM::Interpret(const u8 *ip, Frame *frame) {
  static void *kDispatchTable[] = {
#define ENTRY(name, ...) &&op_##name,
      BYTECODE_LIST(ENTRY)
#undef ENTRY
  };

#ifdef TPL_DEBUG_TRACE_INSTRUCTIONS
#define DEBUG_TRACE_INSTRUCTIONS(op)                                                  \
  do {                                                                                \
    auto bytecode = Bytecodes::FromByte(op);                                          \
    bytecode_counts_[op]++;                                                           \
    EXECUTION_LOG_INFO("{0:p}: {1:s}", ip - sizeof(std::underlying_type_t<Bytecode>), \
             Bytecodes::ToString(bytecode));                                          \
  } while (false)
#else
#define DEBUG_TRACE_INSTRUCTIONS(op) (void)op
#endif

  // TODO(pmenon): Should these READ/PEEK macros take in a vm::OperandType so
  // that we can infer primitive types using traits? This minimizes number of
  // changes if the underlying offset/bytecode/register sizes changes?
#define PEEK_JMP_OFFSET() Peek<i32>(&ip)
#define READ_IMM1() Read<i8>(&ip)
#define READ_IMM2() Read<i16>(&ip)
#define READ_IMM4() Read<i32>(&ip)
#define READ_IMM8() Read<i64>(&ip)
#define READ_UIMM2() Read<u16>(&ip)
#define READ_UIMM4() Read<u32>(&ip)
#define READ_JMP_OFFSET() READ_IMM4()
#define READ_LOCAL_ID() Read<u32>(&ip)
#define READ_OP() Read<std::underlying_type_t<Bytecode>>(&ip)

#define OP(name) op_##name
#define DISPATCH_NEXT()           \
  do {                            \
    auto op = READ_OP();          \
    DEBUG_TRACE_INSTRUCTIONS(op); \
    goto *kDispatchTable[op];     \
  } while (false)

  /*****************************************************************************
   *
   * Below this comment begins the primary section of TPL's register-based
   * virtual machine (VM) dispatch area. The VM uses indirect threaded
   * interpretation; each bytecode handler's label is statically generated and
   * stored in @ref kDispatchTable at server compile time. Bytecode handler
   * logic is written as a case using the CASE_OP macro. Handlers can read from
   * and write to registers using the local execution frame's register file
   * (i.e., through @ref Frame::LocalAt()).
   *
   * Upon entry, the instruction pointer (IP) points to the first bytecode of
   * function that is running. The READ_* macros can be used to directly read
   * values from the bytecode stream. The READ_* macros read values from the
   * bytecode stream and advance the IP whereas the PEEK_* macros do only the
   * former, leaving the IP unmodified.
   *
   * IMPORTANT:
   * ----------
   * Bytecode handler code here should only be simple register/IP manipulation
   * (i.e., reading from and writing to registers). Actual full-blown bytecode
   * logic must be implemented externally and invoked from stubs here. This is a
   * strict requirement necessary because it makes code generation to LLVM much
   * simpler.
   *
   ****************************************************************************/

  // Jump to the first instruction
  DISPATCH_NEXT();

  // -------------------------------------------------------
  // Primitive comparison operations
  // -------------------------------------------------------

#define DO_GEN_COMPARISON(op, type)                       \
  OP(op##_##type) : {                                     \
    auto *dest = frame->LocalAt<bool *>(READ_LOCAL_ID()); \
    auto lhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    auto rhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    Op##op##_##type(dest, lhs, rhs);                      \
    DISPATCH_NEXT();                                      \
  }
#define GEN_COMPARISON_TYPES(type, ...)     \
  DO_GEN_COMPARISON(GreaterThan, type)      \
  DO_GEN_COMPARISON(GreaterThanEqual, type) \
  DO_GEN_COMPARISON(Equal, type)            \
  DO_GEN_COMPARISON(LessThan, type)         \
  DO_GEN_COMPARISON(LessThanEqual, type)    \
  DO_GEN_COMPARISON(NotEqual, type)

  INT_TYPES(GEN_COMPARISON_TYPES)
#undef GEN_COMPARISON_TYPES
#undef DO_GEN_COMPARISON

  // -------------------------------------------------------
  // Primitive arithmetic and binary operations
  // -------------------------------------------------------

#define DO_GEN_ARITHMETIC_OP(op, test, type)              \
  OP(op##_##type) : {                                     \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto lhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    auto rhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    if ((test) && rhs == 0u) {                            \
      /* TODO(pmenon): Proper error */                    \
      EXECUTION_LOG_ERROR("Division by zero error!");     \
    }                                                     \
    Op##op##_##type(dest, lhs, rhs);                      \
    DISPATCH_NEXT();                                      \
  }
#define GEN_ARITHMETIC_OP(type, ...)        \
  DO_GEN_ARITHMETIC_OP(Add, false, type)    \
  DO_GEN_ARITHMETIC_OP(Sub, false, type)    \
  DO_GEN_ARITHMETIC_OP(Mul, false, type)    \
  DO_GEN_ARITHMETIC_OP(Div, true, type)     \
  DO_GEN_ARITHMETIC_OP(Rem, true, type)     \
  DO_GEN_ARITHMETIC_OP(BitAnd, false, type) \
  DO_GEN_ARITHMETIC_OP(BitOr, false, type)  \
  DO_GEN_ARITHMETIC_OP(BitXor, false, type)

  INT_TYPES(GEN_ARITHMETIC_OP)
#undef GEN_ARITHMETIC_OP
#undef DO_GEN_ARITHMETIC_OP

  // -------------------------------------------------------
  // Bitwise and integer negation
  // -------------------------------------------------------

#define GEN_NEG_OP(type, ...)                             \
  OP(Neg##_##type) : {                                    \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto input = frame->LocalAt<type>(READ_LOCAL_ID());   \
    OpNeg##_##type(dest, input);                          \
    DISPATCH_NEXT();                                      \
  }                                                       \
  OP(BitNeg##_##type) : {                                 \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto input = frame->LocalAt<type>(READ_LOCAL_ID());   \
    OpBitNeg##_##type(dest, input);                       \
    DISPATCH_NEXT();                                      \
  }

  INT_TYPES(GEN_NEG_OP)
#undef GEN_NEG_OP

  // -------------------------------------------------------
  // Jumps
  // -------------------------------------------------------

  OP(Jump) : {
    auto skip = PEEK_JMP_OFFSET();
    if (TPL_LIKELY(OpJump())) {
      ip += skip;
    }
    DISPATCH_NEXT();
  }

  OP(JumpIfTrue) : {
    auto cond = frame->LocalAt<bool>(READ_LOCAL_ID());
    auto skip = PEEK_JMP_OFFSET();
    if (OpJumpIfTrue(cond)) {
      ip += skip;
    } else {
      READ_JMP_OFFSET();
    }
    DISPATCH_NEXT();
  }

  OP(JumpIfFalse) : {
    auto cond = frame->LocalAt<bool>(READ_LOCAL_ID());
    auto skip = PEEK_JMP_OFFSET();
    if (OpJumpIfFalse(cond)) {
      ip += skip;
    } else {
      READ_JMP_OFFSET();
    }
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Low-level memory operations
  // -------------------------------------------------------

#define GEN_DEREF(type, size)                             \
  OP(Deref##size) : {                                     \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto *src = frame->LocalAt<type *>(READ_LOCAL_ID());  \
    OpDeref##size(dest, src);                             \
    DISPATCH_NEXT();                                      \
  }
  GEN_DEREF(i8, 1);
  GEN_DEREF(i16, 2);
  GEN_DEREF(i32, 4);
  GEN_DEREF(i64, 8);
#undef GEN_DEREF

  OP(DerefN) : {
    auto *dest = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto *src = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto len = READ_UIMM4();
    OpDerefN(dest, src, len);
    DISPATCH_NEXT();
  }

#define GEN_ASSIGN(type, size)                            \
  OP(Assign##size) : {                                    \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto src = frame->LocalAt<type>(READ_LOCAL_ID());     \
    OpAssign##size(dest, src);                            \
    DISPATCH_NEXT();                                      \
  }                                                       \
  OP(AssignImm##size) : {                                 \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    OpAssignImm##size(dest, READ_IMM##size());            \
    DISPATCH_NEXT();                                      \
  }
  GEN_ASSIGN(i8, 1);
  GEN_ASSIGN(i16, 2);
  GEN_ASSIGN(i32, 4);
  GEN_ASSIGN(i64, 8);
#undef GEN_ASSIGN

  OP(Lea) : {
    auto **dest = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *src = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto offset = READ_UIMM4();
    OpLea(dest, src, offset);
    DISPATCH_NEXT();
  }

  OP(LeaScaled) : {
    auto **dest = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *src = frame->LocalAt<byte *>(READ_LOCAL_ID());
    auto index = frame->LocalAt<u32>(READ_LOCAL_ID());
    auto scale = READ_UIMM4();
    auto offset = READ_UIMM4();
    OpLeaScaled(dest, src, index, scale, offset);
    DISPATCH_NEXT();
  }

  OP(RegionInit) : {
    auto *region = frame->LocalAt<util::Region *>(READ_LOCAL_ID());
    OpRegionInit(region);
    DISPATCH_NEXT();
  }

  OP(RegionFree) : {
    auto *region = frame->LocalAt<util::Region *>(READ_LOCAL_ID());
    OpRegionFree(region);
    DISPATCH_NEXT();
  }

  OP(Call) : {
    ip = ExecuteCall(ip, frame);
    DISPATCH_NEXT();
  }

  OP(Return) : {
    OpReturn();
    return;
  }

  // -------------------------------------------------------
  // Transaction ops
  // -------------------------------------------------------
  OP(BeginTransaction) : {
    auto *txn = frame->LocalAt<terrier::transaction::TransactionContext **>(
        READ_LOCAL_ID());
    OpBeginTransaction(txn);
    DISPATCH_NEXT();
  }

  OP(CommitTransaction) : {
    auto *txn = frame->LocalAt<terrier::transaction::TransactionContext **>(
        READ_LOCAL_ID());
    OpCommitTransaction(txn);
    DISPATCH_NEXT();
  }

  OP(AbortTransaction) : {
    auto *txn = frame->LocalAt<terrier::transaction::TransactionContext **>(
        READ_LOCAL_ID());
    OpAbortTransaction(txn);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Table Vector and ProjectedColumns Iterator (PCI) ops
  // -------------------------------------------------------

  OP(TableVectorIteratorInit) : {
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    auto db_oid = READ_UIMM4();
    auto table_oid = READ_UIMM4();
    auto exec_context = static_cast<uintptr_t>(READ_IMM8());
    OpTableVectorIteratorInit(iter, db_oid, table_oid, exec_context);
    DISPATCH_NEXT();
  }

  OP(TableVectorIteratorPerformInit) : {
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorPerformInit(iter);
    DISPATCH_NEXT();
  }

  OP(TableVectorIteratorNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(TableVectorIteratorFree) : {
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorFree(iter);
    DISPATCH_NEXT();
  }

  OP(TableVectorIteratorGetPCI) : {
    auto *pci =
        frame->LocalAt<sql::ProjectedColumnsIterator **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorGetPCI(pci, iter);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // PCI iteration operations
  // -------------------------------------------------------

  OP(PCIHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter =
        frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID());
    OpPCIHasNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(PCIAdvance) : {
    auto *iter =
        frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID());
    OpPCIAdvance(iter);
    DISPATCH_NEXT();
  }

  OP(PCIReset) : {
    auto *iter =
        frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID());
    OpPCIReset(iter);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // PCI element access
  // -------------------------------------------------------

#define GEN_PCI_ACCESS(type_str, type)                                    \
  OP(PCIGet##type_str) : {                                                \
    auto *result = frame->LocalAt<type *>(READ_LOCAL_ID());               \
    auto *pci =                                                           \
        frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                          \
    OpPCIGet##type_str(result, pci, col_idx);                             \
    DISPATCH_NEXT();                                                      \
  }                                                                       \
  OP(PCIGet##type_str##Null) : {                                          \
    auto *result = frame->LocalAt<type *>(READ_LOCAL_ID());               \
    auto *pci =                                                           \
        frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                          \
    OpPCIGet##type_str##Null(result, pci, col_idx);                       \
    DISPATCH_NEXT();                                                      \
  }
  GEN_PCI_ACCESS(SmallInt, sql::Integer)
  GEN_PCI_ACCESS(Integer, sql::Integer)
  GEN_PCI_ACCESS(BigInt, sql::Integer)
  GEN_PCI_ACCESS(Decimal, sql::Decimal)
#undef GEN_PCI_ACCESS

#define GEN_PCI_FILTER(Op)                                                \
  OP(PCIFilter##Op) : {                                                   \
    auto *size = frame->LocalAt<u32 *>(READ_LOCAL_ID());                  \
    auto *iter =                                                          \
        frame->LocalAt<sql::ProjectedColumnsIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                          \
    auto type = READ_IMM1();                                              \
    auto val = READ_IMM8();                                               \
    OpPCIFilter##Op(size, iter, col_idx, type, val);                      \
    DISPATCH_NEXT();                                                      \
  }
  GEN_PCI_FILTER(Equal)
  GEN_PCI_FILTER(GreaterThan)
  GEN_PCI_FILTER(GreaterThanEqual)
  GEN_PCI_FILTER(LessThan)
  GEN_PCI_FILTER(LessThanEqual)
  GEN_PCI_FILTER(NotEqual)
#undef GEN_PCI_FILTER

  // -------------------------------------------------------
  // SQL Integer Comparison Operations
  // -------------------------------------------------------

  OP(ForceBoolTruth) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *sql_int = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());
    OpForceBoolTruth(result, sql_int);
    DISPATCH_NEXT();
  }

  OP(InitBool) : {
    auto *sql_bool = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());
    auto val = frame->LocalAt<bool>(READ_LOCAL_ID());
    OpInitBool(sql_bool, val);
    DISPATCH_NEXT();
  }

  OP(InitInteger) : {
    auto *sql_int = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto val = frame->LocalAt<i32>(READ_LOCAL_ID());
    OpInitInteger(sql_int, val);
    DISPATCH_NEXT();
  }

  OP(InitReal) : {
    auto *sql_real = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto val = frame->LocalAt<double>(READ_LOCAL_ID());
    OpInitReal(sql_real, val);
    DISPATCH_NEXT();
  }

#define GEN_CMP(op)                                                 \
  OP(op##Integer) : {                                               \
    auto *result = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID()); \
    auto *left = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());   \
    auto *right = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());  \
    Op##op##Integer(result, left, right);                           \
    DISPATCH_NEXT();                                                \
  }
  GEN_CMP(GreaterThan);
  GEN_CMP(GreaterThanEqual);
  GEN_CMP(Equal);
  GEN_CMP(LessThan);
  GEN_CMP(LessThanEqual);
  GEN_CMP(NotEqual);
#undef GEN_CMP

  // -------------------------------------------------------
  // Aggregations
  // -------------------------------------------------------

  OP(CountAggregateInit) : {
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(CountAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Val *>(READ_LOCAL_ID());
    OpCountAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(CountAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(CountAggregateReset) : {
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(CountAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(CountAggregateFree) : {
    auto *agg = frame->LocalAt<sql::CountAggregate *>(READ_LOCAL_ID());
    OpCountAggregateFree(agg);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateInit) : {
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Val *>(READ_LOCAL_ID());
    OpCountStarAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateReset) : {
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(CountStarAggregateFree) : {
    auto *agg = frame->LocalAt<sql::CountStarAggregate *>(READ_LOCAL_ID());
    OpCountStarAggregateFree(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateInit) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerSumAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateAdvanceNullable) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerSumAggregateAdvanceNullable(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateReset) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(IntegerSumAggregateFree) : {
    auto *agg = frame->LocalAt<sql::IntegerSumAggregate *>(READ_LOCAL_ID());
    OpIntegerSumAggregateFree(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateInit) : {
    auto *agg = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateAdvanceNullable) : {
    auto *agg = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateAdvanceNullable(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateReset) : {
    auto *agg = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMaxAggregateFree) : {
    auto *agg = frame->LocalAt<sql::IntegerMaxAggregate *>(READ_LOCAL_ID());
    OpIntegerMaxAggregateFree(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateInit) : {
    auto *agg = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    OpIntegerMinAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerMinAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateAdvanceNullable) : {
    auto *agg = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerMinAggregateAdvanceNullable(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    OpIntegerMinAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateReset) : {
    auto *agg = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    OpIntegerMinAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    OpIntegerMinAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(IntegerMinAggregateFree) : {
    auto *agg = frame->LocalAt<sql::IntegerMinAggregate *>(READ_LOCAL_ID());
    OpIntegerMinAggregateFree(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateInit) : {
    auto *agg = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateAdvance) : {
    auto *agg = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateAdvance(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateAdvanceNullable) : {
    auto *agg = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateAdvanceNullable(agg, val);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateReset) : {
    auto *agg = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(IntegerAvgAggregateFree) : {
    auto *agg = frame->LocalAt<sql::IntegerAvgAggregate *>(READ_LOCAL_ID());
    OpIntegerAvgAggregateFree(agg);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Hash Joins
  // -------------------------------------------------------

  OP(JoinHashTableInit) : {
    auto *join_hash_table =
        frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    auto *region = frame->LocalAt<util::Region *>(READ_LOCAL_ID());
    auto tuple_size = frame->LocalAt<u32>(READ_LOCAL_ID());
    OpJoinHashTableInit(join_hash_table, region, tuple_size);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableAllocTuple) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *join_hash_table =
        frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    auto hash = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    OpJoinHashTableAllocTuple(result, join_hash_table, hash);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableBuild) : {
    auto *join_hash_table =
        frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    OpJoinHashTableBuild(join_hash_table);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableFree) : {
    auto *join_hash_table =
        frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    OpJoinHashTableFree(join_hash_table);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Sorting
  // -------------------------------------------------------

  OP(SorterInit) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto *region = frame->LocalAt<util::Region *>(READ_LOCAL_ID());
    auto cmp_func_id = frame->LocalAt<FunctionId>(READ_LOCAL_ID());
    auto tuple_size = frame->LocalAt<u32>(READ_LOCAL_ID());

    auto cmp_fn = reinterpret_cast<sql::Sorter::ComparisonFunction>(
        module().GetFuncTrampoline(cmp_func_id));
    OpSorterInit(sorter, region, cmp_fn, tuple_size);
    DISPATCH_NEXT();
  }

  OP(SorterAllocTuple) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterAllocTuple(result, sorter);
    DISPATCH_NEXT();
  }

  OP(SorterAllocTupleTopK) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto top_k = READ_IMM8();
    OpSorterAllocTupleTopK(result, sorter, top_k);
    DISPATCH_NEXT();
  }

  OP(SorterAllocTupleTopKFinish) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto top_k = READ_IMM8();
    OpSorterAllocTupleTopKFinish(sorter, top_k);
    DISPATCH_NEXT();
  }

  OP(SorterSort) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterSort(sorter);
    DISPATCH_NEXT();
  }

  OP(SorterFree) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterFree(sorter);
    DISPATCH_NEXT();
  }

  OP(SorterIteratorInit) : {
    auto *iter = frame->LocalAt<sql::SorterIterator *>(READ_LOCAL_ID());
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterIteratorInit(iter, sorter);
    DISPATCH_NEXT();
  }

  OP(SorterIteratorAdvance) : {
    auto *iter = frame->LocalAt<sql::SorterIterator *>(READ_LOCAL_ID());
    OpSorterIteratorAdvance(iter);
    DISPATCH_NEXT();
  }

  OP(SorterIteratorGetRow) : {
    const auto **row = frame->LocalAt<const byte **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::SorterIterator *>(READ_LOCAL_ID());
    OpSorterIteratorGetRow(row, iter);
    DISPATCH_NEXT();
  }

  OP(SorterIteratorFree) : {
    auto *iter = frame->LocalAt<sql::SorterIterator *>(READ_LOCAL_ID());
    OpSorterIteratorFree(iter);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Output Calls
  // -------------------------------------------------------
  OP(OutputAlloc) : {
    auto exec_context = static_cast<uintptr_t>(READ_IMM8());
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    OpOutputAlloc(exec_context, result);
    DISPATCH_NEXT();
  }

  OP(OutputAdvance) : {
    auto exec_context = static_cast<uintptr_t>(READ_IMM8());
    OpOutputAdvance(exec_context);
    DISPATCH_NEXT();
  }

  OP(OutputSetNull) : {
    auto exec_context = static_cast<uintptr_t>(READ_IMM8());
    auto idx = frame->LocalAt<u32>(READ_LOCAL_ID());
    OpOutputSetNull(exec_context, idx);
    DISPATCH_NEXT();
  }

  OP(OutputFinalize) : {
    auto exec_context = static_cast<uintptr_t>(READ_IMM8());
    OpOutputFinalize(exec_context);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Inserts
  // -------------------------------------------------------
  OP(Insert) : {
    auto db_id = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto table_id = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto values = frame->LocalAt<byte*>(READ_LOCAL_ID());
    auto exec_context = static_cast<uintptr_t>(READ_IMM8());
    OpInsert(exec_context, db_id, table_id, values);
    std::cout << "INSERTING";
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Index Iterator
  // -------------------------------------------------------
  OP(IndexIteratorInit) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    auto index_oid = READ_UIMM4();
    auto exec_context = static_cast<uintptr_t>(READ_IMM8());
    OpIndexIteratorInit(iter, index_oid, exec_context);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorScanKey) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    auto *key = frame->LocalAt<byte *>(READ_LOCAL_ID());
    OpIndexIteratorScanKey(iter, key);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorFree) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorFree(iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorHasNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorAdvance) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorAdvance(iter);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // IndexIterator element access
  // -------------------------------------------------------

#define GEN_INDEX_ITERATOR_ACCESS(type_str, type)                       \
  OP(IndexIteratorGet##type_str) : {                                    \
    auto *result = frame->LocalAt<type *>(READ_LOCAL_ID());             \
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                        \
    OpIndexIteratorGet##type_str(result, iter, col_idx);                \
    DISPATCH_NEXT();                                                    \
  }                                                                     \
  OP(IndexIteratorGet##type_str##Null) : {                                 \
    auto *result = frame->LocalAt<type *>(READ_LOCAL_ID());             \
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                        \
    OpIndexIteratorGet##type_str##Null(result, iter, col_idx);          \
    DISPATCH_NEXT();                                                    \
  }
  GEN_INDEX_ITERATOR_ACCESS(SmallInt, sql::Integer)
  GEN_INDEX_ITERATOR_ACCESS(Integer, sql::Integer)
  GEN_INDEX_ITERATOR_ACCESS(BigInt, sql::Integer)
  GEN_INDEX_ITERATOR_ACCESS(Decimal, sql::Decimal)
#undef GEN_INDEX_ITERATOR_ACCESS

  // -------------------------------------------------------
  // Real-value functions
  // -------------------------------------------------------

  // -------------------------------------------------------
  // Trig functions
  // -------------------------------------------------------

  OP(Acos) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpAcos(result, input);
    DISPATCH_NEXT();
  }

  OP(Asin) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpAsin(result, input);
    DISPATCH_NEXT();
  }

  OP(Atan) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpAtan(result, input);
    DISPATCH_NEXT();
  }

  OP(Atan2) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *arg_1 = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *arg_2 = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpAtan2(result, arg_1, arg_2);
    DISPATCH_NEXT();
  }

  OP(Cos) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpCos(result, input);
    DISPATCH_NEXT();
  }

  OP(Cot) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpCot(result, input);
    DISPATCH_NEXT();
  }

  OP(Sin) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpSin(result, input);
    DISPATCH_NEXT();
  }

  OP(Tan) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpTan(result, input);
    DISPATCH_NEXT();
  }

  // Impossible
  UNREACHABLE("Impossible to reach end of interpreter loop. Bad code!");
}

const u8 *VM::ExecuteCall(const u8 *ip, VM::Frame *caller) {
  // Read the function ID and the argument count to the function first
  const auto func_id = READ_UIMM2();
  const auto num_params = READ_UIMM2();

  // Lookup the function
  const FunctionInfo *func = module().GetFuncInfoById(func_id);
  TPL_ASSERT(func != nullptr, "Function doesn't exist in module!");
  const std::size_t frame_size = func->frame_size();

  // Get some space for the function's frame
  bool used_heap = false;
  u8 *raw_frame = nullptr;
  if (frame_size > kMaxStackAllocSize) {
    used_heap = true;
    posix_memalign(reinterpret_cast<void **>(&raw_frame), alignof(u64), frame_size);
  } else if (frame_size > kSoftMaxStackAllocSize) {
    // TODO(pmenon): Check stack before allocation
    raw_frame = static_cast<u8 *>(alloca(frame_size));
  } else {
    raw_frame = static_cast<u8 *>(alloca(frame_size));
  }

  // Set up the arguments to the function
  for (u32 i = 0; i < num_params; i++) {
    const LocalInfo &param_info = func->locals()[i];
    const void *const param = caller->LocalAt<void *>(READ_LOCAL_ID());
    std::memcpy(raw_frame + param_info.offset(), &param, param_info.size());
  }

  // Let's go
  const u8 *bytecode = module().GetBytecodeForFunction(*func);
  TPL_ASSERT(bytecode != nullptr, "Bytecode cannot be null");
  VM::Frame callee(raw_frame, func->frame_size());
  Interpret(bytecode, &callee);

  if (used_heap) {
    std::free(raw_frame);
  }

  return ip;
}

}  // namespace tpl::vm
