#include "execution/vm/vm.h"

#include <numeric>
#include <string>

#include "execution/sql/value.h"
#include "execution/util/memory.h"
#include "execution/vm/bytecode_function_info.h"
#include "execution/vm/bytecode_handlers.h"
#include "execution/vm/module.h"
#include "loggers/execution_logger.h"

namespace terrier::execution::vm {

/**
 * An execution frame where all function's local variables and parameters live
 * for the duration of the function's lifetime.
 */
class VM::Frame {
  friend class VM;

 public:
  Frame(uint8_t *frame_data, std::size_t frame_size) : frame_data_(frame_data), frame_size_(frame_size) {
    TERRIER_ASSERT(frame_data_ != nullptr, "Frame data cannot be null");
    TERRIER_ASSERT(frame_size_ >= 0, "Frame size must be >= 0");
    (void)frame_size_;
  }

  void *PtrToLocalAt(const LocalVar local) const {
    EnsureInFrame(local);
    return frame_data_ + local.GetOffset();
  }

  /**
   * Access the local variable at the given index in the fame. @em index is an encoded LocalVar that
   * contains both the byte offset of the variable to load and the access mode, i.e., whether the
   * local variable is accessed accessed by address or value.
   * @tparam T The type of the variable the user expects.
   * @param index The encoded index into the frame where the variable is.
   * @return The value of the variable. Note that this is copied!
   */
  template <typename T>
  T LocalAt(uint32_t index) const {  // NOLINT (clang tidy doesn't like const unsigned long instantiation)
    LocalVar local = LocalVar::Decode(index);
    const auto val = reinterpret_cast<uintptr_t>(PtrToLocalAt(local));
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
          fmt::format("Accessing local at offset {}, beyond frame of size {}", var.GetOffset(), frame_size_);
      EXECUTION_LOG_ERROR("{}", error_msg);
      throw std::runtime_error(error_msg);
    }
  }
#else
  void EnsureInFrame(UNUSED_ATTRIBUTE LocalVar var) const {}
#endif

 private:
  uint8_t *frame_data_;
  std::size_t frame_size_;
};

// ---------------------------------------------------------
// Virtual Machine
// ---------------------------------------------------------

// The maximum amount of stack to use. If the function requires more than 16K
// bytes, acquire space from the heap.
static constexpr const uint32_t MAX_STACK_ALLOC_SIZE = 1ull << 14ull;
// A soft-maximum amount of stack to use. If a function's frame requires more
// than 4K (the soft max), try the stack and fallback to heap. If the function
// requires less, use the stack.
static constexpr const uint32_t SOFT_MAX_STACK_ALLOC_SIZE = 1ull << 12ull;

VM::VM(const Module *module) : module_(module) {}

// static
void VM::InvokeFunction(const Module *module, const FunctionId func_id, const uint8_t args[]) {
  // The function's info
  const FunctionInfo *func_info = module->GetFuncInfoById(func_id);
  TERRIER_ASSERT(func_info != nullptr, "Function doesn't exist in module!");
  const std::size_t frame_size = func_info->GetFrameSize();

  // Let's try to get some space
  bool used_heap = false;
  uint8_t *raw_frame = nullptr;
  if (frame_size > MAX_STACK_ALLOC_SIZE) {
    used_heap = true;
    raw_frame = static_cast<uint8_t *>(util::Memory::MallocAligned(frame_size, alignof(uint64_t)));
  } else if (frame_size > SOFT_MAX_STACK_ALLOC_SIZE) {
    // TODO(pmenon): Check stack before allocation
    raw_frame = static_cast<uint8_t *>(alloca(frame_size));
  } else {
    raw_frame = static_cast<uint8_t *>(alloca(frame_size));
  }

  // Copy args into frame
  std::memcpy(raw_frame + func_info->GetParamsStartPos(), args, func_info->GetParamsSize());

  // Let's go!
  VM vm(module);
  Frame frame(raw_frame, frame_size);
  vm.Interpret(module->GetBytecodeModule()->AccessBytecodeForFunctionRaw(*func_info), &frame);

  // Done. Now, let's cleanup.
  if (used_heap) {
    std::free(raw_frame);
  }
}

namespace {

template <typename T>
inline ALWAYS_INLINE T Read(const uint8_t **ip) {
  static_assert(std::is_arithmetic_v<T>,
                "Read() should only be used to read primitive arithmetic types "
                "directly from the bytecode instruction stream");
  auto ret = *reinterpret_cast<const T *>(*ip);
  (*ip) += sizeof(T);
  return ret;
}

template <typename T>
inline ALWAYS_INLINE T Peek(const uint8_t **ip) {
  static_assert(std::is_integral_v<T>,
                "Peek() should only be used to read primitive arithmetic types "
                "directly from the bytecode instruction stream");
  return *reinterpret_cast<const T *>(*ip);
}

}  // namespace

void VM::Interpret(const uint8_t *ip, Frame *frame) {  // NOLINT
  static void *kDispatchTable[] = {
#define ENTRY(name, ...) &&op_##name,
      BYTECODE_LIST(ENTRY)
#undef ENTRY
  };

#ifdef TPL_DEBUG_TRACE_INSTRUCTIONS
#define DEBUG_TRACE_INSTRUCTIONS(op)                                                                                   \
  do {                                                                                                                 \
    auto bytecode = Bytecodes::FromByte(op);                                                                           \
    bytecode_counts_[op]++;                                                                                            \
    EXECUTION_LOG_DEBUG("{0:p}: {1:s}", ip - sizeof(std::underlying_type_t<Bytecode>), Bytecodes::ToString(bytecode)); \
  } while (false)
#else
#define DEBUG_TRACE_INSTRUCTIONS(op) (void)op
#endif

  // TODO(pmenon): Should these READ/PEEK macros take in a vm::OperandType so
  // that we can infer primitive types using traits? This minimizes number of
  // changes if the underlying offset/bytecode/register sizes changes?
#define PEEK_JMP_OFFSET() Peek<int32_t>(&ip)                  /* NOLINT */
#define READ_IMM1() Read<int8_t>(&ip)                         /* NOLINT */
#define READ_IMM2() Read<int16_t>(&ip)                        /* NOLINT */
#define READ_IMM4() Read<int32_t>(&ip)                        /* NOLINT */
#define READ_IMM8() Read<int64_t>(&ip)                        /* NOLINT */
#define READ_IMM4F() Read<float>(&ip)                         /* NOLINT */
#define READ_IMM8F() Read<double>(&ip)                        /* NOLINT */
#define READ_UIMM2() Read<uint16_t>(&ip)                      /* NOLINT */
#define READ_UIMM4() Read<uint32_t>(&ip)                      /* NOLINT */
#define READ_JMP_OFFSET() READ_IMM4()                         /* NOLINT */
#define READ_LOCAL_ID() Read<uint32_t>(&ip)                   /* NOLINT */
#define READ_STATIC_LOCAL_ID() Read<uint32_t>(&ip)            /* NOLINT */
#define READ_OP() Read<std::underlying_type_t<Bytecode>>(&ip) /* NOLINT */
#define READ_FUNC_ID() READ_UIMM2()                           /* NOLINT */

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

  ALL_TYPES(GEN_COMPARISON_TYPES)
#undef GEN_COMPARISON_TYPES
#undef DO_GEN_COMPARISON

  // -------------------------------------------------------
  // Primitive arithmetic
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
#define GEN_ARITHMETIC_OP(type, ...)     \
  DO_GEN_ARITHMETIC_OP(Add, false, type) \
  DO_GEN_ARITHMETIC_OP(Sub, false, type) \
  DO_GEN_ARITHMETIC_OP(Mul, false, type) \
  DO_GEN_ARITHMETIC_OP(Div, true, type)  \
  DO_GEN_ARITHMETIC_OP(Mod, true, type)

  ALL_NUMERIC_TYPES(GEN_ARITHMETIC_OP)
#undef GEN_ARITHMETIC_OP
#undef DO_GEN_ARITHMETIC_OP

  // -------------------------------------------------------
  // Arithmetic negation
  // -------------------------------------------------------

#define GEN_NEG_OP(type, ...)                             \
  OP(Neg##_##type) : {                                    \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto input = frame->LocalAt<type>(READ_LOCAL_ID());   \
    OpNeg##_##type(dest, input);                          \
    DISPATCH_NEXT();                                      \
  }

  ALL_NUMERIC_TYPES(GEN_NEG_OP)
#undef GEN_NEG_OP

  // -------------------------------------------------------
  // Bitwise operations
  // -------------------------------------------------------

#define DO_GEN_BIT_OP(op, type)                           \
  OP(op##_##type) : {                                     \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto lhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    auto rhs = frame->LocalAt<type>(READ_LOCAL_ID());     \
    Op##op##_##type(dest, lhs, rhs);                      \
    DISPATCH_NEXT();                                      \
  }
#define DO_GEN_NEG_OP(type, ...)                          \
  OP(BitNeg##_##type) : {                                 \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto input = frame->LocalAt<type>(READ_LOCAL_ID());   \
    OpBitNeg##_##type(dest, input);                       \
    DISPATCH_NEXT();                                      \
  }
#define GEN_BIT_OP(type, ...) \
  DO_GEN_BIT_OP(BitAnd, type) \
  DO_GEN_BIT_OP(BitOr, type)  \
  DO_GEN_BIT_OP(BitXor, type) \
  DO_GEN_NEG_OP(type)

  INT_TYPES(GEN_BIT_OP)
#undef GEN_BIT_OP
#undef GEN_NEG_OP
#undef DO_GEN_BIT_OP

  OP(Not) : {
    auto *dest = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto input = frame->LocalAt<bool>(READ_LOCAL_ID());
    OpNot(dest, input);
    DISPATCH_NEXT();
  }

  OP(NotSql) : {
    auto *dest = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());
    OpNotSql(dest, input);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Jumps
  // -------------------------------------------------------

  OP(Jump) : {
    auto skip = PEEK_JMP_OFFSET();
    if (LIKELY(OpJump())) {
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

  OP(IsNullPtr) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *input_ptr = frame->LocalAt<const void *>(READ_LOCAL_ID());
    OpIsNullPtr(result, input_ptr);
    DISPATCH_NEXT();
  }

  OP(IsNotNullPtr) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *input_ptr = frame->LocalAt<const void *>(READ_LOCAL_ID());
    OpIsNotNullPtr(result, input_ptr);
    DISPATCH_NEXT();
  }

#define GEN_DEREF(type, size)                             \
  OP(Deref##size) : {                                     \
    auto *dest = frame->LocalAt<type *>(READ_LOCAL_ID()); \
    auto *src = frame->LocalAt<type *>(READ_LOCAL_ID());  \
    OpDeref##size(dest, src);                             \
    DISPATCH_NEXT();                                      \
  }
  GEN_DEREF(int8_t, 1);
  GEN_DEREF(int16_t, 2);
  GEN_DEREF(int32_t, 4);
  GEN_DEREF(int64_t, 8);
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
  GEN_ASSIGN(int8_t, 1);
  GEN_ASSIGN(int16_t, 2);
  GEN_ASSIGN(int32_t, 4);
  GEN_ASSIGN(int64_t, 8);
#undef GEN_ASSIGN

  OP(AssignImm4F) : {
    auto *dest = frame->LocalAt<float *>(READ_LOCAL_ID());
    OpAssignImm4F(dest, READ_IMM4F());
    DISPATCH_NEXT();
  }

  OP(AssignImm8F) : {
    auto *dest = frame->LocalAt<double *>(READ_LOCAL_ID());
    OpAssignImm8F(dest, READ_IMM8F());
    DISPATCH_NEXT();
  }

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
    auto index = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto scale = READ_UIMM4();
    auto offset = READ_UIMM4();
    OpLeaScaled(dest, src, index, scale, offset);
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
  // Execution Context
  // -------------------------------------------------------

  OP(ExecutionContextAddRowsAffected) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto rows_affected = frame->LocalAt<int32_t>(READ_LOCAL_ID());

    OpExecutionContextAddRowsAffected(exec_ctx, rows_affected);
    DISPATCH_NEXT();
  }

  OP(ExecutionContextGetMemoryPool) : {
    auto *memory_pool = frame->LocalAt<sql::MemoryPool **>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpExecutionContextGetMemoryPool(memory_pool, exec_ctx);
    DISPATCH_NEXT();
  }

  OP(ExecutionContextRegisterHook) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto idx = frame->LocalAt<uint32_t>(READ_LOCAL_ID());

    auto fn_id = READ_FUNC_ID();
    auto fn = reinterpret_cast<exec::ExecutionContext::HookFn>(module_->GetRawFunctionImpl(fn_id));

    OpExecutionContextRegisterHook(exec_ctx, idx, fn);
    DISPATCH_NEXT();
  }

  OP(ExecutionContextClearHooks) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpExecutionContextClearHooks(exec_ctx);
    DISPATCH_NEXT();
  }

  OP(ExecutionContextInitHooks) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto size = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpExecutionContextInitHooks(exec_ctx, size);
    DISPATCH_NEXT();
  }

  OP(ExecutionContextGetTLS) : {
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer **>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpExecutionContextGetTLS(thread_state_container, exec_ctx);
    DISPATCH_NEXT();
  }

  OP(ExecutionContextStartResourceTracker) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto metrics_component = static_cast<metrics::MetricsComponent>(frame->LocalAt<uint64_t>(READ_LOCAL_ID()));
    OpExecutionContextStartResourceTracker(exec_ctx, metrics_component);
    DISPATCH_NEXT();
  }

  OP(ExecutionContextSetMemoryUseOverride) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto size = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpExecutionContextSetMemoryUseOverride(exec_ctx, size);
    DISPATCH_NEXT();
  }

  OP(ExecutionContextEndResourceTracker) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *name = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    OpExecutionContextEndResourceTracker(exec_ctx, *name);
    DISPATCH_NEXT();
  }

  OP(ExecutionContextStartPipelineTracker) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto pipeline_id = execution::pipeline_id_t{frame->LocalAt<uint32_t>(READ_LOCAL_ID())};
    OpExecutionContextStartPipelineTracker(exec_ctx, pipeline_id);
    DISPATCH_NEXT();
  }

  OP(ExecutionContextEndPipelineTracker) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto query_id = execution::query_id_t{frame->LocalAt<uint32_t>(READ_LOCAL_ID())};
    auto pipeline_id = execution::pipeline_id_t{frame->LocalAt<uint32_t>(READ_LOCAL_ID())};
    auto *ouvec = frame->LocalAt<brain::ExecOUFeatureVector *>(READ_LOCAL_ID());
    OpExecutionContextEndPipelineTracker(exec_ctx, query_id, pipeline_id, ouvec);
    DISPATCH_NEXT();
  }

  OP(ExecOUFeatureVectorRecordFeature) : {
    auto *ouvec = frame->LocalAt<brain::ExecOUFeatureVector *>(READ_LOCAL_ID());
    auto pipeline_id = execution::pipeline_id_t{frame->LocalAt<uint32_t>(READ_LOCAL_ID())};
    auto feature_id = execution::feature_id_t{frame->LocalAt<uint32_t>(READ_LOCAL_ID())};
    auto feature_attribute =
        static_cast<brain::ExecutionOperatingUnitFeatureAttribute>(frame->LocalAt<uint32_t>(READ_LOCAL_ID()));
    auto mode = static_cast<brain::ExecutionOperatingUnitFeatureUpdateMode>(frame->LocalAt<uint32_t>(READ_LOCAL_ID()));
    auto value = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpExecOUFeatureVectorRecordFeature(ouvec, pipeline_id, feature_id, feature_attribute, mode, value);
    DISPATCH_NEXT();
  }

  OP(ExecOUFeatureVectorInitialize) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *ouvec = frame->LocalAt<brain::ExecOUFeatureVector *>(READ_LOCAL_ID());
    auto pipeline_id = execution::pipeline_id_t{frame->LocalAt<uint32_t>(READ_LOCAL_ID())};
    auto is_parallel = frame->LocalAt<bool>(READ_LOCAL_ID());
    OpExecOUFeatureVectorInitialize(exec_ctx, ouvec, pipeline_id, is_parallel);
    DISPATCH_NEXT();
  }

  OP(ExecOUFeatureVectorFilter) : {
    auto *ouvec = frame->LocalAt<brain::ExecOUFeatureVector *>(READ_LOCAL_ID());
    auto type = static_cast<brain::ExecutionOperatingUnitType>(frame->LocalAt<uint32_t>(READ_LOCAL_ID()));
    OpExecOUFeatureVectorFilter(ouvec, type);
    DISPATCH_NEXT();
  }

  OP(ExecOUFeatureVectorReset) : {
    auto *ouvec = frame->LocalAt<brain::ExecOUFeatureVector *>(READ_LOCAL_ID());
    OpExecOUFeatureVectorReset(ouvec);
    DISPATCH_NEXT();
  }

  OP(RegisterMetricsThread) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpRegisterMetricsThread(exec_ctx);
    DISPATCH_NEXT();
  }

  OP(CheckTrackersStopped) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpCheckTrackersStopped(exec_ctx);
    DISPATCH_NEXT();
  }

  OP(AggregateMetricsThread) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpAggregateMetricsThread(exec_ctx);
    DISPATCH_NEXT();
  }

  OP(ThreadStateContainerAccessCurrentThreadState) : {
    auto *state = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    OpThreadStateContainerAccessCurrentThreadState(state, thread_state_container);
    DISPATCH_NEXT();
  }

  OP(ThreadStateContainerIterate) : {
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto ctx = frame->LocalAt<void *>(READ_LOCAL_ID());
    auto iterate_fn_id = READ_FUNC_ID();

    auto iterate_fn =
        reinterpret_cast<sql::ThreadStateContainer::IterateFn>(module_->GetRawFunctionImpl(iterate_fn_id));
    OpThreadStateContainerIterate(thread_state_container, ctx, iterate_fn);
    DISPATCH_NEXT();
  }

  OP(ThreadStateContainerReset) : {
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto size = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto init_fn_id = READ_FUNC_ID();
    auto destroy_fn_id = READ_FUNC_ID();
    auto *ctx = frame->LocalAt<void *>(READ_LOCAL_ID());

    auto init_fn = reinterpret_cast<sql::ThreadStateContainer::InitFn>(module_->GetRawFunctionImpl(init_fn_id));
    auto destroy_fn =
        reinterpret_cast<sql::ThreadStateContainer::DestroyFn>(module_->GetRawFunctionImpl(destroy_fn_id));
    OpThreadStateContainerReset(thread_state_container, size, init_fn, destroy_fn, ctx);
    DISPATCH_NEXT();
  }

  OP(ThreadStateContainerClear) : {
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    OpThreadStateContainerClear(thread_state_container);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Table Vector and Vector Projection Iterator (VPI) ops
  // -------------------------------------------------------

  OP(TableVectorIteratorInit) : {
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    auto exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto table_oid = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto col_oids = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto num_oids = READ_UIMM4();
    OpTableVectorIteratorInit(iter, exec_ctx, table_oid, col_oids, num_oids);
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

  OP(TableVectorIteratorGetVPINumTuples) : {
    auto *num_tuples_vpi = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorGetVPINumTuples(num_tuples_vpi, iter);
    DISPATCH_NEXT();
  }

  OP(TableVectorIteratorGetVPI) : {
    auto *vpi = frame->LocalAt<sql::VectorProjectionIterator **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::TableVectorIterator *>(READ_LOCAL_ID());
    OpTableVectorIteratorGetVPI(vpi, iter);
    DISPATCH_NEXT();
  }

  OP(ParallelScanTable) : {
    auto table_oid = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto col_oids = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto num_oids = READ_UIMM4();
    auto query_state = frame->LocalAt<void *>(READ_LOCAL_ID());
    auto *exec_context = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto scan_fn_id = READ_FUNC_ID();

    auto scan_fn = reinterpret_cast<sql::TableVectorIterator::ScanFn>(module_->GetRawFunctionImpl(scan_fn_id));
    OpParallelScanTable(table_oid, col_oids, num_oids, query_state, exec_context, scan_fn);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // VPI iteration operations
  // -------------------------------------------------------

  OP(VPIInit) : {
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    auto *vector_projection = frame->LocalAt<sql::VectorProjection *>(READ_LOCAL_ID());
    OpVPIInit(iter, vector_projection);
    DISPATCH_NEXT();
  }

  OP(VPIInitWithList) : {
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    auto *vector_projection = frame->LocalAt<sql::VectorProjection *>(READ_LOCAL_ID());
    auto *tid_list = frame->LocalAt<sql::TupleIdList *>(READ_LOCAL_ID());
    OpVPIInitWithList(iter, vector_projection, tid_list);
    DISPATCH_NEXT();
  }

  OP(VPIIsFiltered) : {
    auto *is_filtered = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIIsFiltered(is_filtered, iter);
    DISPATCH_NEXT();
  }

  OP(VPIGetSelectedRowCount) : {
    auto *count = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIGetSelectedRowCount(count, iter);
    DISPATCH_NEXT();
  }

  OP(VPIGetVectorProjection) : {
    auto *vector_projection = frame->LocalAt<sql::VectorProjection **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIGetVectorProjection(vector_projection, iter);
    DISPATCH_NEXT();
  }

  OP(VPIHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIHasNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(VPIHasNextFiltered) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIHasNextFiltered(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(VPIAdvance) : {
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIAdvance(iter);
    DISPATCH_NEXT();
  }

  OP(VPIAdvanceFiltered) : {
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIAdvanceFiltered(iter);
    DISPATCH_NEXT();
  }

  OP(VPISetPosition) : {
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    auto index = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpVPISetPosition(iter, index);
    DISPATCH_NEXT();
  }

  OP(VPISetPositionFiltered) : {
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    auto index = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpVPISetPositionFiltered(iter, index);
    DISPATCH_NEXT();
  }

  OP(VPIMatch) : {
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    auto match = frame->LocalAt<bool>(READ_LOCAL_ID());
    OpVPIMatch(iter, match);
    DISPATCH_NEXT();
  }

  OP(VPIReset) : {
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIReset(iter);
    DISPATCH_NEXT();
  }

  OP(VPIResetFiltered) : {
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIResetFiltered(iter);
    DISPATCH_NEXT();
  }

  OP(VPIFree) : {
    auto *iter = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIFree(iter);
    DISPATCH_NEXT();
  }

  OP(VPIGetSlot) : {
    auto *slot = frame->LocalAt<storage::TupleSlot *>(READ_LOCAL_ID());
    auto *vpi = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    OpVPIGetSlot(slot, vpi);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // VPI element access
  // -------------------------------------------------------

#define GEN_VPI_ACCESS(NAME, CPP_TYPE)                                            \
  OP(VPIGet##NAME) : {                                                            \
    auto *result = frame->LocalAt<CPP_TYPE *>(READ_LOCAL_ID());                   \
    auto *vpi = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                                  \
    OpVPIGet##NAME(result, vpi, col_idx);                                         \
    DISPATCH_NEXT();                                                              \
  }                                                                               \
  OP(VPIGet##NAME##Null) : {                                                      \
    auto *result = frame->LocalAt<CPP_TYPE *>(READ_LOCAL_ID());                   \
    auto *vpi = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM4();                                                  \
    OpVPIGet##NAME##Null(result, vpi, col_idx);                                   \
    DISPATCH_NEXT();                                                              \
  }                                                                               \
  OP(VPISet##NAME) : {                                                            \
    auto *vpi = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID()); \
    auto *input = frame->LocalAt<CPP_TYPE *>(READ_LOCAL_ID());                    \
    auto col_idx = READ_UIMM4();                                                  \
    OpVPISet##NAME(vpi, input, col_idx);                                          \
    DISPATCH_NEXT();                                                              \
  }                                                                               \
  OP(VPISet##NAME##Null) : {                                                      \
    auto *vpi = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID()); \
    auto *input = frame->LocalAt<CPP_TYPE *>(READ_LOCAL_ID());                    \
    auto col_idx = READ_UIMM4();                                                  \
    OpVPISet##NAME##Null(vpi, input, col_idx);                                    \
    DISPATCH_NEXT();                                                              \
  }
  GEN_VPI_ACCESS(Bool, sql::BoolVal)
  GEN_VPI_ACCESS(TinyInt, sql::Integer)
  GEN_VPI_ACCESS(SmallInt, sql::Integer)
  GEN_VPI_ACCESS(Integer, sql::Integer)
  GEN_VPI_ACCESS(BigInt, sql::Integer)
  GEN_VPI_ACCESS(Real, sql::Real)
  GEN_VPI_ACCESS(Double, sql::Real)
  GEN_VPI_ACCESS(Decimal, sql::DecimalVal)
  GEN_VPI_ACCESS(Date, sql::DateVal)
  GEN_VPI_ACCESS(Timestamp, sql::TimestampVal)
  GEN_VPI_ACCESS(String, sql::StringVal)
#undef GEN_VPI_ACCESS

  OP(VPIGetPointer) : {
    auto result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto vpi = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    auto col_idx = READ_UIMM4();
    OpVPIGetPointer(result, vpi, col_idx);
    DISPATCH_NEXT();
  }

  // ------------------------------------------------------
  // Hashing
  // ------------------------------------------------------

#define GEN_HASH(NAME, CPP_TYPE)                                \
  OP(Hash##NAME) : {                                            \
    auto *hash_val = frame->LocalAt<hash_t *>(READ_LOCAL_ID()); \
    auto *input = frame->LocalAt<CPP_TYPE *>(READ_LOCAL_ID());  \
    auto seed = frame->LocalAt<const hash_t>(READ_LOCAL_ID());  \
    OpHash##NAME(hash_val, input, seed);                        \
    DISPATCH_NEXT();                                            \
  }

  GEN_HASH(Int, sql::Integer)
  GEN_HASH(Bool, sql::BoolVal)
  GEN_HASH(Real, sql::Real)
  GEN_HASH(Date, sql::DateVal)
  GEN_HASH(Timestamp, sql::TimestampVal)
  GEN_HASH(String, sql::StringVal)
#undef GEN_HASH

  OP(HashCombine) : {
    auto *hash_val = frame->LocalAt<hash_t *>(READ_LOCAL_ID());
    auto new_hash_val = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    OpHashCombine(hash_val, new_hash_val);
    DISPATCH_NEXT();
  }

  // ------------------------------------------------------
  // Filter Manager
  // ------------------------------------------------------

  OP(FilterManagerInit) : {
    auto *filter_manager = frame->LocalAt<sql::FilterManager *>(READ_LOCAL_ID());
    auto *exec_context = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpFilterManagerInit(filter_manager, exec_context->GetExecutionSettings());
    DISPATCH_NEXT();
  }

  OP(FilterManagerStartNewClause) : {
    auto *filter_manager = frame->LocalAt<sql::FilterManager *>(READ_LOCAL_ID());
    OpFilterManagerStartNewClause(filter_manager);
    DISPATCH_NEXT();
  }

  OP(FilterManagerInsertFilter) : {
    auto *filter_manager = frame->LocalAt<sql::FilterManager *>(READ_LOCAL_ID());
    auto func_id = READ_FUNC_ID();
    auto fn = reinterpret_cast<sql::FilterManager::MatchFn>(module_->GetRawFunctionImpl(func_id));
    OpFilterManagerInsertFilter(filter_manager, fn);
    DISPATCH_NEXT();
  }

  OP(FilterManagerRunFilters) : {
    auto *filter_manager = frame->LocalAt<sql::FilterManager *>(READ_LOCAL_ID());
    auto *vpi = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpFilterManagerRunFilters(filter_manager, vpi, exec_ctx);
    DISPATCH_NEXT();
  }

  OP(FilterManagerFree) : {
    auto *filter_manager = frame->LocalAt<sql::FilterManager *>(READ_LOCAL_ID());
    OpFilterManagerFree(filter_manager);
    DISPATCH_NEXT();
  }

  // ------------------------------------------------------
  // Vector Filter Executor
  // ------------------------------------------------------

#define GEN_VEC_FILTER(BYTECODE)                                                                               \
  OP(BYTECODE) : {                                                                                             \
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());                                \
    auto *vector_projection = frame->LocalAt<sql::VectorProjection *>(READ_LOCAL_ID());                        \
    auto left_col_idx = frame->LocalAt<uint32_t>(READ_LOCAL_ID());                                             \
    auto right_col_idx = frame->LocalAt<uint32_t>(READ_LOCAL_ID());                                            \
    auto *tid_list = frame->LocalAt<sql::TupleIdList *>(READ_LOCAL_ID());                                      \
    Op##BYTECODE(exec_ctx->GetExecutionSettings(), vector_projection, left_col_idx, right_col_idx, tid_list);  \
    DISPATCH_NEXT();                                                                                           \
  }                                                                                                            \
  OP(BYTECODE##Val) : {                                                                                        \
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());                                \
    auto *vector_projection = frame->LocalAt<sql::VectorProjection *>(READ_LOCAL_ID());                        \
    auto left_col_idx = frame->LocalAt<uint32_t>(READ_LOCAL_ID());                                             \
    auto right_val = frame->LocalAt<sql::Val *>(READ_LOCAL_ID());                                              \
    auto *tid_list = frame->LocalAt<sql::TupleIdList *>(READ_LOCAL_ID());                                      \
    Op##BYTECODE##Val(exec_ctx->GetExecutionSettings(), vector_projection, left_col_idx, right_val, tid_list); \
    DISPATCH_NEXT();                                                                                           \
  }

  GEN_VEC_FILTER(VectorFilterEqual)
  GEN_VEC_FILTER(VectorFilterGreaterThan)
  GEN_VEC_FILTER(VectorFilterGreaterThanEqual)
  GEN_VEC_FILTER(VectorFilterLessThan)
  GEN_VEC_FILTER(VectorFilterLessThanEqual)
  GEN_VEC_FILTER(VectorFilterNotEqual)
  GEN_VEC_FILTER(VectorFilterLike)
  GEN_VEC_FILTER(VectorFilterNotLike)

#undef GEN_VEC_FILTER

  // -------------------------------------------------------
  // SQL Value Creation.
  // -------------------------------------------------------

  OP(ForceBoolTruth) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *sql_bool = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());
    OpForceBoolTruth(result, sql_bool);
    DISPATCH_NEXT();
  }

  OP(InitSqlNull) : {
    auto *sql_null = frame->LocalAt<sql::Val *>(READ_LOCAL_ID());
    OpInitSqlNull(sql_null);
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
    auto val = frame->LocalAt<int32_t>(READ_LOCAL_ID());
    OpInitInteger(sql_int, val);
    DISPATCH_NEXT();
  }

  OP(InitInteger64) : {
    auto *sql_int = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto val = frame->LocalAt<int64_t>(READ_LOCAL_ID());
    OpInitInteger64(sql_int, val);
    DISPATCH_NEXT();
  }

  OP(InitReal) : {
    auto *sql_real = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto val = frame->LocalAt<double>(READ_LOCAL_ID());
    OpInitReal(sql_real, val);
    DISPATCH_NEXT();
  }

  OP(InitDate) : {
    auto *sql_date = frame->LocalAt<sql::DateVal *>(READ_LOCAL_ID());
    auto year = frame->LocalAt<int32_t>(READ_LOCAL_ID());
    auto month = frame->LocalAt<int32_t>(READ_LOCAL_ID());
    auto day = frame->LocalAt<int32_t>(READ_LOCAL_ID());
    OpInitDate(sql_date, year, month, day);
    DISPATCH_NEXT();
  }

  OP(InitTimestamp) : {
    auto *sql_timestamp = frame->LocalAt<sql::TimestampVal *>(READ_LOCAL_ID());
    auto usec = frame->LocalAt<uint64_t>(READ_LOCAL_ID());
    OpInitTimestamp(sql_timestamp, usec);
    DISPATCH_NEXT();
  }

  OP(InitTimestampYMDHMSMU) : {
    auto *sql_timestamp = frame->LocalAt<sql::TimestampVal *>(READ_LOCAL_ID());
    auto year = frame->LocalAt<int32_t>(READ_LOCAL_ID());
    auto month = frame->LocalAt<int32_t>(READ_LOCAL_ID());
    auto day = frame->LocalAt<int32_t>(READ_LOCAL_ID());
    auto h = frame->LocalAt<int32_t>(READ_LOCAL_ID());
    auto m = frame->LocalAt<int32_t>(READ_LOCAL_ID());
    auto s = frame->LocalAt<int32_t>(READ_LOCAL_ID());
    auto ms = frame->LocalAt<int32_t>(READ_LOCAL_ID());
    auto us = frame->LocalAt<int32_t>(READ_LOCAL_ID());
    OpInitTimestampYMDHMSMU(sql_timestamp, year, month, day, h, m, s, ms, us);
    DISPATCH_NEXT();
  }

  OP(InitString) : {
    auto *sql_string = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *string = module_->GetBytecodeModule()->AccessStaticLocalDataRaw(LocalVar::Decode(READ_STATIC_LOCAL_ID()));
    auto length = READ_UIMM4();
    OpInitString(sql_string, string, length);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // SQL Value Casts.
  // -------------------------------------------------------

#define GEN_CONVERT_TO_STRING(Bytecode, InputType)                              \
  OP(Bytecode) : {                                                              \
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());           \
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID()); \
    auto *input = frame->LocalAt<InputType *>(READ_LOCAL_ID());                 \
    Op##Bytecode(result, exec_ctx, input);                                      \
    DISPATCH_NEXT();                                                            \
  }

  // Convert something to string.
  GEN_CONVERT_TO_STRING(IntegerToString, sql::Integer);
  GEN_CONVERT_TO_STRING(RealToString, sql::Real);
  GEN_CONVERT_TO_STRING(DateToString, sql::DateVal);
  GEN_CONVERT_TO_STRING(TimestampToString, sql::TimestampVal);
#undef GEN_CONVERT_TO_STRING

#define GEN_CONVERSION(Bytecode, InputType, OutputType)           \
  OP(Bytecode) : {                                                \
    auto *result = frame->LocalAt<OutputType *>(READ_LOCAL_ID()); \
    auto *input = frame->LocalAt<InputType *>(READ_LOCAL_ID());   \
    Op##Bytecode(result, input);                                  \
    DISPATCH_NEXT();                                              \
  }

  // Boolean to something.
  GEN_CONVERSION(BoolToInteger, sql::BoolVal, sql::Integer);
  // Integer to something.
  GEN_CONVERSION(IntegerToBool, sql::Integer, sql::BoolVal);
  GEN_CONVERSION(IntegerToReal, sql::Integer, sql::Real);
  // Real to something.
  GEN_CONVERSION(RealToBool, sql::Real, sql::BoolVal);
  GEN_CONVERSION(RealToInteger, sql::Real, sql::Integer);
  // Date to something.
  GEN_CONVERSION(DateToTimestamp, sql::DateVal, sql::TimestampVal);
  // Timestamp to something.
  GEN_CONVERSION(TimestampToDate, sql::TimestampVal, sql::DateVal);
  // String to something.
  GEN_CONVERSION(StringToBool, sql::StringVal, sql::BoolVal);
  GEN_CONVERSION(StringToInteger, sql::StringVal, sql::Integer);
  GEN_CONVERSION(StringToReal, sql::StringVal, sql::Real);
  GEN_CONVERSION(StringToDate, sql::StringVal, sql::DateVal);
  GEN_CONVERSION(StringToTimestamp, sql::StringVal, sql::TimestampVal);

#undef GEN_CONVERSION

  // -------------------------------------------------------
  // Comparisons.
  // -------------------------------------------------------

#define GEN_CMP(op)                                                     \
  OP(op##Bool) : {                                                      \
    auto *result = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());     \
    auto *left = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());       \
    auto *right = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());      \
    Op##op##Bool(result, left, right);                                  \
    DISPATCH_NEXT();                                                    \
  }                                                                     \
  OP(op##Integer) : {                                                   \
    auto *result = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());     \
    auto *left = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());       \
    auto *right = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());      \
    Op##op##Integer(result, left, right);                               \
    DISPATCH_NEXT();                                                    \
  }                                                                     \
  OP(op##Real) : {                                                      \
    auto *result = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());     \
    auto *left = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());          \
    auto *right = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());         \
    Op##op##Real(result, left, right);                                  \
    DISPATCH_NEXT();                                                    \
  }                                                                     \
  OP(op##Date) : {                                                      \
    auto *result = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());     \
    auto *left = frame->LocalAt<sql::DateVal *>(READ_LOCAL_ID());       \
    auto *right = frame->LocalAt<sql::DateVal *>(READ_LOCAL_ID());      \
    Op##op##Date(result, left, right);                                  \
    DISPATCH_NEXT();                                                    \
  }                                                                     \
  OP(op##Timestamp) : {                                                 \
    auto *result = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());     \
    auto *left = frame->LocalAt<sql::TimestampVal *>(READ_LOCAL_ID());  \
    auto *right = frame->LocalAt<sql::TimestampVal *>(READ_LOCAL_ID()); \
    Op##op##Timestamp(result, left, right);                             \
    DISPATCH_NEXT();                                                    \
  }                                                                     \
  OP(op##String) : {                                                    \
    auto *result = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());     \
    auto *left = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());     \
    auto *right = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());    \
    Op##op##String(result, left, right);                                \
    DISPATCH_NEXT();                                                    \
  }
  GEN_CMP(GreaterThan);
  GEN_CMP(GreaterThanEqual);
  GEN_CMP(Equal);
  GEN_CMP(LessThan);
  GEN_CMP(LessThanEqual);
  GEN_CMP(NotEqual);
#undef GEN_CMP

#define GEN_UNARY_MATH_OPS(op)                                      \
  OP(op##Integer) : {                                               \
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID()); \
    auto *input = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());  \
    Op##op##Integer(result, input);                                 \
    DISPATCH_NEXT();                                                \
  }                                                                 \
  OP(op##Real) : {                                                  \
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());    \
    auto *input = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());     \
    Op##op##Real(result, input);                                    \
    DISPATCH_NEXT();                                                \
  }

  GEN_UNARY_MATH_OPS(Abs)

#undef GEN_UNARY_MATH_OPS

  OP(ValIsNull) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<const sql::Val *>(READ_LOCAL_ID());
    OpValIsNull(result, val);
    DISPATCH_NEXT();
  }

  OP(ValIsNotNull) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<const sql::Val *>(READ_LOCAL_ID());
    OpValIsNotNull(result, val);
    DISPATCH_NEXT();
  }

#define GEN_MATH_OPS(op)                                            \
  OP(op##Integer) : {                                               \
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID()); \
    auto *left = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());   \
    auto *right = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());  \
    Op##op##Integer(result, left, right);                           \
    DISPATCH_NEXT();                                                \
  }                                                                 \
  OP(op##Real) : {                                                  \
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());    \
    auto *left = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());      \
    auto *right = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());     \
    Op##op##Real(result, left, right);                              \
    DISPATCH_NEXT();                                                \
  }

  GEN_MATH_OPS(Add)
  GEN_MATH_OPS(Sub)
  GEN_MATH_OPS(Mul)
  GEN_MATH_OPS(Div)
  GEN_MATH_OPS(Mod)

#undef GEN_MATH_OPS

  // -------------------------------------------------------
  // Aggregations
  // -------------------------------------------------------

  OP(AggregationHashTableInit) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto payload_size = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpAggregationHashTableInit(agg_hash_table, exec_ctx, payload_size);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableGetTupleCount) : {
    auto *result = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    OpAggregationHashTableGetTupleCount(result, agg_hash_table);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableGetInsertCount) : {
    auto *result = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    OpAggregationHashTableGetInsertCount(result, agg_hash_table);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableAllocTuple) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto hash = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    OpAggregationHashTableAllocTuple(result, agg_hash_table, hash);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableAllocTuplePartitioned) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto hash = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    OpAggregationHashTableAllocTuplePartitioned(result, agg_hash_table, hash);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableLinkHashTableEntry) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto *entry = frame->LocalAt<sql::HashTableEntry *>(READ_LOCAL_ID());
    OpAggregationHashTableLinkHashTableEntry(agg_hash_table, entry);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableLookup) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto hash = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    auto key_eq_fn_id = READ_FUNC_ID();
    auto *probe_tuple = frame->LocalAt<void *>(READ_LOCAL_ID());

    auto key_eq_fn = reinterpret_cast<sql::AggregationHashTable::KeyEqFn>(module_->GetRawFunctionImpl(key_eq_fn_id));
    OpAggregationHashTableLookup(result, agg_hash_table, hash, key_eq_fn, probe_tuple);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableProcessBatch) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto *vpi = frame->LocalAt<sql::VectorProjectionIterator *>(READ_LOCAL_ID());
    auto num_keys = READ_UIMM4();
    auto key_cols = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto init_agg_fn_id = READ_FUNC_ID();
    auto merge_agg_fn_id = READ_FUNC_ID();
    auto partitioned = frame->LocalAt<bool>(READ_LOCAL_ID());

    auto init_agg_fn =
        reinterpret_cast<sql::AggregationHashTable::VectorInitAggFn>(module_->GetRawFunctionImpl(init_agg_fn_id));
    auto merge_agg_fn =
        reinterpret_cast<sql::AggregationHashTable::VectorAdvanceAggFn>(module_->GetRawFunctionImpl(merge_agg_fn_id));
    OpAggregationHashTableProcessBatch(agg_hash_table, vpi, num_keys, key_cols, init_agg_fn, merge_agg_fn, partitioned);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableTransferPartitions) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto agg_ht_offset = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto merge_partition_fn_id = READ_FUNC_ID();

    auto merge_partition_fn = reinterpret_cast<sql::AggregationHashTable::MergePartitionFn>(
        module_->GetRawFunctionImpl(merge_partition_fn_id));
    OpAggregationHashTableTransferPartitions(agg_hash_table, thread_state_container, agg_ht_offset, merge_partition_fn);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableBuildAllHashTablePartitions) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto *query_state = frame->LocalAt<void *>(READ_LOCAL_ID());
    OpAggregationHashTableBuildAllHashTablePartitions(agg_hash_table, query_state);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableRepartition) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    OpAggregationHashTableRepartition(agg_hash_table);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableMergePartitions) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto *target_agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto *query_state = frame->LocalAt<void *>(READ_LOCAL_ID());
    auto merge_partition_fn_id = READ_FUNC_ID();
    auto merge_partition_fn = reinterpret_cast<sql::AggregationHashTable::MergePartitionFn>(
        module_->GetRawFunctionImpl(merge_partition_fn_id));
    OpAggregationHashTableMergePartitions(agg_hash_table, target_agg_hash_table, query_state, merge_partition_fn);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableParallelPartitionedScan) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    auto *query_state = frame->LocalAt<void *>(READ_LOCAL_ID());
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto scan_partition_fn_id = READ_FUNC_ID();

    auto scan_partition_fn =
        reinterpret_cast<sql::AggregationHashTable::ScanPartitionFn>(module_->GetRawFunctionImpl(scan_partition_fn_id));
    OpAggregationHashTableParallelPartitionedScan(agg_hash_table, query_state, thread_state_container,
                                                  scan_partition_fn);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableFree) : {
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    OpAggregationHashTableFree(agg_hash_table);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableIteratorInit) : {
    auto *iter = frame->LocalAt<sql::AHTIterator *>(READ_LOCAL_ID());
    auto *agg_hash_table = frame->LocalAt<sql::AggregationHashTable *>(READ_LOCAL_ID());
    OpAggregationHashTableIteratorInit(iter, agg_hash_table);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableIteratorHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::AHTIterator *>(READ_LOCAL_ID());
    OpAggregationHashTableIteratorHasNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableIteratorNext) : {
    auto *agg_hash_table_iter = frame->LocalAt<sql::AHTIterator *>(READ_LOCAL_ID());
    OpAggregationHashTableIteratorNext(agg_hash_table_iter);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableIteratorGetRow) : {
    auto *row = frame->LocalAt<const byte **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::AHTIterator *>(READ_LOCAL_ID());
    OpAggregationHashTableIteratorGetRow(row, iter);
    DISPATCH_NEXT();
  }

  OP(AggregationHashTableIteratorFree) : {
    auto *agg_hash_table_iter = frame->LocalAt<sql::AHTIterator *>(READ_LOCAL_ID());
    OpAggregationHashTableIteratorFree(agg_hash_table_iter);
    DISPATCH_NEXT();
  }

  OP(AggregationOverflowPartitionIteratorHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *overflow_iter = frame->LocalAt<sql::AHTOverflowPartitionIterator *>(READ_LOCAL_ID());
    OpAggregationOverflowPartitionIteratorHasNext(has_more, overflow_iter);
    DISPATCH_NEXT();
  }

  OP(AggregationOverflowPartitionIteratorNext) : {
    auto *overflow_iter = frame->LocalAt<sql::AHTOverflowPartitionIterator *>(READ_LOCAL_ID());
    OpAggregationOverflowPartitionIteratorNext(overflow_iter);
    DISPATCH_NEXT();
  }

  OP(AggregationOverflowPartitionIteratorGetHash) : {
    auto *hash = frame->LocalAt<hash_t *>(READ_LOCAL_ID());
    auto *overflow_iter = frame->LocalAt<sql::AHTOverflowPartitionIterator *>(READ_LOCAL_ID());
    OpAggregationOverflowPartitionIteratorGetHash(hash, overflow_iter);
    DISPATCH_NEXT();
  }

  OP(AggregationOverflowPartitionIteratorGetRow) : {
    auto *row = frame->LocalAt<const byte **>(READ_LOCAL_ID());
    auto *overflow_iter = frame->LocalAt<sql::AHTOverflowPartitionIterator *>(READ_LOCAL_ID());
    OpAggregationOverflowPartitionIteratorGetRow(row, overflow_iter);
    DISPATCH_NEXT();
  }

  OP(AggregationOverflowPartitionIteratorGetRowEntry) : {
    auto *entry = frame->LocalAt<sql::HashTableEntry **>(READ_LOCAL_ID());
    auto *overflow_iter = frame->LocalAt<sql::AHTOverflowPartitionIterator *>(READ_LOCAL_ID());
    OpAggregationOverflowPartitionIteratorGetRowEntry(entry, overflow_iter);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Aggregates
  // -------------------------------------------------------

#define GEN_COUNT_AGG(AGG_TYPE)                                     \
  OP(AGG_TYPE##Init) : {                                            \
    auto *agg = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID());   \
    Op##AGG_TYPE##Init(agg);                                        \
    DISPATCH_NEXT();                                                \
  }                                                                 \
                                                                    \
  OP(AGG_TYPE##Advance) : {                                         \
    auto *agg = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID());   \
    auto *val = frame->LocalAt<sql::Val *>(READ_LOCAL_ID());        \
    Op##AGG_TYPE##Advance(agg, val);                                \
    DISPATCH_NEXT();                                                \
  }                                                                 \
                                                                    \
  OP(AGG_TYPE##Merge) : {                                           \
    auto *agg_1 = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID()); \
    auto *agg_2 = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID()); \
    Op##AGG_TYPE##Merge(agg_1, agg_2);                              \
    DISPATCH_NEXT();                                                \
  }                                                                 \
                                                                    \
  OP(AGG_TYPE##Reset) : {                                           \
    auto *agg = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID());   \
    Op##AGG_TYPE##Reset(agg);                                       \
    DISPATCH_NEXT();                                                \
  }                                                                 \
                                                                    \
  OP(AGG_TYPE##GetResult) : {                                       \
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID()); \
    auto *agg = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID());   \
    Op##AGG_TYPE##GetResult(result, agg);                           \
    DISPATCH_NEXT();                                                \
  }                                                                 \
                                                                    \
  OP(AGG_TYPE##Free) : {                                            \
    auto *agg = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID());   \
    Op##AGG_TYPE##Free(agg);                                        \
    DISPATCH_NEXT();                                                \
  }

  GEN_COUNT_AGG(CountAggregate)
  GEN_COUNT_AGG(CountStarAggregate)

#undef GEN_COUNT_AGG

#define GEN_AGGREGATE(SQL_TYPE, AGG_TYPE)                            \
  OP(AGG_TYPE##Init) : {                                             \
    auto *agg = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID());    \
    Op##AGG_TYPE##Init(agg);                                         \
    DISPATCH_NEXT();                                                 \
  }                                                                  \
  OP(AGG_TYPE##Advance) : {                                          \
    auto *agg = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID());    \
    auto *val = frame->LocalAt<sql::SQL_TYPE *>(READ_LOCAL_ID());    \
    Op##AGG_TYPE##Advance(agg, val);                                 \
    DISPATCH_NEXT();                                                 \
  }                                                                  \
  OP(AGG_TYPE##Merge) : {                                            \
    auto *agg_1 = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID());  \
    auto *agg_2 = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID());  \
    Op##AGG_TYPE##Merge(agg_1, agg_2);                               \
    DISPATCH_NEXT();                                                 \
  }                                                                  \
  OP(AGG_TYPE##Reset) : {                                            \
    auto *agg = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID());    \
    Op##AGG_TYPE##Reset(agg);                                        \
    DISPATCH_NEXT();                                                 \
  }                                                                  \
  OP(AGG_TYPE##GetResult) : {                                        \
    auto *result = frame->LocalAt<sql::SQL_TYPE *>(READ_LOCAL_ID()); \
    auto *agg = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID());    \
    Op##AGG_TYPE##GetResult(result, agg);                            \
    DISPATCH_NEXT();                                                 \
  }                                                                  \
  OP(AGG_TYPE##Free) : {                                             \
    auto *agg = frame->LocalAt<sql::AGG_TYPE *>(READ_LOCAL_ID());    \
    Op##AGG_TYPE##Free(agg);                                         \
    DISPATCH_NEXT();                                                 \
  }

  GEN_AGGREGATE(Integer, IntegerSumAggregate);
  GEN_AGGREGATE(Integer, IntegerMaxAggregate);
  GEN_AGGREGATE(Integer, IntegerMinAggregate);
  GEN_AGGREGATE(Real, RealSumAggregate);
  GEN_AGGREGATE(Real, RealMaxAggregate);
  GEN_AGGREGATE(Real, RealMinAggregate);
  GEN_AGGREGATE(DateVal, DateMaxAggregate);
  GEN_AGGREGATE(DateVal, DateMinAggregate);
  GEN_AGGREGATE(StringVal, StringMaxAggregate);
  GEN_AGGREGATE(StringVal, StringMinAggregate);

#undef GEN_AGGREGATE

  OP(AvgAggregateInit) : {
    auto *agg = frame->LocalAt<sql::AvgAggregate *>(READ_LOCAL_ID());
    OpAvgAggregateInit(agg);
    DISPATCH_NEXT();
  }

  OP(AvgAggregateAdvanceInteger) : {
    auto *agg = frame->LocalAt<sql::AvgAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    OpAvgAggregateAdvanceInteger(agg, val);
    DISPATCH_NEXT();
  }

  OP(AvgAggregateAdvanceReal) : {
    auto *agg = frame->LocalAt<sql::AvgAggregate *>(READ_LOCAL_ID());
    auto *val = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpAvgAggregateAdvanceReal(agg, val);
    DISPATCH_NEXT();
  }

  OP(AvgAggregateMerge) : {
    auto *agg_1 = frame->LocalAt<sql::AvgAggregate *>(READ_LOCAL_ID());
    auto *agg_2 = frame->LocalAt<sql::AvgAggregate *>(READ_LOCAL_ID());
    OpAvgAggregateMerge(agg_1, agg_2);
    DISPATCH_NEXT();
  }

  OP(AvgAggregateReset) : {
    auto *agg = frame->LocalAt<sql::AvgAggregate *>(READ_LOCAL_ID());
    OpAvgAggregateReset(agg);
    DISPATCH_NEXT();
  }

  OP(AvgAggregateGetResult) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *agg = frame->LocalAt<sql::AvgAggregate *>(READ_LOCAL_ID());
    OpAvgAggregateGetResult(result, agg);
    DISPATCH_NEXT();
  }

  OP(AvgAggregateFree) : {
    auto *agg = frame->LocalAt<sql::AvgAggregate *>(READ_LOCAL_ID());
    OpAvgAggregateFree(agg);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Hash Joins
  // -------------------------------------------------------

  OP(JoinHashTableInit) : {
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto tuple_size = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpJoinHashTableInit(join_hash_table, exec_ctx, tuple_size);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableAllocTuple) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    auto hash = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    OpJoinHashTableAllocTuple(result, join_hash_table, hash);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableGetTupleCount) : {
    auto *result = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    OpJoinHashTableGetTupleCount(result, join_hash_table);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableBuild) : {
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    OpJoinHashTableBuild(join_hash_table);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableBuildParallel) : {
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto jht_offset = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpJoinHashTableBuildParallel(join_hash_table, thread_state_container, jht_offset);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableLookup) : {
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    auto *ht_entry_iter = frame->LocalAt<sql::HashTableEntryIterator *>(READ_LOCAL_ID());
    auto hash_val = frame->LocalAt<hash_t>(READ_LOCAL_ID());
    OpJoinHashTableLookup(join_hash_table, ht_entry_iter, hash_val);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableFree) : {
    auto *join_hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    OpJoinHashTableFree(join_hash_table);
    DISPATCH_NEXT();
  }

  OP(HashTableEntryIteratorHasNext) : {
    auto *has_next = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *ht_entry_iter = frame->LocalAt<sql::HashTableEntryIterator *>(READ_LOCAL_ID());
    OpHashTableEntryIteratorHasNext(has_next, ht_entry_iter);
    DISPATCH_NEXT();
  }

  OP(HashTableEntryIteratorGetRow) : {
    const auto **row = frame->LocalAt<const byte **>(READ_LOCAL_ID());
    auto *ht_entry_iter = frame->LocalAt<sql::HashTableEntryIterator *>(READ_LOCAL_ID());
    OpHashTableEntryIteratorGetRow(row, ht_entry_iter);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableIteratorInit) : {
    auto *iter = frame->LocalAt<sql::JoinHashTableIterator *>(READ_LOCAL_ID());
    auto *hash_table = frame->LocalAt<sql::JoinHashTable *>(READ_LOCAL_ID());
    OpJoinHashTableIteratorInit(iter, hash_table);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableIteratorHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::JoinHashTableIterator *>(READ_LOCAL_ID());
    OpJoinHashTableIteratorHasNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableIteratorNext) : {
    auto *hash_table_iter = frame->LocalAt<sql::JoinHashTableIterator *>(READ_LOCAL_ID());
    OpJoinHashTableIteratorNext(hash_table_iter);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableIteratorGetRow) : {
    auto *row = frame->LocalAt<const byte **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::JoinHashTableIterator *>(READ_LOCAL_ID());
    OpJoinHashTableIteratorGetRow(row, iter);
    DISPATCH_NEXT();
  }

  OP(JoinHashTableIteratorFree) : {
    auto *hash_table_iter = frame->LocalAt<sql::JoinHashTableIterator *>(READ_LOCAL_ID());
    OpJoinHashTableIteratorFree(hash_table_iter);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Sorting
  // -------------------------------------------------------

  OP(SorterInit) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<terrier::execution::exec::ExecutionContext *>(READ_LOCAL_ID());
    auto cmp_func_id = READ_FUNC_ID();
    auto tuple_size = frame->LocalAt<uint32_t>(READ_LOCAL_ID());

    auto cmp_fn = reinterpret_cast<sql::Sorter::ComparisonFunction>(module_->GetRawFunctionImpl(cmp_func_id));
    OpSorterInit(sorter, exec_ctx, cmp_fn, tuple_size);
    DISPATCH_NEXT();
  }

  OP(SorterGetTupleCount) : {
    auto *result = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterGetTupleCount(result, sorter);
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
    auto top_k = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpSorterAllocTupleTopK(result, sorter, top_k);
    DISPATCH_NEXT();
  }

  OP(SorterAllocTupleTopKFinish) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto top_k = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpSorterAllocTupleTopKFinish(sorter, top_k);
    DISPATCH_NEXT();
  }

  OP(SorterSort) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    OpSorterSort(sorter);
    DISPATCH_NEXT();
  }

  OP(SorterSortParallel) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto sorter_offset = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpSorterSortParallel(sorter, thread_state_container, sorter_offset);
    DISPATCH_NEXT();
  }

  OP(SorterSortTopKParallel) : {
    auto *sorter = frame->LocalAt<sql::Sorter *>(READ_LOCAL_ID());
    auto *thread_state_container = frame->LocalAt<sql::ThreadStateContainer *>(READ_LOCAL_ID());
    auto sorter_offset = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto top_k = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpSorterSortTopKParallel(sorter, thread_state_container, sorter_offset, top_k);
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

  OP(SorterIteratorHasNext) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::SorterIterator *>(READ_LOCAL_ID());
    OpSorterIteratorHasNext(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(SorterIteratorNext) : {
    auto *iter = frame->LocalAt<sql::SorterIterator *>(READ_LOCAL_ID());
    OpSorterIteratorNext(iter);
    DISPATCH_NEXT();
  }

  OP(SorterIteratorSkipRows) : {
    auto *iter = frame->LocalAt<sql::SorterIterator *>(READ_LOCAL_ID());
    auto n = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpSorterIteratorSkipRows(iter, n);
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
  // Output
  // -------------------------------------------------------

  OP(ResultBufferNew) : {
    auto *result = frame->LocalAt<exec::OutputBuffer **>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpResultBufferNew(result, exec_ctx);
    DISPATCH_NEXT();
  }

  OP(ResultBufferAllocOutputRow) : {
    auto *result = frame->LocalAt<byte **>(READ_LOCAL_ID());
    auto *out = frame->LocalAt<exec::OutputBuffer *>(READ_LOCAL_ID());
    OpResultBufferAllocOutputRow(result, out);
    DISPATCH_NEXT();
  }

  OP(ResultBufferFinalize) : {
    auto *out = frame->LocalAt<exec::OutputBuffer *>(READ_LOCAL_ID());
    OpResultBufferFinalize(out);
    DISPATCH_NEXT();
  }

  OP(ResultBufferFree) : {
    auto *out = frame->LocalAt<exec::OutputBuffer *>(READ_LOCAL_ID());
    OpResultBufferFree(out);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // CSV Reader
  // -------------------------------------------------------
#if 0
  OP(CSVReaderInit) : {
    auto *reader = frame->LocalAt<util::CSVReader *>(READ_LOCAL_ID());
    auto *file_name = module_->GetBytecodeModule()->AccessStaticLocalDataRaw(LocalVar::Decode(READ_STATIC_LOCAL_ID()));
    auto length = READ_UIMM4();
    OpCSVReaderInit(reader, file_name, length);
    DISPATCH_NEXT();
  }

  OP(CSVReaderPerformInit) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *reader = frame->LocalAt<util::CSVReader *>(READ_LOCAL_ID());
    OpCSVReaderPerformInit(result, reader);
    DISPATCH_NEXT();
  }

  OP(CSVReaderAdvance) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *reader = frame->LocalAt<util::CSVReader *>(READ_LOCAL_ID());
    OpCSVReaderAdvance(has_more, reader);
    DISPATCH_NEXT();
  }

  OP(CSVReaderGetField) : {
    auto reader = frame->LocalAt<util::CSVReader *>(READ_LOCAL_ID());
    auto field_index = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto field = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    OpCSVReaderGetField(reader, field_index, field);
    DISPATCH_NEXT();
  }

  OP(CSVReaderGetRecordNumber) : {
    auto record_num = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto reader = frame->LocalAt<util::CSVReader *>(READ_LOCAL_ID());
    OpCSVReaderGetRecordNumber(record_num, reader);
    DISPATCH_NEXT();
  }

  OP(CSVReaderClose) : {
    auto *reader = frame->LocalAt<util::CSVReader *>(READ_LOCAL_ID());
    OpCSVReaderClose(reader);
    DISPATCH_NEXT();
  }
#endif
  // -------------------------------------------------------
  // Index Iterator
  // -------------------------------------------------------
  OP(IndexIteratorInit) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    auto exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto num_attrs = READ_UIMM4();
    auto table_oid = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto index_oid = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto col_oids = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto num_oids = READ_UIMM4();
    OpIndexIteratorInit(iter, exec_ctx, num_attrs, table_oid, index_oid, col_oids, num_oids);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorPerformInit) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorPerformInit(iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorGetSize) : {
    auto *index_size = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorGetSize(index_size, iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorScanKey) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorScanKey(iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorScanAscending) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    auto scan_type = frame->LocalAt<storage::index::ScanType>(READ_LOCAL_ID());
    auto limit = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpIndexIteratorScanAscending(iter, scan_type, limit);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorScanDescending) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorScanDescending(iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorScanLimitDescending) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    auto limit = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    OpIndexIteratorScanLimitDescending(iter, limit);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorFree) : {
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorFree(iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorAdvance) : {
    auto *has_more = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorAdvance(has_more, iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorGetPR) : {
    auto *pr = frame->LocalAt<storage::ProjectedRow **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorGetPR(pr, iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorGetLoPR) : {
    auto *pr = frame->LocalAt<storage::ProjectedRow **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorGetLoPR(pr, iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorGetHiPR) : {
    auto *pr = frame->LocalAt<storage::ProjectedRow **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorGetHiPR(pr, iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorGetTablePR) : {
    auto *pr = frame->LocalAt<storage::ProjectedRow **>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorGetTablePR(pr, iter);
    DISPATCH_NEXT();
  }

  OP(IndexIteratorGetSlot) : {
    auto *slot = frame->LocalAt<storage::TupleSlot *>(READ_LOCAL_ID());
    auto *iter = frame->LocalAt<sql::IndexIterator *>(READ_LOCAL_ID());
    OpIndexIteratorGetSlot(slot, iter);
    DISPATCH_NEXT();
  }

  OP(AbortTxn) : {
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpAbortTxn(exec_ctx);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // PR Calls
  // -------------------------------------------------------

#define GEN_PR_ACCESS(type_str, type)                                    \
  OP(PRGet##type_str) : {                                                \
    auto *result = frame->LocalAt<type *>(READ_LOCAL_ID());              \
    auto *pr = frame->LocalAt<storage::ProjectedRow *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM2();                                         \
    OpPRGet##type_str(result, pr, col_idx);                              \
    DISPATCH_NEXT();                                                     \
  }                                                                      \
  OP(PRGet##type_str##Null) : {                                          \
    auto *result = frame->LocalAt<type *>(READ_LOCAL_ID());              \
    auto *pr = frame->LocalAt<storage::ProjectedRow *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM2();                                         \
    OpPRGet##type_str##Null(result, pr, col_idx);                        \
    DISPATCH_NEXT();                                                     \
  }
  GEN_PR_ACCESS(Bool, sql::BoolVal)
  GEN_PR_ACCESS(TinyInt, sql::Integer)
  GEN_PR_ACCESS(SmallInt, sql::Integer)
  GEN_PR_ACCESS(Int, sql::Integer)
  GEN_PR_ACCESS(BigInt, sql::Integer)
  GEN_PR_ACCESS(Real, sql::Real)
  GEN_PR_ACCESS(Double, sql::Real)
  GEN_PR_ACCESS(DateVal, sql::DateVal)
  GEN_PR_ACCESS(TimestampVal, sql::TimestampVal)
  GEN_PR_ACCESS(Varlen, sql::StringVal)
#undef GEN_PR_ACCESS

#define GEN_PR_SET(type_str, type)                                       \
  OP(PRSet##type_str) : {                                                \
    auto *pr = frame->LocalAt<storage::ProjectedRow *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM2();                                         \
    auto val = frame->LocalAt<type *>(READ_LOCAL_ID());                  \
    OpPRSet##type_str(pr, col_idx, val);                                 \
    DISPATCH_NEXT();                                                     \
  }                                                                      \
  OP(PRSet##type_str##Null) : {                                          \
    auto *pr = frame->LocalAt<storage::ProjectedRow *>(READ_LOCAL_ID()); \
    auto col_idx = READ_UIMM2();                                         \
    auto val = frame->LocalAt<type *>(READ_LOCAL_ID());                  \
    OpPRSet##type_str##Null(pr, col_idx, val);                           \
    DISPATCH_NEXT();                                                     \
  }
  GEN_PR_SET(Bool, sql::BoolVal)
  GEN_PR_SET(TinyInt, sql::Integer)
  GEN_PR_SET(SmallInt, sql::Integer)
  GEN_PR_SET(Int, sql::Integer)
  GEN_PR_SET(BigInt, sql::Integer)
  GEN_PR_SET(Real, sql::Real)
  GEN_PR_SET(Double, sql::Real)
  GEN_PR_SET(DateVal, sql::DateVal)
  GEN_PR_SET(TimestampVal, sql::TimestampVal)
#undef GEN_PR_SET

  OP(PRSetVarlen) : {
    auto *pr = frame->LocalAt<storage::ProjectedRow *>(READ_LOCAL_ID());
    auto col_idx = READ_UIMM2();
    auto val = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto own = frame->LocalAt<bool>(READ_LOCAL_ID());
    OpPRSetVarlen(pr, col_idx, val, own);
    DISPATCH_NEXT();
  }

  OP(PRSetVarlenNull) : {
    auto *pr = frame->LocalAt<storage::ProjectedRow *>(READ_LOCAL_ID());
    auto col_idx = READ_UIMM2();
    auto val = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto own = frame->LocalAt<bool>(READ_LOCAL_ID());
    OpPRSetVarlenNull(pr, col_idx, val, own);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // StorageInterface Calls
  // -------------------------------------------------------

  OP(StorageInterfaceInit) : {
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto table_oid = frame->LocalAt<uint32_t>(READ_LOCAL_ID());
    auto *col_oids = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto num_oids = READ_UIMM4();
    auto need_indexes = frame->LocalAt<bool>(READ_LOCAL_ID());

    OpStorageInterfaceInit(storage_interface, exec_ctx, table_oid, col_oids, num_oids, need_indexes);
    DISPATCH_NEXT();
  }

  OP(StorageInterfaceGetTablePR) : {
    auto *pr_result = frame->LocalAt<storage::ProjectedRow **>(READ_LOCAL_ID());
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());

    OpStorageInterfaceGetTablePR(pr_result, storage_interface);
    DISPATCH_NEXT();
  }

  OP(StorageInterfaceTableInsert) : {
    auto *tuple_slot = frame->LocalAt<storage::TupleSlot *>(READ_LOCAL_ID());
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());

    OpStorageInterfaceTableInsert(tuple_slot, storage_interface);
    DISPATCH_NEXT();
  }

  OP(StorageInterfaceTableDelete) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());
    auto *tuple_slot = frame->LocalAt<storage::TupleSlot *>(READ_LOCAL_ID());

    OpStorageInterfaceTableDelete(result, storage_interface, tuple_slot);
    DISPATCH_NEXT();
  }

  OP(StorageInterfaceTableUpdate) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());
    auto *tuple_slot = frame->LocalAt<storage::TupleSlot *>(READ_LOCAL_ID());

    OpStorageInterfaceTableUpdate(result, storage_interface, tuple_slot);
    DISPATCH_NEXT();
  }

  OP(StorageInterfaceGetIndexPR) : {
    auto *pr_result = frame->LocalAt<storage::ProjectedRow **>(READ_LOCAL_ID());
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());
    auto index_oid = frame->LocalAt<uint32_t>(READ_LOCAL_ID());

    OpStorageInterfaceGetIndexPR(pr_result, storage_interface, index_oid);
    DISPATCH_NEXT();
  }

  OP(StorageInterfaceIndexGetSize) : {
    auto *result = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());
    OpStorageInterfaceIndexGetSize(result, storage_interface);
    DISPATCH_NEXT();
  }

  OP(StorageInterfaceGetIndexHeapSize) : {
    auto *size = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());
    OpStorageInterfaceGetIndexHeapSize(size, storage_interface);
    DISPATCH_NEXT();
  }

  OP(StorageInterfaceIndexInsert) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());
    OpStorageInterfaceIndexInsert(result, storage_interface);
    DISPATCH_NEXT();
  }

  OP(StorageInterfaceIndexInsertUnique) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());
    OpStorageInterfaceIndexInsertUnique(result, storage_interface);
    DISPATCH_NEXT();
  }

  OP(StorageInterfaceIndexInsertWithSlot) : {
    auto *result = frame->LocalAt<bool *>(READ_LOCAL_ID());
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());
    auto *tuple_slot = frame->LocalAt<storage::TupleSlot *>(READ_LOCAL_ID());
    auto unique = frame->LocalAt<bool>(READ_LOCAL_ID());
    OpStorageInterfaceIndexInsertWithSlot(result, storage_interface, tuple_slot, unique);
    DISPATCH_NEXT();
  }

  OP(StorageInterfaceIndexDelete) : {
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());
    auto *tuple_slot = frame->LocalAt<storage::TupleSlot *>(READ_LOCAL_ID());
    OpStorageInterfaceIndexDelete(storage_interface, tuple_slot);
    DISPATCH_NEXT();
  }

  OP(StorageInterfaceFree) : {
    auto *storage_interface = frame->LocalAt<sql::StorageInterface *>(READ_LOCAL_ID());
    OpStorageInterfaceFree(storage_interface);
    DISPATCH_NEXT();
  }

  // ----------------------------
  // Parameter calls
  // -----------------------------
#define GEN_PARAM_GET(Name, SqlType)                                            \
  OP(GetParam##Name) : {                                                        \
    auto *ret = frame->LocalAt<sql::SqlType *>(READ_LOCAL_ID());                \
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID()); \
    auto param_idx = frame->LocalAt<uint32_t>(READ_LOCAL_ID());                 \
    OpGetParam##Name(ret, exec_ctx, param_idx);                                 \
    DISPATCH_NEXT();                                                            \
  }

  GEN_PARAM_GET(Bool, BoolVal)
  GEN_PARAM_GET(TinyInt, Integer)
  GEN_PARAM_GET(SmallInt, Integer)
  GEN_PARAM_GET(Int, Integer)
  GEN_PARAM_GET(BigInt, Integer)
  GEN_PARAM_GET(Real, Real)
  GEN_PARAM_GET(Double, Real)
  GEN_PARAM_GET(DateVal, DateVal)
  GEN_PARAM_GET(TimestampVal, TimestampVal)
  GEN_PARAM_GET(String, StringVal)
#undef GEN_PARAM_GET

  // -------------------------------------------------------
  // Trig functions
  // -------------------------------------------------------

  OP(Pi) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpPi(result);
    DISPATCH_NEXT();
  }

  OP(E) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    OpE(result);
    DISPATCH_NEXT();
  }

#define UNARY_REAL_MATH_OP(TOP)                                       \
  OP(TOP) : {                                                         \
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());      \
    auto *input = frame->LocalAt<const sql::Real *>(READ_LOCAL_ID()); \
    Op##TOP(result, input);                                           \
    DISPATCH_NEXT();                                                  \
  }

#define BINARY_REAL_MATH_OP(TOP)                                       \
  OP(TOP) : {                                                          \
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());       \
    auto *input1 = frame->LocalAt<const sql::Real *>(READ_LOCAL_ID()); \
    auto *input2 = frame->LocalAt<const sql::Real *>(READ_LOCAL_ID()); \
    Op##TOP(result, input1, input2);                                   \
    DISPATCH_NEXT();                                                   \
  }

  UNARY_REAL_MATH_OP(Sin);
  UNARY_REAL_MATH_OP(Asin);
  UNARY_REAL_MATH_OP(Cos);
  UNARY_REAL_MATH_OP(Acos);
  UNARY_REAL_MATH_OP(Tan);
  UNARY_REAL_MATH_OP(Cot);
  UNARY_REAL_MATH_OP(Atan);
  UNARY_REAL_MATH_OP(Cosh);
  UNARY_REAL_MATH_OP(Tanh);
  UNARY_REAL_MATH_OP(Sinh);
  UNARY_REAL_MATH_OP(Sqrt);
  UNARY_REAL_MATH_OP(Cbrt);
  UNARY_REAL_MATH_OP(Exp);
  UNARY_REAL_MATH_OP(Ceil);
  UNARY_REAL_MATH_OP(Floor);
  UNARY_REAL_MATH_OP(Truncate);
  UNARY_REAL_MATH_OP(Ln);
  UNARY_REAL_MATH_OP(Log2);
  UNARY_REAL_MATH_OP(Log10);
  UNARY_REAL_MATH_OP(Sign);
  UNARY_REAL_MATH_OP(Radians);
  UNARY_REAL_MATH_OP(Degrees);
  UNARY_REAL_MATH_OP(Round);
  BINARY_REAL_MATH_OP(Atan2);
  BINARY_REAL_MATH_OP(Log);
  BINARY_REAL_MATH_OP(Pow);

  OP(Round2) : {
    auto *result = frame->LocalAt<sql::Real *>(READ_LOCAL_ID());
    auto *v = frame->LocalAt<const sql::Real *>(READ_LOCAL_ID());
    auto *precision = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    OpRound2(result, v, precision);
    DISPATCH_NEXT();
  }

#undef BINARY_REAL_MATH_OP
#undef UNARY_REAL_MATH_OP

  // -------------------------------------------------------
  // Mini runners functions
  // -------------------------------------------------------

  OP(NpRunnersEmitInt) : {
    auto *ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *num_tuple = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    auto *num_col = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    auto *num_int = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    auto *num_real = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    OpNpRunnersEmitInt(ctx, num_tuple, num_col, num_int, num_real);
    DISPATCH_NEXT();
  }

  OP(NpRunnersEmitReal) : {
    auto *ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *num_tuple = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    auto *num_col = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    auto *num_int = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    auto *num_real = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    OpNpRunnersEmitReal(ctx, num_tuple, num_col, num_int, num_real);
    DISPATCH_NEXT();
  }

  OP(NpRunnersDummyInt) : {
    auto *ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpNpRunnersDummyInt(ctx);
    DISPATCH_NEXT();
  }

  OP(NpRunnersDummyReal) : {
    auto *ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpNpRunnersDummyReal(ctx);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // String functions
  // -------------------------------------------------------
  OP(Chr) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    OpChr(result, exec_ctx, input);
    DISPATCH_NEXT();
  }

  OP(CharLength) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpCharLength(result, exec_ctx, input);
    DISPATCH_NEXT();
  }

  OP(ASCII) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpASCII(result, exec_ctx, input);
    DISPATCH_NEXT();
  }

  OP(Concat) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto **inputs = frame->LocalAt<const sql::StringVal **>(READ_LOCAL_ID());
    auto num_inputs = READ_UIMM4();
    OpConcat(result, exec_ctx, inputs, num_inputs);
    DISPATCH_NEXT();
  }

  OP(Left) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *n = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    OpLeft(result, exec_ctx, input, n);
    DISPATCH_NEXT();
  }

  OP(Length) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpLength(result, exec_ctx, input);
    DISPATCH_NEXT();
  }

  OP(Like) : {
    auto *result = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *pattern = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpLike(result, input, pattern);
    DISPATCH_NEXT();
  }

  OP(Lower) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpLower(result, exec_ctx, input);
    DISPATCH_NEXT();
  }

  OP(Position) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *search_str = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *search_sub_str = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpPosition(result, exec_ctx, search_str, search_sub_str);
    DISPATCH_NEXT();
  }

  OP(LPad3Arg) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *n = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    auto *chars = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpLPad3Arg(result, exec_ctx, input, n, chars);
    DISPATCH_NEXT();
  }

  OP(LPad2Arg) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *n = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    OpLPad2Arg(result, exec_ctx, input, n);
    DISPATCH_NEXT();
  }

  OP(LTrim2Arg) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *chars = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpLTrim2Arg(result, exec_ctx, input, chars);
    DISPATCH_NEXT();
  }

  OP(LTrim1Arg) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpLTrim1Arg(result, exec_ctx, input);
    DISPATCH_NEXT();
  }

  OP(Repeat) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *n = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    OpRepeat(result, exec_ctx, input, n);
    DISPATCH_NEXT();
  }

  OP(Reverse) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpReverse(result, exec_ctx, input);
    DISPATCH_NEXT();
  }

  OP(Right) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *n = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    OpRight(result, exec_ctx, input, n);
    DISPATCH_NEXT();
  }

  OP(RPad3Arg) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *n = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    auto *chars = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpRPad3Arg(result, exec_ctx, input, n, chars);
    DISPATCH_NEXT();
  }

  OP(RPad2Arg) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *n = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    OpRPad2Arg(result, exec_ctx, input, n);
    DISPATCH_NEXT();
  }

  OP(RTrim2Arg) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *chars = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpRTrim2Arg(result, exec_ctx, input, chars);
    DISPATCH_NEXT();
  }

  OP(RTrim1Arg) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpRTrim1Arg(result, exec_ctx, input);
    DISPATCH_NEXT();
  }

  OP(SplitPart) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *str = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *delim = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *field = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    OpSplitPart(result, exec_ctx, str, delim, field);
    DISPATCH_NEXT();
  }

  OP(Substring) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *str = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *pos = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    auto *len = frame->LocalAt<const sql::Integer *>(READ_LOCAL_ID());
    OpSubstring(result, exec_ctx, str, pos, len);
    DISPATCH_NEXT();
  }

  OP(StartsWith) : {
    auto *result = frame->LocalAt<sql::BoolVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *str = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *start = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpStartsWith(result, exec_ctx, str, start);
    DISPATCH_NEXT();
  }

  OP(Trim) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *str = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpTrim(result, exec_ctx, str);
    DISPATCH_NEXT();
  }

  OP(Trim2) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *str = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    auto *chars = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpTrim2(result, exec_ctx, str, chars);
    DISPATCH_NEXT();
  }

  OP(Upper) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *str = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpUpper(result, exec_ctx, str);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Date functions
  // -------------------------------------------------------

  OP(ExtractYearFromDate) : {
    auto *result = frame->LocalAt<sql::Integer *>(READ_LOCAL_ID());
    auto *input = frame->LocalAt<sql::DateVal *>(READ_LOCAL_ID());
    OpExtractYearFromDate(result, input);
    DISPATCH_NEXT();
  }

  // -------------------------------------------------------
  // Testing only functions
  // -------------------------------------------------------

  OP(TestCatalogLookup) : {
    auto *oid_var = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *table_name = module_->GetBytecodeModule()->AccessStaticLocalDataRaw(LocalVar::Decode(READ_STATIC_LOCAL_ID()));
    auto table_name_len = READ_IMM4();
    auto *col_name = module_->GetBytecodeModule()->AccessStaticLocalDataRaw(LocalVar::Decode(READ_STATIC_LOCAL_ID()));
    auto col_name_len = READ_IMM4();
    OpTestCatalogLookup(oid_var, exec_ctx, table_name, table_name_len, col_name, col_name_len);
    DISPATCH_NEXT();
  }

  OP(TestCatalogIndexLookup) : {
    auto *oid_var = frame->LocalAt<uint32_t *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *table_name = module_->GetBytecodeModule()->AccessStaticLocalDataRaw(LocalVar::Decode(READ_STATIC_LOCAL_ID()));
    auto table_name_len = READ_IMM4();
    OpTestCatalogIndexLookup(oid_var, exec_ctx, table_name, table_name_len);
    DISPATCH_NEXT();
  }

  OP(Version) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    OpVersion(exec_ctx, result);
    DISPATCH_NEXT();
  }

  OP(InitCap) : {
    auto *result = frame->LocalAt<sql::StringVal *>(READ_LOCAL_ID());
    auto *exec_ctx = frame->LocalAt<exec::ExecutionContext *>(READ_LOCAL_ID());
    auto *str = frame->LocalAt<const sql::StringVal *>(READ_LOCAL_ID());
    OpInitCap(result, exec_ctx, str);
    DISPATCH_NEXT();
  }

  // Impossible
  UNREACHABLE("Impossible to reach end of interpreter loop. Bad code!");
}  // NOLINT(readability/fn_size)

const uint8_t *VM::ExecuteCall(const uint8_t *ip, VM::Frame *caller) {
  // Read the function ID and the argument count to the function first
  const uint16_t func_id = READ_FUNC_ID();   // NOLINT (something wrong with clang tidy)
  const uint16_t num_params = READ_UIMM2();  // NOLINT (something wrong with clang tidy)

  // Lookup the function
  const FunctionInfo *func_info = module_->GetFuncInfoById(func_id);
  TERRIER_ASSERT(func_info != nullptr, "Function doesn't exist in module!");
  const std::size_t frame_size = func_info->GetFrameSize();

  // Get some space for the function's frame
  bool used_heap = false;
  uint8_t *raw_frame = nullptr;
  if (frame_size > MAX_STACK_ALLOC_SIZE) {
    used_heap = true;
    raw_frame = static_cast<uint8_t *>(util::Memory::MallocAligned(frame_size, alignof(uint64_t)));
  } else if (frame_size > SOFT_MAX_STACK_ALLOC_SIZE) {
    // TODO(pmenon): Check stack before allocation
    raw_frame = static_cast<uint8_t *>(alloca(frame_size));
  } else {
    raw_frame = static_cast<uint8_t *>(alloca(frame_size));
  }

  // Set up the arguments to the function
  for (uint32_t i = 0; i < num_params; i++) {
    const LocalInfo &param_info = func_info->GetLocals()[i];
    const LocalVar param = LocalVar::Decode(READ_LOCAL_ID());
    const void *param_ptr = caller->PtrToLocalAt(param);
    if (param.GetAddressMode() == LocalVar::AddressMode::Address) {
      std::memcpy(raw_frame + param_info.GetOffset(), &param_ptr, param_info.GetSize());
    } else {
      std::memcpy(raw_frame + param_info.GetOffset(), param_ptr, param_info.GetSize());
    }
  }

  // Let's go
  Frame callee(raw_frame, func_info->GetFrameSize());
  Interpret(module_->GetBytecodeModule()->AccessBytecodeForFunctionRaw(*func_info), &callee);

  // Done. Now, let's cleanup.
  if (used_heap) {
    std::free(raw_frame);
  }

  return ip;
}

}  // namespace terrier::execution::vm
