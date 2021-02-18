#pragma once

#include <cstdint>
#include <string>

#include "catalog/catalog_accessor.h"
#include "common/macros.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/aggregation_hash_table.h"
#include "execution/sql/aggregators.h"
#include "execution/sql/filter_manager.h"
#include "execution/sql/functions/arithmetic_functions.h"
#include "execution/sql/functions/casting_functions.h"
#include "execution/sql/functions/comparison_functions.h"
#include "execution/sql/functions/is_null_predicate.h"
#include "execution/sql/functions/runners_functions.h"
#include "execution/sql/functions/string_functions.h"
#include "execution/sql/functions/system_functions.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/join_hash_table.h"
#include "execution/sql/operators/hash_operators.h"
#include "execution/sql/sorter.h"
#include "execution/sql/sql_def.h"
#include "execution/sql/storage_interface.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql/thread_state_container.h"
#include "execution/sql/vector_filter_executor.h"
#include "metrics/metrics_manager.h"
#include "parser/expression/constant_value_expression.h"

// #include "execution/util/csv_reader.h" Fix later.

// All VM bytecode op handlers must use this macro
#define VM_OP EXPORT

// VM bytecodes that are hot and should be inlined should use this macro
#define VM_OP_HOT VM_OP ALWAYS_INLINE inline
#define VM_OP_WARM VM_OP inline
#define VM_OP_COLD VM_OP NEVER_INLINE

extern "C" {

// ---------------------------------------------------------
// Primitive comparisons
// ---------------------------------------------------------

#define COMPARISONS(type, ...)                                                                             \
  /* Primitive greater-than-equal implementation */                                                        \
  VM_OP_HOT void OpGreaterThanEqual##_##type(bool *result, type lhs, type rhs) { *result = (lhs >= rhs); } \
                                                                                                           \
  /* Primitive greater-than implementation */                                                              \
  VM_OP_HOT void OpGreaterThan##_##type(bool *result, type lhs, type rhs) { *result = (lhs > rhs); }       \
                                                                                                           \
  /* Primitive equal-to implementation */                                                                  \
  VM_OP_HOT void OpEqual##_##type(bool *result, type lhs, type rhs) { *result = (lhs == rhs); }            \
                                                                                                           \
  /* Primitive less-than-equal implementation */                                                           \
  VM_OP_HOT void OpLessThanEqual##_##type(bool *result, type lhs, type rhs) { *result = (lhs <= rhs); }    \
                                                                                                           \
  /* Primitive less-than implementation */                                                                 \
  VM_OP_HOT void OpLessThan##_##type(bool *result, type lhs, type rhs) { *result = (lhs < rhs); }          \
                                                                                                           \
  /* Primitive not-equal-to implementation */                                                              \
  VM_OP_HOT void OpNotEqual##_##type(bool *result, type lhs, type rhs) { *result = (lhs != rhs); }

ALL_TYPES(COMPARISONS);

#undef COMPARISONS

VM_OP_HOT void OpNot(bool *const result, const bool input) { *result = !input; }

VM_OP_HOT void OpNotSql(noisepage::execution::sql::BoolVal *const result,
                        const noisepage::execution::sql::BoolVal *input) {
  noisepage::execution::sql::ComparisonFunctions::NotBoolVal(result, *input);
}

// ---------------------------------------------------------
// Primitive arithmetic
// ---------------------------------------------------------

#define ARITHMETIC(type, ...)                                                              \
  /* Primitive addition */                                                                 \
  VM_OP_HOT void OpAdd##_##type(type *result, type lhs, type rhs) { *result = lhs + rhs; } \
                                                                                           \
  /* Primitive subtraction */                                                              \
  VM_OP_HOT void OpSub##_##type(type *result, type lhs, type rhs) { *result = lhs - rhs; } \
                                                                                           \
  /* Primitive multiplication */                                                           \
  VM_OP_HOT void OpMul##_##type(type *result, type lhs, type rhs) { *result = lhs * rhs; } \
                                                                                           \
  /* Primitive negation */                                                                 \
  VM_OP_HOT void OpNeg##_##type(type *result, type input) { *result = -input; }            \
                                                                                           \
  /* Primitive division (no zero-check) */                                                 \
  VM_OP_HOT void OpDiv##_##type(type *result, type lhs, type rhs) {                        \
    NOISEPAGE_ASSERT(rhs != 0, "Division-by-zero error!");                                 \
    *result = lhs / rhs;                                                                   \
  }

ALL_NUMERIC_TYPES(ARITHMETIC);

#undef ARITHMETIC

#define INT_MODULAR(type, ...)                                      \
  /* Primitive modulo-remainder (no zero-check) */                  \
  VM_OP_HOT void OpMod##_##type(type *result, type lhs, type rhs) { \
    NOISEPAGE_ASSERT(rhs != 0, "Division-by-zero error!");          \
    *result = lhs % rhs;                                            \
  }

#define FLOAT_MODULAR(type, ...)                                    \
  /* Primitive modulo-remainder (no zero-check) */                  \
  VM_OP_HOT void OpMod##_##type(type *result, type lhs, type rhs) { \
    NOISEPAGE_ASSERT(rhs != 0, "Division-by-zero error!");          \
    *result = std::fmod(lhs, rhs);                                  \
  }

INT_TYPES(INT_MODULAR)
FLOAT_TYPES(FLOAT_MODULAR)

#undef FLOAT_MODULAR
#undef INT_MODULAR

// ---------------------------------------------------------
// Bitwise operations
// ---------------------------------------------------------

#define BITS(type, ...)                                                                                          \
  /* Primitive bitwise AND */                                                                                    \
  VM_OP_HOT void OpBitAnd##_##type(type *result, type lhs, type rhs) { *result = static_cast<type>(lhs & rhs); } \
                                                                                                                 \
  /* Primitive bitwise OR */                                                                                     \
  VM_OP_HOT void OpBitOr##_##type(type *result, type lhs, type rhs) { *result = static_cast<type>(lhs | rhs); }  \
                                                                                                                 \
  /* Primitive bitwise XOR */                                                                                    \
  VM_OP_HOT void OpBitXor##_##type(type *result, type lhs, type rhs) { *result = static_cast<type>(lhs ^ rhs); } \
                                                                                                                 \
  /* Primitive bitwise COMPLEMENT */                                                                             \
  VM_OP_HOT void OpBitNeg##_##type(type *result, type input) { *result = static_cast<type>(~input); }

INT_TYPES(BITS);

#undef BITS

// ---------------------------------------------------------
// Memory operations
// ---------------------------------------------------------

VM_OP_HOT void OpIsNullPtr(bool *result, const void *const ptr) { *result = (ptr == nullptr); }

VM_OP_HOT void OpIsNotNullPtr(bool *result, const void *const ptr) { *result = (ptr != nullptr); }

VM_OP_HOT void OpDeref1(int8_t *dest, const int8_t *const src) { *dest = *src; }

VM_OP_HOT void OpDeref2(int16_t *dest, const int16_t *const src) { *dest = *src; }

VM_OP_HOT void OpDeref4(int32_t *dest, const int32_t *const src) { *dest = *src; }

VM_OP_HOT void OpDeref8(int64_t *dest, const int64_t *const src) { *dest = *src; }

VM_OP_HOT void OpDerefN(noisepage::byte *dest, const noisepage::byte *const src, uint32_t len) {
  std::memcpy(dest, src, len);
}

VM_OP_HOT void OpAssign1(int8_t *dest, int8_t src) { *dest = src; }

VM_OP_HOT void OpAssign2(int16_t *dest, int16_t src) { *dest = src; }

VM_OP_HOT void OpAssign4(int32_t *dest, int32_t src) { *dest = src; }

VM_OP_HOT void OpAssign8(int64_t *dest, int64_t src) { *dest = src; }

VM_OP_HOT void OpAssignImm1(int8_t *dest, int8_t src) { *dest = src; }

VM_OP_HOT void OpAssignImm2(int16_t *dest, int16_t src) { *dest = src; }

VM_OP_HOT void OpAssignImm4(int32_t *dest, int32_t src) { *dest = src; }

VM_OP_HOT void OpAssignImm8(int64_t *dest, int64_t src) { *dest = src; }

VM_OP_HOT void OpAssignImm4F(float *dest, float src) { *dest = src; }

VM_OP_HOT void OpAssignImm8F(double *dest, double src) { *dest = src; }

VM_OP_HOT void OpLea(noisepage::byte **dest, noisepage::byte *base, uint32_t offset) { *dest = base + offset; }

VM_OP_HOT void OpLeaScaled(noisepage::byte **dest, noisepage::byte *base, uint32_t index, uint32_t scale,
                           uint32_t offset) {
  *dest = base + (scale * index) + offset;
}

VM_OP_HOT bool OpJump() { return true; }

VM_OP_HOT bool OpJumpIfTrue(bool cond) { return cond; }

VM_OP_HOT bool OpJumpIfFalse(bool cond) { return !cond; }

VM_OP_HOT void OpCall(UNUSED_ATTRIBUTE uint16_t func_id, UNUSED_ATTRIBUTE uint16_t num_args) {}

VM_OP_HOT void OpReturn() {}

// ---------------------------------------------------------
// Execution Context
// ---------------------------------------------------------

VM_OP_HOT void OpExecutionContextAddRowsAffected(noisepage::execution::exec::ExecutionContext *exec_ctx,
                                                 int64_t rows_affected) {
  exec_ctx->AddRowsAffected(rows_affected);
}

VM_OP_COLD void OpExecutionContextRegisterHook(noisepage::execution::exec::ExecutionContext *exec_ctx,
                                               uint32_t hook_idx,
                                               noisepage::execution::exec::ExecutionContext::HookFn hook);

VM_OP_COLD void OpExecutionContextClearHooks(noisepage::execution::exec::ExecutionContext *exec_ctx);

VM_OP_COLD void OpExecutionContextInitHooks(noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t num_hooks);

VM_OP_WARM void OpExecutionContextGetMemoryPool(noisepage::execution::sql::MemoryPool **const memory,
                                                noisepage::execution::exec::ExecutionContext *const exec_ctx) {
  *memory = exec_ctx->GetMemoryPool();
}

VM_OP_HOT void OpExecutionContextStartResourceTracker(noisepage::execution::exec::ExecutionContext *const exec_ctx,
                                                      noisepage::metrics::MetricsComponent component) {
  exec_ctx->StartResourceTracker(component);
}

VM_OP_COLD void OpExecutionContextSetMemoryUseOverride(noisepage::execution::exec::ExecutionContext *exec_ctx,
                                                       uint32_t memory_use);

VM_OP_HOT void OpExecutionContextEndResourceTracker(noisepage::execution::exec::ExecutionContext *exec_ctx,
                                                    const noisepage::execution::sql::StringVal &name) {
  exec_ctx->EndResourceTracker(name.GetContent(), name.GetLength());
}

VM_OP_COLD void OpExecutionContextStartPipelineTracker(noisepage::execution::exec::ExecutionContext *exec_ctx,
                                                       noisepage::execution::pipeline_id_t pipeline_id);

VM_OP_COLD void OpExecutionContextEndPipelineTracker(noisepage::execution::exec::ExecutionContext *exec_ctx,
                                                     noisepage::execution::query_id_t query_id,
                                                     noisepage::execution::pipeline_id_t pipeline_id,
                                                     noisepage::selfdriving::ExecOUFeatureVector *ouvec);

VM_OP_COLD void OpExecOUFeatureVectorRecordFeature(
    noisepage::selfdriving::ExecOUFeatureVector *ouvec, noisepage::execution::pipeline_id_t pipeline_id,
    noisepage::execution::feature_id_t feature_id,
    noisepage::selfdriving::ExecutionOperatingUnitFeatureAttribute feature_attribute,
    noisepage::selfdriving::ExecutionOperatingUnitFeatureUpdateMode mode, uint32_t value);

VM_OP_COLD void OpExecOUFeatureVectorInitialize(noisepage::execution::exec::ExecutionContext *exec_ctx,
                                                noisepage::selfdriving::ExecOUFeatureVector *ouvec,
                                                noisepage::execution::pipeline_id_t pipeline_id, bool is_parallel);

VM_OP_COLD void OpExecOUFeatureVectorFilter(noisepage::selfdriving::ExecOUFeatureVector *ouvec,
                                            noisepage::selfdriving::ExecutionOperatingUnitType filter);

VM_OP_COLD void OpExecOUFeatureVectorReset(noisepage::selfdriving::ExecOUFeatureVector *ouvec);

VM_OP_WARM
void OpExecutionContextGetTLS(noisepage::execution::sql::ThreadStateContainer **const thread_state_container,
                              noisepage::execution::exec::ExecutionContext *const exec_ctx) {
  *thread_state_container = exec_ctx->GetThreadStateContainer();
}

VM_OP_WARM void OpThreadStateContainerAccessCurrentThreadState(
    noisepage::byte **state, noisepage::execution::sql::ThreadStateContainer *thread_state_container) {
  *state = thread_state_container->AccessCurrentThreadState();
}

VM_OP_WARM void OpThreadStateContainerReset(noisepage::execution::sql::ThreadStateContainer *thread_state_container,
                                            const uint32_t size,
                                            noisepage::execution::sql::ThreadStateContainer::InitFn init_fn,
                                            noisepage::execution::sql::ThreadStateContainer::DestroyFn destroy_fn,
                                            void *ctx) {
  thread_state_container->Reset(size, init_fn, destroy_fn, ctx);
}

VM_OP_WARM void OpThreadStateContainerIterate(noisepage::execution::sql::ThreadStateContainer *thread_state_container,
                                              void *const state,
                                              noisepage::execution::sql::ThreadStateContainer::IterateFn iterate_fn) {
  thread_state_container->IterateStates(state, iterate_fn);
}

VM_OP_WARM void OpThreadStateContainerClear(noisepage::execution::sql::ThreadStateContainer *thread_state_container) {
  thread_state_container->Clear();
}

VM_OP_COLD void OpRegisterThreadWithMetricsManager(noisepage::execution::exec::ExecutionContext *exec_ctx);

VM_OP_COLD void OpEnsureTrackersStopped(noisepage::execution::exec::ExecutionContext *exec_ctx);

VM_OP_COLD void OpAggregateMetricsThread(noisepage::execution::exec::ExecutionContext *exec_ctx);

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

VM_OP void OpTableVectorIteratorInit(noisepage::execution::sql::TableVectorIterator *iter,
                                     noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t table_oid,
                                     uint32_t *col_oids, uint32_t num_oids);

VM_OP void OpTableVectorIteratorPerformInit(noisepage::execution::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorNext(bool *has_more, noisepage::execution::sql::TableVectorIterator *iter) {
  *has_more = iter->Advance();
}

VM_OP void OpTableVectorIteratorFree(noisepage::execution::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorGetVPINumTuples(uint32_t *result,
                                                    noisepage::execution::sql::TableVectorIterator *iter) {
  // TODO(WAN): result should be uint64_t, see #1049
  *result = iter->GetVectorProjectionIteratorNumTuples();
}

VM_OP_HOT void OpTableVectorIteratorGetVPI(noisepage::execution::sql::VectorProjectionIterator **vpi,
                                           noisepage::execution::sql::TableVectorIterator *iter) {
  *vpi = iter->GetVectorProjectionIterator();
}

VM_OP_HOT void OpParallelScanTable(uint32_t table_oid, uint32_t *col_oids, uint32_t num_oids, void *const query_state,
                                   noisepage::execution::exec::ExecutionContext *exec_ctx,
                                   const noisepage::execution::sql::TableVectorIterator::ScanFn scanner) {
  noisepage::execution::sql::TableVectorIterator::ParallelScan(table_oid, col_oids, num_oids, query_state, exec_ctx,
                                                               scanner);
}

// ---------------------------------------------------------
// Vector Projection Iterator
// ---------------------------------------------------------

VM_OP void OpVPIInit(noisepage::execution::sql::VectorProjectionIterator *vpi,
                     noisepage::execution::sql::VectorProjection *vp);

VM_OP void OpVPIInitWithList(noisepage::execution::sql::VectorProjectionIterator *vpi,
                             noisepage::execution::sql::VectorProjection *vp,
                             noisepage::execution::sql::TupleIdList *tid_list);

VM_OP void OpVPIFree(noisepage::execution::sql::VectorProjectionIterator *vpi);

VM_OP_HOT void OpVPIIsFiltered(bool *is_filtered, const noisepage::execution::sql::VectorProjectionIterator *vpi) {
  *is_filtered = vpi->IsFiltered();
}

VM_OP_HOT void OpVPIGetSelectedRowCount(uint32_t *count,
                                        const noisepage::execution::sql::VectorProjectionIterator *vpi) {
  *count = vpi->GetSelectedTupleCount();
}

VM_OP_HOT void OpVPIGetVectorProjection(noisepage::execution::sql::VectorProjection **vector_projection,
                                        const noisepage::execution::sql::VectorProjectionIterator *vpi) {
  *vector_projection = vpi->GetVectorProjection();
}

VM_OP_HOT void OpVPIHasNext(bool *has_more, const noisepage::execution::sql::VectorProjectionIterator *vpi) {
  *has_more = vpi->HasNext();
}

VM_OP_HOT void OpVPIHasNextFiltered(bool *has_more, const noisepage::execution::sql::VectorProjectionIterator *vpi) {
  *has_more = vpi->HasNextFiltered();
}

VM_OP_HOT void OpVPIAdvance(noisepage::execution::sql::VectorProjectionIterator *vpi) { vpi->Advance(); }

VM_OP_HOT void OpVPIAdvanceFiltered(noisepage::execution::sql::VectorProjectionIterator *vpi) {
  vpi->AdvanceFiltered();
}

VM_OP_HOT void OpVPISetPosition(noisepage::execution::sql::VectorProjectionIterator *vpi, const uint32_t index) {
  vpi->SetPosition<false>(index);
}

VM_OP_HOT void OpVPISetPositionFiltered(noisepage::execution::sql::VectorProjectionIterator *const vpi,
                                        const uint32_t index) {
  vpi->SetPosition<true>(index);
}

VM_OP_HOT void OpVPIMatch(noisepage::execution::sql::VectorProjectionIterator *vpi, const bool match) {
  vpi->Match(match);
}

VM_OP_HOT void OpVPIReset(noisepage::execution::sql::VectorProjectionIterator *vpi) { vpi->Reset(); }

VM_OP_HOT void OpVPIResetFiltered(noisepage::execution::sql::VectorProjectionIterator *vpi) { vpi->ResetFiltered(); }

VM_OP_HOT void OpVPIGetSlot(noisepage::storage::TupleSlot *slot,
                            noisepage::execution::sql::VectorProjectionIterator *vpi) {
  *slot = vpi->GetCurrentSlot();
}

// ---------------------------------------------------------
// VPI Get
// ---------------------------------------------------------

#define GEN_VPI_GET(Name, SqlValueType, CppType)                                                      \
  VM_OP_HOT void OpVPIGet##Name(noisepage::execution::sql::SqlValueType *out,                         \
                                noisepage::execution::sql::VectorProjectionIterator *const vpi,       \
                                const uint32_t col_idx) {                                             \
    auto *ptr = vpi->GetValue<CppType, false>(col_idx, nullptr);                                      \
    NOISEPAGE_ASSERT(ptr != nullptr, "Null data pointer when trying to read attribute");              \
    out->is_null_ = false;                                                                            \
    out->val_ = *ptr;                                                                                 \
  }                                                                                                   \
  VM_OP_HOT void OpVPIGet##Name##Null(noisepage::execution::sql::SqlValueType *out,                   \
                                      noisepage::execution::sql::VectorProjectionIterator *const vpi, \
                                      const uint32_t col_idx) {                                       \
    bool null = false;                                                                                \
    auto *ptr = vpi->GetValue<CppType, true>(col_idx, &null);                                         \
    NOISEPAGE_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");                     \
    out->is_null_ = null;                                                                             \
    out->val_ = *ptr;                                                                                 \
  }

#define GEN_VPI_SET(Name, SqlValueType, CppType)                                                                \
  VM_OP_HOT void OpVPISet##Name(noisepage::execution::sql::VectorProjectionIterator *const vpi,                 \
                                noisepage::execution::sql::SqlValueType *input, const uint32_t col_idx) {       \
    vpi->SetValue<CppType, false>(col_idx, input->val_, false);                                                 \
  }                                                                                                             \
  VM_OP_HOT void OpVPISet##Name##Null(noisepage::execution::sql::VectorProjectionIterator *const vpi,           \
                                      noisepage::execution::sql::SqlValueType *input, const uint32_t col_idx) { \
    vpi->SetValue<CppType, true>(col_idx, input->val_, input->is_null_);                                        \
  }

GEN_VPI_GET(Bool, BoolVal, bool);
GEN_VPI_GET(TinyInt, Integer, int8_t);
GEN_VPI_GET(SmallInt, Integer, int16_t);
GEN_VPI_GET(Integer, Integer, int32_t);
GEN_VPI_GET(BigInt, Integer, int64_t);
GEN_VPI_GET(Real, Real, float);
GEN_VPI_GET(Double, Real, double);
GEN_VPI_GET(Decimal, DecimalVal, noisepage::execution::sql::Decimal64);
GEN_VPI_GET(Date, DateVal, noisepage::execution::sql::Date);
GEN_VPI_GET(Timestamp, TimestampVal, noisepage::execution::sql::Timestamp);
GEN_VPI_GET(String, StringVal, noisepage::storage::VarlenEntry);

GEN_VPI_SET(Bool, BoolVal, bool);
GEN_VPI_SET(TinyInt, Integer, int8_t);
GEN_VPI_SET(SmallInt, Integer, int16_t);
GEN_VPI_SET(Integer, Integer, int32_t);
GEN_VPI_SET(BigInt, Integer, int64_t);
GEN_VPI_SET(Real, Real, float);
GEN_VPI_SET(Double, Real, double);
GEN_VPI_SET(Decimal, DecimalVal, noisepage::execution::sql::Decimal64);
GEN_VPI_SET(Date, DateVal, noisepage::execution::sql::Date);
GEN_VPI_SET(Timestamp, TimestampVal, noisepage::execution::sql::Timestamp);
GEN_VPI_SET(String, StringVal, noisepage::storage::VarlenEntry);

#undef GEN_VPI_SET
#undef GEN_VPI_GET

VM_OP_HOT void OpVPIGetPointer(noisepage::byte **out, noisepage::execution::sql::VectorProjectionIterator *const vpi,
                               const uint32_t col_idx) {
  auto *ptr = vpi->GetValue<noisepage::byte *, false>(col_idx, nullptr);
  NOISEPAGE_ASSERT(ptr != nullptr, "Null data pointer when trying to read attribute");
  *out = *ptr;
}

// ---------------------------------------------------------
// Hashing
// ---------------------------------------------------------

VM_OP_HOT void OpHashInt(noisepage::hash_t *const hash_val, const noisepage::execution::sql::Integer *const input,
                         const noisepage::hash_t seed) {
  *hash_val = input->is_null_ ? 0 : noisepage::common::HashUtil::HashCrc(input->val_, seed);
}

VM_OP_HOT void OpHashBool(noisepage::hash_t *const hash_val, const noisepage::execution::sql::BoolVal *const input,
                          const noisepage::hash_t seed) {
  *hash_val = input->is_null_ ? 0 : noisepage::common::HashUtil::HashCrc(input->val_, seed);
}

VM_OP_HOT void OpHashReal(noisepage::hash_t *const hash_val, const noisepage::execution::sql::Real *const input,
                          const noisepage::hash_t seed) {
  *hash_val = input->is_null_ ? 0 : noisepage::common::HashUtil::HashCrc(input->val_, seed);
}

VM_OP_HOT void OpHashString(noisepage::hash_t *const hash_val, const noisepage::execution::sql::StringVal *const input,
                            const noisepage::hash_t seed) {
  *hash_val = input->is_null_ ? 0 : input->val_.Hash(seed);
}

VM_OP_HOT void OpHashDate(noisepage::hash_t *const hash_val, const noisepage::execution::sql::DateVal *const input,
                          const noisepage::hash_t seed) {
  *hash_val = input->is_null_ ? 0 : input->val_.Hash(seed);
}

VM_OP_HOT void OpHashTimestamp(noisepage::hash_t *const hash_val,
                               const noisepage::execution::sql::TimestampVal *const input,
                               const noisepage::hash_t seed) {
  *hash_val = input->is_null_ ? 0 : input->val_.Hash(seed);
}

VM_OP_HOT void OpHashCombine(noisepage::hash_t *hash_val, noisepage::hash_t new_hash_val) {
  *hash_val = noisepage::common::HashUtil::CombineHashes(*hash_val, new_hash_val);
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

VM_OP void OpFilterManagerInit(noisepage::execution::sql::FilterManager *filter_manager,
                               const noisepage::execution::exec::ExecutionSettings &exec_settings);

VM_OP void OpFilterManagerStartNewClause(noisepage::execution::sql::FilterManager *filter_manager);

VM_OP void OpFilterManagerInsertFilter(noisepage::execution::sql::FilterManager *filter_manager,
                                       noisepage::execution::sql::FilterManager::MatchFn clause);

VM_OP void OpFilterManagerRunFilters(noisepage::execution::sql::FilterManager *filter,
                                     noisepage::execution::sql::VectorProjectionIterator *vpi,
                                     noisepage::execution::exec::ExecutionContext *exec_ctx);

VM_OP void OpFilterManagerFree(noisepage::execution::sql::FilterManager *filter);

// ---------------------------------------------------------
// Vector Filter Executor
// ---------------------------------------------------------

#define GEN_VECTOR_FILTER(Name)                                                                                        \
  VM_OP_HOT void OpVectorFilter##Name(const noisepage::execution::exec::ExecutionSettings &exec_settings,              \
                                      noisepage::execution::sql::VectorProjection *vector_projection,                  \
                                      const uint32_t left_col_idx, const uint32_t right_col_idx,                       \
                                      noisepage::execution::sql::TupleIdList *tid_list) {                              \
    noisepage::execution::sql::VectorFilterExecutor::Select##Name(exec_settings, vector_projection, left_col_idx,      \
                                                                  right_col_idx, tid_list);                            \
  }                                                                                                                    \
  VM_OP_HOT void OpVectorFilter##Name##Val(const noisepage::execution::exec::ExecutionSettings &exec_settings,         \
                                           noisepage::execution::sql::VectorProjection *vector_projection,             \
                                           const uint32_t left_col_idx, const noisepage::execution::sql::Val *val,     \
                                           noisepage::execution::sql::TupleIdList *tid_list) {                         \
    noisepage::execution::sql::VectorFilterExecutor::Select##Name##Val(exec_settings, vector_projection, left_col_idx, \
                                                                       *val, tid_list);                                \
  }

GEN_VECTOR_FILTER(Equal)
GEN_VECTOR_FILTER(GreaterThan)
GEN_VECTOR_FILTER(GreaterThanEqual)
GEN_VECTOR_FILTER(LessThan)
GEN_VECTOR_FILTER(LessThanEqual)
GEN_VECTOR_FILTER(NotEqual)
GEN_VECTOR_FILTER(Like)
GEN_VECTOR_FILTER(NotLike)

#undef GEN_VECTOR_FILTER

// ---------------------------------------------------------
// Scalar SQL comparisons
// ---------------------------------------------------------

VM_OP_HOT void OpForceBoolTruth(bool *result, noisepage::execution::sql::BoolVal *input) {
  *result = input->ForceTruth();
}

VM_OP_HOT void OpInitSqlNull(noisepage::execution::sql::Val *result) { *result = noisepage::execution::sql::Val(true); }

VM_OP_HOT void OpInitBool(noisepage::execution::sql::BoolVal *result, bool input) {
  result->is_null_ = false;
  result->val_ = input;
}

VM_OP_HOT void OpInitInteger(noisepage::execution::sql::Integer *result, int64_t input) {
  result->is_null_ = false;
  result->val_ = input;
}

VM_OP_HOT void OpInitInteger64(noisepage::execution::sql::Integer *result, int64_t input) {
  result->is_null_ = false;
  result->val_ = input;
}

VM_OP_HOT void OpInitReal(noisepage::execution::sql::Real *result, double input) {
  result->is_null_ = false;
  result->val_ = input;
}

VM_OP_HOT void OpInitDate(noisepage::execution::sql::DateVal *result, int32_t year, int32_t month, int32_t day) {
  result->is_null_ = false;
  result->val_ = noisepage::execution::sql::Date::FromYMD(year, month, day);
}

VM_OP_HOT void OpInitTimestamp(noisepage::execution::sql::TimestampVal *result, uint64_t usec) {
  result->is_null_ = false;
  result->val_ = noisepage::execution::sql::Timestamp::FromMicroseconds(usec);
}

VM_OP_HOT void OpInitTimestampYMDHMSMU(noisepage::execution::sql::TimestampVal *result, int32_t year, int32_t month,
                                       int32_t day, int32_t hour, int32_t minute, int32_t sec, int32_t milli,
                                       int32_t micro) {
  result->is_null_ = false;
  auto res = noisepage::execution::sql::Timestamp::FromYMDHMSMU(year, month, day, hour, minute, sec, milli, micro);
  result->val_ = res;
}

VM_OP_HOT void OpInitString(noisepage::execution::sql::StringVal *result, const uint8_t *str, uint32_t length) {
  *result = noisepage::execution::sql::StringVal(reinterpret_cast<const char *>(str), length);
}

VM_OP_WARM void OpBoolToInteger(noisepage::execution::sql::Integer *result,
                                const noisepage::execution::sql::BoolVal *input) {
  noisepage::execution::sql::CastingFunctions::CastToInteger(result, *input);
}

VM_OP_WARM void OpIntegerToBool(noisepage::execution::sql::BoolVal *result,
                                const noisepage::execution::sql::Integer *input) {
  noisepage::execution::sql::CastingFunctions::CastToBoolVal(result, *input);
}

VM_OP_WARM void OpIntegerToReal(noisepage::execution::sql::Real *result,
                                const noisepage::execution::sql::Integer *input) {
  noisepage::execution::sql::CastingFunctions::CastToReal(result, *input);
}

VM_OP_WARM void OpIntegerToString(noisepage::execution::sql::StringVal *result,
                                  noisepage::execution::exec::ExecutionContext *exec_ctx,
                                  const noisepage::execution::sql::Integer *input) {
  noisepage::execution::sql::CastingFunctions::CastToStringVal(result, exec_ctx, *input);
}

VM_OP_WARM void OpRealToBool(noisepage::execution::sql::BoolVal *result, const noisepage::execution::sql::Real *input) {
  noisepage::execution::sql::CastingFunctions::CastToBoolVal(result, *input);
}

VM_OP_WARM void OpRealToInteger(noisepage::execution::sql::Integer *result,
                                const noisepage::execution::sql::Real *input) {
  noisepage::execution::sql::CastingFunctions::CastToInteger(result, *input);
}

VM_OP_WARM void OpRealToString(noisepage::execution::sql::StringVal *result,
                               noisepage::execution::exec::ExecutionContext *exec_ctx,
                               const noisepage::execution::sql::Real *input) {
  noisepage::execution::sql::CastingFunctions::CastToStringVal(result, exec_ctx, *input);
}

VM_OP_WARM void OpDateToTimestamp(noisepage::execution::sql::TimestampVal *result,
                                  const noisepage::execution::sql::DateVal *input) {
  noisepage::execution::sql::CastingFunctions::CastToTimestampVal(result, *input);
}

VM_OP_WARM void OpDateToString(noisepage::execution::sql::StringVal *result,
                               noisepage::execution::exec::ExecutionContext *exec_ctx,
                               const noisepage::execution::sql::DateVal *input) {
  noisepage::execution::sql::CastingFunctions::CastToStringVal(result, exec_ctx, *input);
}

VM_OP_WARM void OpTimestampToDate(noisepage::execution::sql::DateVal *result,
                                  const noisepage::execution::sql::TimestampVal *input) {
  noisepage::execution::sql::CastingFunctions::CastToDateVal(result, *input);
}

VM_OP_WARM void OpTimestampToString(noisepage::execution::sql::StringVal *result,
                                    noisepage::execution::exec::ExecutionContext *exec_ctx,
                                    const noisepage::execution::sql::TimestampVal *input) {
  noisepage::execution::sql::CastingFunctions::CastToStringVal(result, exec_ctx, *input);
}

VM_OP_WARM void OpStringToBool(noisepage::execution::sql::BoolVal *result,
                               const noisepage::execution::sql::StringVal *input) {
  noisepage::execution::sql::CastingFunctions::CastToBoolVal(result, *input);
}

VM_OP_WARM void OpStringToInteger(noisepage::execution::sql::Integer *result,
                                  const noisepage::execution::sql::StringVal *input) {
  noisepage::execution::sql::CastingFunctions::CastToInteger(result, *input);
}

VM_OP_WARM void OpStringToReal(noisepage::execution::sql::Real *result,
                               const noisepage::execution::sql::StringVal *input) {
  noisepage::execution::sql::CastingFunctions::CastToReal(result, *input);
}

VM_OP_WARM void OpStringToDate(noisepage::execution::sql::DateVal *result,
                               const noisepage::execution::sql::StringVal *input) {
  noisepage::execution::sql::CastingFunctions::CastToDateVal(result, *input);
}

VM_OP_WARM void OpStringToTimestamp(noisepage::execution::sql::TimestampVal *result,
                                    const noisepage::execution::sql::StringVal *input) {
  noisepage::execution::sql::CastingFunctions::CastToTimestampVal(result, *input);
}

#define GEN_SQL_COMPARISONS(NAME, TYPE)                                                         \
  VM_OP_HOT void OpGreaterThan##NAME(noisepage::execution::sql::BoolVal *const result,          \
                                     const noisepage::execution::sql::TYPE *const left,         \
                                     const noisepage::execution::sql::TYPE *const right) {      \
    noisepage::execution::sql::ComparisonFunctions::Gt##TYPE(result, *left, *right);            \
  }                                                                                             \
  VM_OP_HOT void OpGreaterThanEqual##NAME(noisepage::execution::sql::BoolVal *const result,     \
                                          const noisepage::execution::sql::TYPE *const left,    \
                                          const noisepage::execution::sql::TYPE *const right) { \
    noisepage::execution::sql::ComparisonFunctions::Ge##TYPE(result, *left, *right);            \
  }                                                                                             \
  VM_OP_HOT void OpEqual##NAME(noisepage::execution::sql::BoolVal *const result,                \
                               const noisepage::execution::sql::TYPE *const left,               \
                               const noisepage::execution::sql::TYPE *const right) {            \
    noisepage::execution::sql::ComparisonFunctions::Eq##TYPE(result, *left, *right);            \
  }                                                                                             \
  VM_OP_HOT void OpLessThan##NAME(noisepage::execution::sql::BoolVal *const result,             \
                                  const noisepage::execution::sql::TYPE *const left,            \
                                  const noisepage::execution::sql::TYPE *const right) {         \
    noisepage::execution::sql::ComparisonFunctions::Lt##TYPE(result, *left, *right);            \
  }                                                                                             \
  VM_OP_HOT void OpLessThanEqual##NAME(noisepage::execution::sql::BoolVal *const result,        \
                                       const noisepage::execution::sql::TYPE *const left,       \
                                       const noisepage::execution::sql::TYPE *const right) {    \
    noisepage::execution::sql::ComparisonFunctions::Le##TYPE(result, *left, *right);            \
  }                                                                                             \
  VM_OP_HOT void OpNotEqual##NAME(noisepage::execution::sql::BoolVal *const result,             \
                                  const noisepage::execution::sql::TYPE *const left,            \
                                  const noisepage::execution::sql::TYPE *const right) {         \
    noisepage::execution::sql::ComparisonFunctions::Ne##TYPE(result, *left, *right);            \
  }

GEN_SQL_COMPARISONS(Bool, BoolVal)
GEN_SQL_COMPARISONS(Integer, Integer)
GEN_SQL_COMPARISONS(Real, Real)
GEN_SQL_COMPARISONS(Date, DateVal)
GEN_SQL_COMPARISONS(Timestamp, TimestampVal)
GEN_SQL_COMPARISONS(String, StringVal)

#undef GEN_SQL_COMPARISONS

VM_OP_WARM void OpAbsInteger(noisepage::execution::sql::Integer *const result,
                             const noisepage::execution::sql::Integer *const left) {
  noisepage::execution::sql::ArithmeticFunctions::Abs(result, *left);
}

VM_OP_WARM void OpAbsReal(noisepage::execution::sql::Real *const result,
                          const noisepage::execution::sql::Real *const left) {
  noisepage::execution::sql::ArithmeticFunctions::Abs(result, *left);
}

VM_OP_HOT void OpAddInteger(noisepage::execution::sql::Integer *const result,
                            const noisepage::execution::sql::Integer *const left,
                            const noisepage::execution::sql::Integer *const right) {
  UNUSED_ATTRIBUTE bool overflow;
  noisepage::execution::sql::ArithmeticFunctions::Add(result, *left, *right, &overflow);
}

VM_OP_HOT void OpSubInteger(noisepage::execution::sql::Integer *const result,
                            const noisepage::execution::sql::Integer *const left,
                            const noisepage::execution::sql::Integer *const right) {
  UNUSED_ATTRIBUTE bool overflow;
  noisepage::execution::sql::ArithmeticFunctions::Sub(result, *left, *right, &overflow);
}

VM_OP_HOT void OpMulInteger(noisepage::execution::sql::Integer *const result,
                            const noisepage::execution::sql::Integer *const left,
                            const noisepage::execution::sql::Integer *const right) {
  UNUSED_ATTRIBUTE bool overflow;
  noisepage::execution::sql::ArithmeticFunctions::Mul(result, *left, *right, &overflow);
}

VM_OP_HOT void OpDivInteger(noisepage::execution::sql::Integer *const result,
                            const noisepage::execution::sql::Integer *const left,
                            const noisepage::execution::sql::Integer *const right) {
  UNUSED_ATTRIBUTE bool div_by_zero = false;
  noisepage::execution::sql::ArithmeticFunctions::IntDiv(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpModInteger(noisepage::execution::sql::Integer *const result,
                            const noisepage::execution::sql::Integer *const left,
                            const noisepage::execution::sql::Integer *const right) {
  UNUSED_ATTRIBUTE bool div_by_zero = false;
  noisepage::execution::sql::ArithmeticFunctions::IntMod(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpAddReal(noisepage::execution::sql::Real *const result,
                         const noisepage::execution::sql::Real *const left,
                         const noisepage::execution::sql::Real *const right) {
  noisepage::execution::sql::ArithmeticFunctions::Add(result, *left, *right);
}

VM_OP_HOT void OpSubReal(noisepage::execution::sql::Real *const result,
                         const noisepage::execution::sql::Real *const left,
                         const noisepage::execution::sql::Real *const right) {
  noisepage::execution::sql::ArithmeticFunctions::Sub(result, *left, *right);
}

VM_OP_HOT void OpMulReal(noisepage::execution::sql::Real *const result,
                         const noisepage::execution::sql::Real *const left,
                         const noisepage::execution::sql::Real *const right) {
  noisepage::execution::sql::ArithmeticFunctions::Mul(result, *left, *right);
}

VM_OP_HOT void OpDivReal(noisepage::execution::sql::Real *const result,
                         const noisepage::execution::sql::Real *const left,
                         const noisepage::execution::sql::Real *const right) {
  UNUSED_ATTRIBUTE bool div_by_zero = false;
  noisepage::execution::sql::ArithmeticFunctions::Div(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpModReal(noisepage::execution::sql::Real *const result,
                         const noisepage::execution::sql::Real *const left,
                         const noisepage::execution::sql::Real *const right) {
  UNUSED_ATTRIBUTE bool div_by_zero = false;
  noisepage::execution::sql::ArithmeticFunctions::Mod(result, *left, *right, &div_by_zero);
}

// ---------------------------------------------------------
// SQL Aggregations
// ---------------------------------------------------------

VM_OP void OpAggregationHashTableInit(noisepage::execution::sql::AggregationHashTable *agg_hash_table,
                                      noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t payload_size);

VM_OP void OpAggregationHashTableGetTupleCount(uint32_t *result,
                                               noisepage::execution::sql::AggregationHashTable *agg_hash_table);

VM_OP void OpAggregationHashTableGetInsertCount(uint32_t *result,
                                                noisepage::execution::sql::AggregationHashTable *agg_hash_table);

VM_OP_HOT void OpAggregationHashTableAllocTuple(noisepage::byte **result,
                                                noisepage::execution::sql::AggregationHashTable *agg_hash_table,
                                                const noisepage::hash_t hash_val) {
  *result = agg_hash_table->AllocInputTuple(hash_val);
}

VM_OP_HOT void OpAggregationHashTableAllocTuplePartitioned(
    noisepage::byte **result, noisepage::execution::sql::AggregationHashTable *agg_hash_table,
    const noisepage::hash_t hash_val) {
  *result = agg_hash_table->AllocInputTuplePartitioned(hash_val);
}

VM_OP_HOT void OpAggregationHashTableLinkHashTableEntry(noisepage::execution::sql::AggregationHashTable *agg_hash_table,
                                                        noisepage::execution::sql::HashTableEntry *entry) {
  agg_hash_table->Insert(entry);
}

VM_OP_HOT void OpAggregationHashTableLookup(noisepage::byte **result,
                                            noisepage::execution::sql::AggregationHashTable *const agg_hash_table,
                                            const noisepage::hash_t hash_val,
                                            const noisepage::execution::sql::AggregationHashTable::KeyEqFn key_eq_fn,
                                            const void *probe_tuple) {
  *result = agg_hash_table->Lookup(hash_val, key_eq_fn, probe_tuple);
}

VM_OP_HOT void OpAggregationHashTableProcessBatch(
    noisepage::execution::sql::AggregationHashTable *const agg_hash_table,
    noisepage::execution::sql::VectorProjectionIterator *vpi, const uint32_t num_keys, const uint32_t key_cols[],
    const noisepage::execution::sql::AggregationHashTable::VectorInitAggFn init_fn,
    const noisepage::execution::sql::AggregationHashTable::VectorAdvanceAggFn advance_fn, const bool partitioned) {
  agg_hash_table->ProcessBatch(vpi, {key_cols, key_cols + num_keys}, init_fn, advance_fn, partitioned);
}

VM_OP_HOT void OpAggregationHashTableTransferPartitions(
    noisepage::execution::sql::AggregationHashTable *const agg_hash_table,
    noisepage::execution::sql::ThreadStateContainer *const thread_state_container, const uint32_t agg_ht_offset,
    const noisepage::execution::sql::AggregationHashTable::MergePartitionFn merge_partition_fn) {
  agg_hash_table->TransferMemoryAndPartitions(thread_state_container, agg_ht_offset, merge_partition_fn);
}

VM_OP void OpAggregationHashTableBuildAllHashTablePartitions(
    noisepage::execution::sql::AggregationHashTable *agg_hash_table, void *query_state);

VM_OP void OpAggregationHashTableRepartition(noisepage::execution::sql::AggregationHashTable *agg_hash_table);

VM_OP void OpAggregationHashTableMergePartitions(
    noisepage::execution::sql::AggregationHashTable *agg_hash_table,
    noisepage::execution::sql::AggregationHashTable *target_agg_hash_table, void *query_state,
    noisepage::execution::sql::AggregationHashTable::MergePartitionFn merge_partition_fn);

VM_OP_HOT void OpAggregationHashTableParallelPartitionedScan(
    noisepage::execution::sql::AggregationHashTable *const agg_hash_table, void *const query_state,
    noisepage::execution::sql::ThreadStateContainer *const thread_state_container,
    const noisepage::execution::sql::AggregationHashTable::ScanPartitionFn scan_partition_fn) {
  agg_hash_table->ExecuteParallelPartitionedScan(query_state, thread_state_container, scan_partition_fn);
}

VM_OP void OpAggregationHashTableFree(noisepage::execution::sql::AggregationHashTable *agg_hash_table);

VM_OP void OpAggregationHashTableIteratorInit(noisepage::execution::sql::AHTIterator *iter,
                                              noisepage::execution::sql::AggregationHashTable *agg_hash_table);

VM_OP_HOT void OpAggregationHashTableIteratorHasNext(bool *has_more, noisepage::execution::sql::AHTIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationHashTableIteratorNext(noisepage::execution::sql::AHTIterator *iter) { iter->Next(); }

VM_OP_HOT void OpAggregationHashTableIteratorGetRow(const noisepage::byte **row,
                                                    noisepage::execution::sql::AHTIterator *iter) {
  *row = iter->GetCurrentAggregateRow();
}

VM_OP void OpAggregationHashTableIteratorFree(noisepage::execution::sql::AHTIterator *iter);

VM_OP_HOT void OpAggregationOverflowPartitionIteratorHasNext(
    bool *has_more, noisepage::execution::sql::AHTOverflowPartitionIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorNext(
    noisepage::execution::sql::AHTOverflowPartitionIterator *iter) {
  iter->Next();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetHash(
    noisepage::hash_t *hash_val, noisepage::execution::sql::AHTOverflowPartitionIterator *iter) {
  *hash_val = iter->GetRowHash();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetRow(
    const noisepage::byte **row, noisepage::execution::sql::AHTOverflowPartitionIterator *iter) {
  *row = iter->GetRow();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetRowEntry(
    noisepage::execution::sql::HashTableEntry **entry, noisepage::execution::sql::AHTOverflowPartitionIterator *iter) {
  *entry = iter->GetEntryForRow();
}

// ---------------------------------------------------------
// COUNT
// ---------------------------------------------------------

VM_OP_HOT void OpCountAggregateInit(noisepage::execution::sql::CountAggregate *agg) {
  new (agg) noisepage::execution::sql::CountAggregate();
}

VM_OP_HOT void OpCountAggregateAdvance(noisepage::execution::sql::CountAggregate *agg,
                                       const noisepage::execution::sql::Val *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpCountAggregateMerge(noisepage::execution::sql::CountAggregate *agg_1,
                                     const noisepage::execution::sql::CountAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountAggregateReset(noisepage::execution::sql::CountAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpCountAggregateGetResult(noisepage::execution::sql::Integer *result,
                                         const noisepage::execution::sql::CountAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountAggregateFree(noisepage::execution::sql::CountAggregate *agg) { agg->~CountAggregate(); }

// ---------------------------------------------------------
// COUNT(*)
// ---------------------------------------------------------

VM_OP_HOT void OpCountStarAggregateInit(noisepage::execution::sql::CountStarAggregate *agg) {
  new (agg) noisepage::execution::sql::CountStarAggregate();
}

VM_OP_HOT void OpCountStarAggregateAdvance(noisepage::execution::sql::CountStarAggregate *agg,
                                           const noisepage::execution::sql::Val *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpCountStarAggregateMerge(noisepage::execution::sql::CountStarAggregate *agg_1,
                                         const noisepage::execution::sql::CountStarAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountStarAggregateReset(noisepage::execution::sql::CountStarAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpCountStarAggregateGetResult(noisepage::execution::sql::Integer *result,
                                             const noisepage::execution::sql::CountStarAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountStarAggregateFree(noisepage::execution::sql::CountStarAggregate *agg) {
  agg->~CountStarAggregate();
}

// ---------------------------------------------------------
// SUM
// ---------------------------------------------------------

VM_OP_HOT void OpIntegerSumAggregateInit(noisepage::execution::sql::IntegerSumAggregate *agg) {
  new (agg) noisepage::execution::sql::IntegerSumAggregate();
}

VM_OP_HOT void OpIntegerSumAggregateAdvance(noisepage::execution::sql::IntegerSumAggregate *agg,
                                            const noisepage::execution::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerSumAggregateMerge(noisepage::execution::sql::IntegerSumAggregate *agg_1,
                                          const noisepage::execution::sql::IntegerSumAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerSumAggregateReset(noisepage::execution::sql::IntegerSumAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerSumAggregateGetResult(noisepage::execution::sql::Integer *result,
                                              const noisepage::execution::sql::IntegerSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpIntegerSumAggregateFree(noisepage::execution::sql::IntegerSumAggregate *agg) {
  agg->~IntegerSumAggregate();
}

VM_OP_HOT void OpRealSumAggregateInit(noisepage::execution::sql::RealSumAggregate *agg) {
  new (agg) noisepage::execution::sql::RealSumAggregate();
}

VM_OP_HOT void OpRealSumAggregateAdvance(noisepage::execution::sql::RealSumAggregate *agg,
                                         const noisepage::execution::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealSumAggregateMerge(noisepage::execution::sql::RealSumAggregate *agg_1,
                                       const noisepage::execution::sql::RealSumAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealSumAggregateReset(noisepage::execution::sql::RealSumAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealSumAggregateGetResult(noisepage::execution::sql::Real *result,
                                           const noisepage::execution::sql::RealSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpRealSumAggregateFree(noisepage::execution::sql::RealSumAggregate *agg) { agg->~RealSumAggregate(); }

// ---------------------------------------------------------
// MAX
// ---------------------------------------------------------

VM_OP_HOT void OpIntegerMaxAggregateInit(noisepage::execution::sql::IntegerMaxAggregate *agg) {
  new (agg) noisepage::execution::sql::IntegerMaxAggregate();
}

VM_OP_HOT void OpIntegerMaxAggregateAdvance(noisepage::execution::sql::IntegerMaxAggregate *agg,
                                            const noisepage::execution::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerMaxAggregateMerge(noisepage::execution::sql::IntegerMaxAggregate *agg_1,
                                          const noisepage::execution::sql::IntegerMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMaxAggregateReset(noisepage::execution::sql::IntegerMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerMaxAggregateGetResult(noisepage::execution::sql::Integer *result,
                                              const noisepage::execution::sql::IntegerMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpIntegerMaxAggregateFree(noisepage::execution::sql::IntegerMaxAggregate *agg) {
  agg->~IntegerMaxAggregate();
}

VM_OP_HOT void OpRealMaxAggregateInit(noisepage::execution::sql::RealMaxAggregate *agg) {
  new (agg) noisepage::execution::sql::RealMaxAggregate();
}

VM_OP_HOT void OpRealMaxAggregateAdvance(noisepage::execution::sql::RealMaxAggregate *agg,
                                         const noisepage::execution::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealMaxAggregateMerge(noisepage::execution::sql::RealMaxAggregate *agg_1,
                                       const noisepage::execution::sql::RealMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealMaxAggregateReset(noisepage::execution::sql::RealMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealMaxAggregateGetResult(noisepage::execution::sql::Real *result,
                                           const noisepage::execution::sql::RealMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpRealMaxAggregateFree(noisepage::execution::sql::RealMaxAggregate *agg) { agg->~RealMaxAggregate(); }

VM_OP_HOT void OpDateMaxAggregateInit(noisepage::execution::sql::DateMaxAggregate *agg) {
  new (agg) noisepage::execution::sql::DateMaxAggregate();
}

VM_OP_HOT void OpDateMaxAggregateAdvance(noisepage::execution::sql::DateMaxAggregate *agg,
                                         const noisepage::execution::sql::DateVal *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpDateMaxAggregateMerge(noisepage::execution::sql::DateMaxAggregate *agg_1,
                                       const noisepage::execution::sql::DateMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpDateMaxAggregateReset(noisepage::execution::sql::DateMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpDateMaxAggregateGetResult(noisepage::execution::sql::DateVal *result,
                                           const noisepage::execution::sql::DateMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpDateMaxAggregateFree(noisepage::execution::sql::DateMaxAggregate *agg) { agg->~DateMaxAggregate(); }

VM_OP_HOT void OpStringMaxAggregateInit(noisepage::execution::sql::StringMaxAggregate *agg) {
  new (agg) noisepage::execution::sql::StringMaxAggregate();
}

VM_OP_HOT void OpStringMaxAggregateAdvance(noisepage::execution::sql::StringMaxAggregate *agg,
                                           const noisepage::execution::sql::StringVal *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpStringMaxAggregateMerge(noisepage::execution::sql::StringMaxAggregate *agg_1,
                                         const noisepage::execution::sql::StringMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpStringMaxAggregateReset(noisepage::execution::sql::StringMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpStringMaxAggregateGetResult(noisepage::execution::sql::StringVal *result,
                                             const noisepage::execution::sql::StringMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpStringMaxAggregateFree(noisepage::execution::sql::StringMaxAggregate *agg) {
  agg->~StringMaxAggregate();
}

// ---------------------------------------------------------
// MIN
// ---------------------------------------------------------

VM_OP_HOT void OpIntegerMinAggregateInit(noisepage::execution::sql::IntegerMinAggregate *agg) {
  new (agg) noisepage::execution::sql::IntegerMinAggregate();
}

VM_OP_HOT void OpIntegerMinAggregateAdvance(noisepage::execution::sql::IntegerMinAggregate *agg,
                                            const noisepage::execution::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerMinAggregateMerge(noisepage::execution::sql::IntegerMinAggregate *agg_1,
                                          const noisepage::execution::sql::IntegerMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMinAggregateReset(noisepage::execution::sql::IntegerMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerMinAggregateGetResult(noisepage::execution::sql::Integer *result,
                                              const noisepage::execution::sql::IntegerMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpIntegerMinAggregateFree(noisepage::execution::sql::IntegerMinAggregate *agg) {
  agg->~IntegerMinAggregate();
}

VM_OP_HOT void OpRealMinAggregateInit(noisepage::execution::sql::RealMinAggregate *agg) {
  new (agg) noisepage::execution::sql::RealMinAggregate();
}

VM_OP_HOT void OpRealMinAggregateAdvance(noisepage::execution::sql::RealMinAggregate *agg,
                                         const noisepage::execution::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealMinAggregateMerge(noisepage::execution::sql::RealMinAggregate *agg_1,
                                       const noisepage::execution::sql::RealMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealMinAggregateReset(noisepage::execution::sql::RealMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealMinAggregateGetResult(noisepage::execution::sql::Real *result,
                                           const noisepage::execution::sql::RealMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpRealMinAggregateFree(noisepage::execution::sql::RealMinAggregate *agg) { agg->~RealMinAggregate(); }

VM_OP_HOT void OpDateMinAggregateInit(noisepage::execution::sql::DateMinAggregate *agg) {
  new (agg) noisepage::execution::sql::DateMinAggregate();
}

VM_OP_HOT void OpDateMinAggregateAdvance(noisepage::execution::sql::DateMinAggregate *agg,
                                         const noisepage::execution::sql::DateVal *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpDateMinAggregateMerge(noisepage::execution::sql::DateMinAggregate *agg_1,
                                       const noisepage::execution::sql::DateMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpDateMinAggregateReset(noisepage::execution::sql::DateMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpDateMinAggregateGetResult(noisepage::execution::sql::DateVal *result,
                                           const noisepage::execution::sql::DateMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpDateMinAggregateFree(noisepage::execution::sql::DateMinAggregate *agg) { agg->~DateMinAggregate(); }

VM_OP_HOT void OpStringMinAggregateInit(noisepage::execution::sql::StringMinAggregate *agg) {
  new (agg) noisepage::execution::sql::StringMinAggregate();
}

VM_OP_HOT void OpStringMinAggregateAdvance(noisepage::execution::sql::StringMinAggregate *agg,
                                           const noisepage::execution::sql::StringVal *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpStringMinAggregateMerge(noisepage::execution::sql::StringMinAggregate *agg_1,
                                         const noisepage::execution::sql::StringMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpStringMinAggregateReset(noisepage::execution::sql::StringMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpStringMinAggregateGetResult(noisepage::execution::sql::StringVal *result,
                                             const noisepage::execution::sql::StringMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpStringMinAggregateFree(noisepage::execution::sql::StringMinAggregate *agg) {
  agg->~StringMinAggregate();
}

// ---------------------------------------------------------
// AVG
// ---------------------------------------------------------

VM_OP_HOT void OpAvgAggregateInit(noisepage::execution::sql::AvgAggregate *agg) {
  new (agg) noisepage::execution::sql::AvgAggregate();
}

VM_OP_HOT void OpAvgAggregateAdvanceInteger(noisepage::execution::sql::AvgAggregate *agg,
                                            const noisepage::execution::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpAvgAggregateAdvanceReal(noisepage::execution::sql::AvgAggregate *agg,
                                         const noisepage::execution::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpAvgAggregateMerge(noisepage::execution::sql::AvgAggregate *agg_1,
                                   const noisepage::execution::sql::AvgAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpAvgAggregateReset(noisepage::execution::sql::AvgAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpAvgAggregateGetResult(noisepage::execution::sql::Real *result,
                                       const noisepage::execution::sql::AvgAggregate *agg) {
  *result = agg->GetResultAvg();
}

VM_OP_HOT void OpAvgAggregateFree(noisepage::execution::sql::AvgAggregate *agg) { agg->~AvgAggregate(); }

#define GEN_BINARY_AGGREGATE(SQL_TYPE, AGG_TYPE)                                                 \
  VM_OP_HOT void Op##AGG_TYPE##Init(noisepage::execution::sql::AGG_TYPE *agg) {                  \
    new (agg) noisepage::execution::sql::AGG_TYPE();                                             \
  }                                                                                              \
  VM_OP_HOT void Op##AGG_TYPE##Advance(noisepage::execution::sql::AGG_TYPE *agg,                 \
                                       const noisepage::execution::sql::SQL_TYPE *val) {         \
    agg->Advance(*val);                                                                          \
  }                                                                                              \
  VM_OP_HOT void Op##AGG_TYPE##Merge(noisepage::execution::sql::AGG_TYPE *agg_1,                 \
                                     const noisepage::execution::sql::AGG_TYPE *agg_2) {         \
    agg_1->Merge(*agg_2);                                                                        \
  }                                                                                              \
  VM_OP_HOT void Op##AGG_TYPE##Reset(noisepage::execution::sql::AGG_TYPE *agg) { agg->Reset(); } \
  VM_OP_HOT void Op##AGG_TYPE##GetResult(noisepage::execution::sql::StringVal *result,           \
                                         noisepage::execution::exec::ExecutionContext *ctx,      \
                                         const noisepage::execution::sql::AGG_TYPE *agg) {       \
    *result = agg->GetResult(ctx);                                                               \
  }                                                                                              \
  VM_OP_HOT void Op##AGG_TYPE##Free(noisepage::execution::sql::AGG_TYPE *agg) { agg->~AGG_TYPE(); }

// ---------------------------------------------------------
// TOP K
// ---------------------------------------------------------
GEN_BINARY_AGGREGATE(BoolVal, BooleanTopKAggregate);
GEN_BINARY_AGGREGATE(Integer, IntegerTopKAggregate);
GEN_BINARY_AGGREGATE(Real, RealTopKAggregate);
GEN_BINARY_AGGREGATE(DecimalVal, DecimalTopKAggregate);
GEN_BINARY_AGGREGATE(StringVal, StringTopKAggregate);
GEN_BINARY_AGGREGATE(DateVal, DateTopKAggregate);
GEN_BINARY_AGGREGATE(TimestampVal, TimestampTopKAggregate);

// ---------------------------------------------------------
// Histogram
// ---------------------------------------------------------
GEN_BINARY_AGGREGATE(BoolVal, BooleanHistogramAggregate);
GEN_BINARY_AGGREGATE(Integer, IntegerHistogramAggregate);
GEN_BINARY_AGGREGATE(Real, RealHistogramAggregate);
GEN_BINARY_AGGREGATE(DecimalVal, DecimalHistogramAggregate);
GEN_BINARY_AGGREGATE(StringVal, StringHistogramAggregate);
GEN_BINARY_AGGREGATE(DateVal, DateHistogramAggregate);
GEN_BINARY_AGGREGATE(TimestampVal, TimestampHistogramAggregate);

#undef GEN_BINARY_AGGREGATE

// ---------------------------------------------------------
// Hash Joins
// ---------------------------------------------------------

VM_OP void OpJoinHashTableInit(noisepage::execution::sql::JoinHashTable *join_hash_table,
                               noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t tuple_size);

VM_OP_HOT void OpJoinHashTableAllocTuple(noisepage::byte **result,
                                         noisepage::execution::sql::JoinHashTable *join_hash_table,
                                         noisepage::hash_t hash) {
  *result = join_hash_table->AllocInputTuple(hash);
}

VM_OP_HOT void OpJoinHashTableGetTupleCount(uint32_t *result,
                                            noisepage::execution::sql::JoinHashTable *join_hash_table) {
  *result = join_hash_table->GetTupleCount();
}

VM_OP void OpJoinHashTableBuild(noisepage::execution::sql::JoinHashTable *join_hash_table);

VM_OP void OpJoinHashTableBuildParallel(noisepage::execution::sql::JoinHashTable *join_hash_table,
                                        noisepage::execution::sql::ThreadStateContainer *thread_state_container,
                                        uint32_t jht_offset);

VM_OP_HOT void OpJoinHashTableLookup(noisepage::execution::sql::JoinHashTable *join_hash_table,
                                     noisepage::execution::sql::HashTableEntryIterator *ht_entry_iter,
                                     const noisepage::hash_t hash_val) {
  *ht_entry_iter = join_hash_table->Lookup<false>(hash_val);
}

VM_OP void OpJoinHashTableFree(noisepage::execution::sql::JoinHashTable *join_hash_table);

VM_OP_HOT void OpHashTableEntryIteratorHasNext(bool *has_next,
                                               noisepage::execution::sql::HashTableEntryIterator *ht_entry_iter) {
  *has_next = ht_entry_iter->HasNext();
}

VM_OP_HOT void OpHashTableEntryIteratorGetRow(const noisepage::byte **row,
                                              noisepage::execution::sql::HashTableEntryIterator *ht_entry_iter) {
  *row = ht_entry_iter->GetMatchPayload();
}

VM_OP void OpJoinHashTableIteratorInit(noisepage::execution::sql::JoinHashTableIterator *iter,
                                       noisepage::execution::sql::JoinHashTable *join_hash_table);

VM_OP_HOT void OpJoinHashTableIteratorHasNext(bool *has_more, noisepage::execution::sql::JoinHashTableIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpJoinHashTableIteratorNext(noisepage::execution::sql::JoinHashTableIterator *iter) { iter->Next(); }

VM_OP_HOT void OpJoinHashTableIteratorGetRow(const noisepage::byte **row,
                                             noisepage::execution::sql::JoinHashTableIterator *iter) {
  *row = iter->GetCurrentRow();
}

VM_OP void OpJoinHashTableIteratorFree(noisepage::execution::sql::JoinHashTableIterator *iter);

// ---------------------------------------------------------
// Sorting
// ---------------------------------------------------------

VM_OP void OpSorterInit(noisepage::execution::sql::Sorter *sorter,
                        noisepage::execution::exec::ExecutionContext *exec_ctx,
                        noisepage::execution::sql::Sorter::ComparisonFunction cmp_fn, uint32_t tuple_size);

VM_OP_HOT void OpSorterGetTupleCount(uint32_t *result, noisepage::execution::sql::Sorter *sorter) {
  *result = sorter->GetTupleCount();
}

VM_OP_HOT void OpSorterAllocTuple(noisepage::byte **result, noisepage::execution::sql::Sorter *sorter) {
  *result = sorter->AllocInputTuple();
}

VM_OP_HOT void OpSorterAllocTupleTopK(noisepage::byte **result, noisepage::execution::sql::Sorter *sorter,
                                      uint64_t top_k) {
  *result = sorter->AllocInputTupleTopK(top_k);
}

VM_OP_HOT void OpSorterAllocTupleTopKFinish(noisepage::execution::sql::Sorter *sorter, uint64_t top_k) {
  sorter->AllocInputTupleTopKFinish(top_k);
}

VM_OP void OpSorterSort(noisepage::execution::sql::Sorter *sorter);

VM_OP void OpSorterSortParallel(noisepage::execution::sql::Sorter *sorter,
                                noisepage::execution::sql::ThreadStateContainer *thread_state_container,
                                uint32_t sorter_offset);

VM_OP void OpSorterSortTopKParallel(noisepage::execution::sql::Sorter *sorter,
                                    noisepage::execution::sql::ThreadStateContainer *thread_state_container,
                                    uint32_t sorter_offset, uint64_t top_k);

VM_OP void OpSorterFree(noisepage::execution::sql::Sorter *sorter);

VM_OP void OpSorterIteratorInit(noisepage::execution::sql::SorterIterator *iter,
                                noisepage::execution::sql::Sorter *sorter);

VM_OP_HOT void OpSorterIteratorHasNext(bool *has_more, noisepage::execution::sql::SorterIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpSorterIteratorNext(noisepage::execution::sql::SorterIterator *iter) { iter->Next(); }

VM_OP_HOT void OpSorterIteratorGetRow(const noisepage::byte **row, noisepage::execution::sql::SorterIterator *iter) {
  *row = iter->GetRow();
}

VM_OP_WARM void OpSorterIteratorSkipRows(noisepage::execution::sql::SorterIterator *iter, const uint64_t n) {
  iter->AdvanceBy(n);
}

VM_OP void OpSorterIteratorFree(noisepage::execution::sql::SorterIterator *iter);

// ---------------------------------------------------------
// Output
// ---------------------------------------------------------

VM_OP_WARM void OpResultBufferNew(noisepage::execution::exec::OutputBuffer **out,
                                  noisepage::execution::exec::ExecutionContext *ctx) {
  *out = ctx->OutputBufferNew();
}

VM_OP_WARM void OpResultBufferAllocOutputRow(noisepage::byte **result, noisepage::execution::exec::OutputBuffer *out) {
  *result = out->AllocOutputSlot();
}

VM_OP_WARM void OpResultBufferFinalize(noisepage::execution::exec::OutputBuffer *out) { out->Finalize(); }

VM_OP_WARM void OpResultBufferFree(noisepage::execution::exec::OutputBuffer *out) {
  auto *mem_pool = out->GetMemoryPool();
  out->~OutputBuffer();
  mem_pool->Deallocate(out, sizeof(noisepage::execution::exec::OutputBuffer));
}

// ---------------------------------------------------------
// CSV Reader
// ---------------------------------------------------------

#if 0
VM_OP void OpCSVReaderInit(noisepage::execution::util::CSVReader *reader, const uint8_t *file_name, uint32_t len);

VM_OP void OpCSVReaderPerformInit(bool *result, noisepage::execution::util::CSVReader *reader);

VM_OP_WARM void OpCSVReaderAdvance(bool *has_more, noisepage::execution::util::CSVReader *reader) {
  *has_more = reader->Advance();
}

VM_OP_WARM void OpCSVReaderGetField(noisepage::execution::util::CSVReader *reader, const uint32_t field_index,
                                    noisepage::execution::sql::StringVal *result) {
  // TODO(pmenon): There's an extra copy here. Revisit if it's a performance issue.
  const std::string field_string = reader->GetRowCell(field_index)->AsString();
  *result = noisepage::execution::sql::StringVal(noisepage::storage::VarlenEntry::Create(field_string));
}

VM_OP_WARM void OpCSVReaderGetRecordNumber(uint32_t *result, noisepage::execution::util::CSVReader *reader) {
  *result = reader->GetRecordNumber();
}

VM_OP void OpCSVReaderClose(noisepage::execution::util::CSVReader *reader);
#endif

// ---------------------------------------------------------
// Trig functions
// ---------------------------------------------------------

VM_OP_WARM void OpPi(noisepage::execution::sql::Real *result) {
  noisepage::execution::sql::ArithmeticFunctions::Pi(result);
}

VM_OP_WARM void OpE(noisepage::execution::sql::Real *result) {
  noisepage::execution::sql::ArithmeticFunctions::E(result);
}

VM_OP_WARM void OpAcos(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *input) {
  noisepage::execution::sql::ArithmeticFunctions::Acos(result, *input);
}

VM_OP_WARM void OpAsin(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *input) {
  noisepage::execution::sql::ArithmeticFunctions::Asin(result, *input);
}

VM_OP_WARM void OpAtan(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *input) {
  noisepage::execution::sql::ArithmeticFunctions::Atan(result, *input);
}

VM_OP_WARM void OpAtan2(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *arg_1,
                        const noisepage::execution::sql::Real *arg_2) {
  noisepage::execution::sql::ArithmeticFunctions::Atan2(result, *arg_1, *arg_2);
}

VM_OP_WARM void OpCos(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *input) {
  noisepage::execution::sql::ArithmeticFunctions::Cos(result, *input);
}

VM_OP_WARM void OpCot(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *input) {
  noisepage::execution::sql::ArithmeticFunctions::Cot(result, *input);
}

VM_OP_WARM void OpSin(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *input) {
  noisepage::execution::sql::ArithmeticFunctions::Sin(result, *input);
}

VM_OP_WARM void OpTan(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *input) {
  noisepage::execution::sql::ArithmeticFunctions::Tan(result, *input);
}

VM_OP_WARM void OpCosh(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Cosh(result, *v);
}

VM_OP_WARM void OpTanh(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Tanh(result, *v);
}

VM_OP_WARM void OpSinh(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Sinh(result, *v);
}

VM_OP_WARM void OpSqrt(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Sqrt(result, *v);
}

VM_OP_WARM void OpCbrt(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Cbrt(result, *v);
}

VM_OP_WARM void OpExp(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Exp(result, *v);
}

VM_OP_WARM void OpCeil(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Ceil(result, *v);
}

VM_OP_WARM void OpFloor(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Floor(result, *v);
}

VM_OP_WARM void OpTruncate(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Truncate(result, *v);
}

VM_OP_WARM void OpLn(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Ln(result, *v);
}

VM_OP_WARM void OpLog2(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Log2(result, *v);
}

VM_OP_WARM void OpLog10(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Log10(result, *v);
}

VM_OP_WARM void OpSign(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Sign(result, *v);
}

VM_OP_WARM void OpRadians(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Radians(result, *v);
}

VM_OP_WARM void OpDegrees(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Degrees(result, *v);
}

VM_OP_WARM void OpRound(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v) {
  noisepage::execution::sql::ArithmeticFunctions::Round(result, *v);
}

VM_OP_WARM void OpRound2(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *v,
                         const noisepage::execution::sql::Integer *precision) {
  noisepage::execution::sql::ArithmeticFunctions::Round2(result, *v, *precision);
}

VM_OP_WARM void OpLog(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *base,
                      const noisepage::execution::sql::Real *val) {
  noisepage::execution::sql::ArithmeticFunctions::Log(result, *base, *val);
}

VM_OP_WARM void OpPow(noisepage::execution::sql::Real *result, const noisepage::execution::sql::Real *base,
                      const noisepage::execution::sql::Real *val) {
  noisepage::execution::sql::ArithmeticFunctions::Pow(result, *base, *val);
}

// ---------------------------------------------------------
// Atomic memory operations
// ---------------------------------------------------------

#define ATOMIC(OP, DEST, VAL) (__atomic_fetch_##OP((DEST), (VAL), __ATOMIC_SEQ_CST))

// NOLINTNEXTLINE (clang-tidy incorrectly thinks "dest" should be a pointer to a const)
VM_OP_HOT void OpAtomicAnd1(uint8_t *ret, uint8_t *dest, uint8_t val) { *ret = ATOMIC(and, dest, val); }

// NOLINTNEXTLINE (clang-tidy incorrectly thinks "dest" should be a pointer to a const)
VM_OP_HOT void OpAtomicAnd2(uint16_t *ret, uint16_t *dest, uint16_t val) { *ret = ATOMIC(and, dest, val); }

// NOLINTNEXTLINE (clang-tidy incorrectly thinks "dest" should be a pointer to a const)
VM_OP_HOT void OpAtomicAnd4(uint32_t *ret, uint32_t *dest, uint32_t val) { *ret = ATOMIC(and, dest, val); }

// NOLINTNEXTLINE (clang-tidy incorrectly thinks "dest" should be a pointer to a const)
VM_OP_HOT void OpAtomicAnd8(uint64_t *ret, uint64_t *dest, uint64_t val) { *ret = ATOMIC(and, dest, val); }

// NOLINTNEXTLINE (clang-tidy incorrectly thinks "dest" should be a pointer to a const)
VM_OP_HOT void OpAtomicOr1(uint8_t *ret, uint8_t *dest, uint8_t val) { *ret = ATOMIC(or, dest, val); }

// NOLINTNEXTLINE (clang-tidy incorrectly thinks "dest" should be a pointer to a const)
VM_OP_HOT void OpAtomicOr2(uint16_t *ret, uint16_t *dest, uint16_t val) { *ret = ATOMIC(or, dest, val); }

// NOLINTNEXTLINE (clang-tidy incorrectly thinks "dest" should be a pointer to a const)
VM_OP_HOT void OpAtomicOr4(uint32_t *ret, uint32_t *dest, uint32_t val) { *ret = ATOMIC(or, dest, val); }

// NOLINTNEXTLINE (clang-tidy incorrectly thinks "dest" should be a pointer to a const)
VM_OP_HOT void OpAtomicOr8(uint64_t *ret, uint64_t *dest, uint64_t val) { *ret = ATOMIC(or, dest, val); }

#undef ATOMIC

#define CMPXCHG(DEST, EXPECTED, DESIRED) \
  (__atomic_compare_exchange_n((DEST), (EXPECTED), (DESIRED), false, __ATOMIC_SEQ_CST, __ATOMIC_RELAXED))

// NOLINTNEXTLINE (clang-tidy incorrectly thinks "dest" and "expected" should be pointers to const)
VM_OP_HOT void OpAtomicCompareExchange1(bool *ret, uint8_t *dest, uint8_t *expected, uint8_t desired) {
  *ret = CMPXCHG(dest, expected, desired);
}

// NOLINTNEXTLINE (clang-tidy incorrectly thinks "dest" and "expected" should be pointers to const)
VM_OP_HOT void OpAtomicCompareExchange2(bool *ret, uint16_t *dest, uint16_t *expected, uint16_t desired) {
  *ret = CMPXCHG(dest, expected, desired);
}

// NOLINTNEXTLINE (clang-tidy incorrectly thinks "dest" and "expected" should be pointers to const)
VM_OP_HOT void OpAtomicCompareExchange4(bool *ret, uint32_t *dest, uint32_t *expected, uint32_t desired) {
  *ret = CMPXCHG(dest, expected, desired);
}

// NOLINTNEXTLINE (clang-tidy incorrectly thinks "dest" and "expected" should be pointers to const)
VM_OP_HOT void OpAtomicCompareExchange8(bool *ret, uint64_t *dest, uint64_t *expected, uint64_t desired) {
  *ret = CMPXCHG(dest, expected, desired);
}

#undef CMPXCHG
// ---------------------------------------------------------
// Null/Not Null predicates
// ---------------------------------------------------------

VM_OP_WARM void OpValIsNull(bool *result, const noisepage::execution::sql::Val *val) {
  *result = noisepage::execution::sql::IsNullPredicate::IsNull(*val);
}

VM_OP_WARM void OpValIsNotNull(bool *result, const noisepage::execution::sql::Val *val) {
  *result = noisepage::execution::sql::IsNullPredicate::IsNotNull(*val);
}

// ---------------------------------------------------------
// Internal mini-runner functions
// ---------------------------------------------------------

VM_OP_WARM void OpNpRunnersEmitInt(noisepage::execution::exec::ExecutionContext *ctx,
                                   const noisepage::execution::sql::Integer *num_tuples,
                                   const noisepage::execution::sql::Integer *num_cols,
                                   const noisepage::execution::sql::Integer *num_int_cols,
                                   const noisepage::execution::sql::Integer *num_real_cols) {
  noisepage::execution::sql::MiniRunnersFunctions::EmitTuples(ctx, *num_tuples, *num_cols, *num_int_cols,
                                                              *num_real_cols);
}

VM_OP_WARM void OpNpRunnersEmitReal(noisepage::execution::exec::ExecutionContext *ctx,
                                    const noisepage::execution::sql::Integer *num_tuples,
                                    const noisepage::execution::sql::Integer *num_cols,
                                    const noisepage::execution::sql::Integer *num_int_cols,
                                    const noisepage::execution::sql::Integer *num_real_cols) {
  noisepage::execution::sql::MiniRunnersFunctions::EmitTuples(ctx, *num_tuples, *num_cols, *num_int_cols,
                                                              *num_real_cols);
}

VM_OP_WARM void OpNpRunnersDummyInt(UNUSED_ATTRIBUTE noisepage::execution::exec::ExecutionContext *ctx) {}

VM_OP_WARM void OpNpRunnersDummyReal(UNUSED_ATTRIBUTE noisepage::execution::exec::ExecutionContext *ctx) {}

// ---------------------------------------------------------
// String functions
// ---------------------------------------------------------
VM_OP_WARM void OpChr(noisepage::execution::sql::StringVal *result, noisepage::execution::exec::ExecutionContext *ctx,
                      const noisepage::execution::sql::Integer *n) {
  noisepage::execution::sql::StringFunctions::Chr(result, ctx, *n);
}

VM_OP_WARM void OpASCII(noisepage::execution::sql::Integer *result, noisepage::execution::exec::ExecutionContext *ctx,
                        const noisepage::execution::sql::StringVal *str) {
  noisepage::execution::sql::StringFunctions::ASCII(result, ctx, *str);
}

VM_OP_WARM void OpCharLength(noisepage::execution::sql::Integer *result,
                             noisepage::execution::exec::ExecutionContext *ctx,
                             const noisepage::execution::sql::StringVal *str) {
  noisepage::execution::sql::StringFunctions::CharLength(result, ctx, *str);
}

VM_OP_WARM void OpConcat(noisepage::execution::sql::StringVal *result,
                         noisepage::execution::exec::ExecutionContext *ctx,
                         const noisepage::execution::sql::StringVal *inputs[], const int64_t num_inputs) {
  noisepage::execution::sql::StringFunctions::Concat(result, ctx, inputs, num_inputs);
}

VM_OP_WARM void OpLeft(noisepage::execution::sql::StringVal *result, noisepage::execution::exec::ExecutionContext *ctx,
                       const noisepage::execution::sql::StringVal *str, const noisepage::execution::sql::Integer *n) {
  noisepage::execution::sql::StringFunctions::Left(result, ctx, *str, *n);
}

VM_OP_WARM void OpLike(noisepage::execution::sql::BoolVal *result, const noisepage::execution::sql::StringVal *str,
                       const noisepage::execution::sql::StringVal *pattern) {
  noisepage::execution::sql::StringFunctions::Like(result, nullptr, *str, *pattern);
}

VM_OP_WARM void OpLength(noisepage::execution::sql::Integer *result, noisepage::execution::exec::ExecutionContext *ctx,
                         const noisepage::execution::sql::StringVal *str) {
  noisepage::execution::sql::StringFunctions::Length(result, ctx, *str);
}

VM_OP_WARM void OpLower(noisepage::execution::sql::StringVal *result, noisepage::execution::exec::ExecutionContext *ctx,
                        const noisepage::execution::sql::StringVal *str) {
  noisepage::execution::sql::StringFunctions::Lower(result, ctx, *str);
}

VM_OP_WARM void OpPosition(noisepage::execution::sql::Integer *result,
                           noisepage::execution::exec::ExecutionContext *ctx,
                           const noisepage::execution::sql::StringVal *search_str,
                           const noisepage::execution::sql::StringVal *search_sub_str) {
  noisepage::execution::sql::StringFunctions::Position(result, ctx, *search_str, *search_sub_str);
}

VM_OP_WARM void OpLPad3Arg(noisepage::execution::sql::StringVal *result,
                           noisepage::execution::exec::ExecutionContext *ctx,
                           const noisepage::execution::sql::StringVal *str,
                           const noisepage::execution::sql::Integer *len,
                           const noisepage::execution::sql::StringVal *pad) {
  noisepage::execution::sql::StringFunctions::Lpad(result, ctx, *str, *len, *pad);
}

VM_OP_WARM void OpLPad2Arg(noisepage::execution::sql::StringVal *result,
                           noisepage::execution::exec::ExecutionContext *ctx,
                           const noisepage::execution::sql::StringVal *str,
                           const noisepage::execution::sql::Integer *len) {
  noisepage::execution::sql::StringFunctions::Lpad(result, ctx, *str, *len);
}

VM_OP_WARM void OpLTrim2Arg(noisepage::execution::sql::StringVal *result,
                            noisepage::execution::exec::ExecutionContext *ctx,
                            const noisepage::execution::sql::StringVal *str,
                            const noisepage::execution::sql::StringVal *chars) {
  noisepage::execution::sql::StringFunctions::Ltrim(result, ctx, *str, *chars);
}

VM_OP_WARM void OpLTrim1Arg(noisepage::execution::sql::StringVal *result,
                            noisepage::execution::exec::ExecutionContext *ctx,
                            const noisepage::execution::sql::StringVal *str) {
  noisepage::execution::sql::StringFunctions::Ltrim(result, ctx, *str);
}

VM_OP_WARM void OpRepeat(noisepage::execution::sql::StringVal *result,
                         noisepage::execution::exec::ExecutionContext *ctx,
                         const noisepage::execution::sql::StringVal *str, const noisepage::execution::sql::Integer *n) {
  noisepage::execution::sql::StringFunctions::Repeat(result, ctx, *str, *n);
}

VM_OP_WARM void OpReverse(noisepage::execution::sql::StringVal *result,
                          noisepage::execution::exec::ExecutionContext *ctx,
                          const noisepage::execution::sql::StringVal *str) {
  noisepage::execution::sql::StringFunctions::Reverse(result, ctx, *str);
}

VM_OP_WARM void OpRight(noisepage::execution::sql::StringVal *result, noisepage::execution::exec::ExecutionContext *ctx,
                        const noisepage::execution::sql::StringVal *str, const noisepage::execution::sql::Integer *n) {
  noisepage::execution::sql::StringFunctions::Right(result, ctx, *str, *n);
}

VM_OP_WARM void OpRPad3Arg(noisepage::execution::sql::StringVal *result,
                           noisepage::execution::exec::ExecutionContext *ctx,
                           const noisepage::execution::sql::StringVal *str,
                           const noisepage::execution::sql::Integer *len,
                           const noisepage::execution::sql::StringVal *pad) {
  noisepage::execution::sql::StringFunctions::Rpad(result, ctx, *str, *len, *pad);
}

VM_OP_WARM void OpRPad2Arg(noisepage::execution::sql::StringVal *result,
                           noisepage::execution::exec::ExecutionContext *ctx,
                           const noisepage::execution::sql::StringVal *str,
                           const noisepage::execution::sql::Integer *len) {
  noisepage::execution::sql::StringFunctions::Rpad(result, ctx, *str, *len);
}

VM_OP_WARM void OpRTrim2Arg(noisepage::execution::sql::StringVal *result,
                            noisepage::execution::exec::ExecutionContext *ctx,
                            const noisepage::execution::sql::StringVal *str,
                            const noisepage::execution::sql::StringVal *chars) {
  noisepage::execution::sql::StringFunctions::Rtrim(result, ctx, *str, *chars);
}

VM_OP_WARM void OpRTrim1Arg(noisepage::execution::sql::StringVal *result,
                            noisepage::execution::exec::ExecutionContext *ctx,
                            const noisepage::execution::sql::StringVal *str) {
  noisepage::execution::sql::StringFunctions::Rtrim(result, ctx, *str);
}

VM_OP_WARM void OpSplitPart(noisepage::execution::sql::StringVal *result,
                            noisepage::execution::exec::ExecutionContext *ctx,
                            const noisepage::execution::sql::StringVal *str,
                            const noisepage::execution::sql::StringVal *delim,
                            const noisepage::execution::sql::Integer *field) {
  noisepage::execution::sql::StringFunctions::SplitPart(result, ctx, *str, *delim, *field);
}

VM_OP_WARM void OpSubstring(noisepage::execution::sql::StringVal *result,
                            noisepage::execution::exec::ExecutionContext *ctx,
                            const noisepage::execution::sql::StringVal *str,
                            const noisepage::execution::sql::Integer *pos,
                            const noisepage::execution::sql::Integer *len) {
  noisepage::execution::sql::StringFunctions::Substring(result, ctx, *str, *pos, *len);
}

VM_OP_WARM void OpStartsWith(noisepage::execution::sql::BoolVal *result,
                             noisepage::execution::exec::ExecutionContext *ctx,
                             const noisepage::execution::sql::StringVal *str,
                             const noisepage::execution::sql::StringVal *start) {
  noisepage::execution::sql::StringFunctions::StartsWith(result, ctx, *str, *start);
}

VM_OP_WARM void OpTrim(noisepage::execution::sql::StringVal *result, noisepage::execution::exec::ExecutionContext *ctx,
                       const noisepage::execution::sql::StringVal *str) {
  noisepage::execution::sql::StringFunctions::Trim(result, ctx, *str);
}

VM_OP_WARM void OpTrim2(noisepage::execution::sql::StringVal *result, noisepage::execution::exec::ExecutionContext *ctx,
                        const noisepage::execution::sql::StringVal *str,
                        const noisepage::execution::sql::StringVal *chars) {
  noisepage::execution::sql::StringFunctions::Trim(result, ctx, *str, *chars);
}

VM_OP_WARM void OpUpper(noisepage::execution::sql::StringVal *result, noisepage::execution::exec::ExecutionContext *ctx,
                        const noisepage::execution::sql::StringVal *str) {
  noisepage::execution::sql::StringFunctions::Upper(result, ctx, *str);
}

VM_OP_WARM void OpVersion(noisepage::execution::exec::ExecutionContext *ctx,
                          noisepage::execution::sql::StringVal *result) {
  noisepage::execution::sql::SystemFunctions::Version(ctx, result);
}

VM_OP_WARM void OpInitCap(noisepage::execution::sql::StringVal *result,
                          noisepage::execution::exec::ExecutionContext *ctx,
                          const noisepage::execution::sql::StringVal *str) {
  noisepage::execution::sql::StringFunctions::InitCap(result, ctx, *str);
}

// ---------------------------------------------------------------
// Index Iterator
// ---------------------------------------------------------------
VM_OP void OpIndexIteratorInit(noisepage::execution::sql::IndexIterator *iter,
                               noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t num_attrs,
                               uint32_t table_oid, uint32_t index_oid, uint32_t *col_oids, uint32_t num_oids);
VM_OP void OpIndexIteratorFree(noisepage::execution::sql::IndexIterator *iter);

VM_OP void OpIndexIteratorGetSize(uint32_t *index_size, noisepage::execution::sql::IndexIterator *iter);

VM_OP void OpIndexIteratorPerformInit(noisepage::execution::sql::IndexIterator *iter);

VM_OP_WARM void OpIndexIteratorScanKey(noisepage::execution::sql::IndexIterator *iter) { iter->ScanKey(); }

VM_OP_WARM void OpIndexIteratorScanAscending(noisepage::execution::sql::IndexIterator *iter,
                                             noisepage::storage::index::ScanType scan_type, uint32_t limit) {
  iter->ScanAscending(scan_type, limit);
}

VM_OP_WARM void OpIndexIteratorScanDescending(noisepage::execution::sql::IndexIterator *iter) {
  iter->ScanDescending();
}

VM_OP_WARM void OpIndexIteratorScanLimitDescending(noisepage::execution::sql::IndexIterator *iter, uint32_t limit) {
  iter->ScanLimitDescending(limit);
}

VM_OP_WARM void OpIndexIteratorAdvance(bool *has_more, noisepage::execution::sql::IndexIterator *iter) {
  *has_more = iter->Advance();
}

VM_OP_WARM void OpIndexIteratorGetPR(noisepage::storage::ProjectedRow **pr,
                                     noisepage::execution::sql::IndexIterator *iter) {
  *pr = iter->PR();
}

VM_OP_WARM void OpIndexIteratorGetLoPR(noisepage::storage::ProjectedRow **pr,
                                       noisepage::execution::sql::IndexIterator *iter) {
  *pr = iter->LoPR();
}

VM_OP_WARM void OpIndexIteratorGetHiPR(noisepage::storage::ProjectedRow **pr,
                                       noisepage::execution::sql::IndexIterator *iter) {
  *pr = iter->HiPR();
}

VM_OP_WARM void OpIndexIteratorGetTablePR(noisepage::storage::ProjectedRow **pr,
                                          noisepage::execution::sql::IndexIterator *iter) {
  *pr = iter->TablePR();
}

VM_OP_WARM void OpIndexIteratorGetSlot(noisepage::storage::TupleSlot *slot,
                                       noisepage::execution::sql::IndexIterator *iter) {
  *slot = iter->CurrentSlot();
}

#define GEN_PR_SCALAR_SET_CALLS(Name, SqlType, CppType)                                      \
  VM_OP_HOT void OpPRSet##Name(noisepage::storage::ProjectedRow *pr, uint16_t col_idx,       \
                               noisepage::execution::sql::SqlType *val) {                    \
    pr->Set<CppType, false>(col_idx, static_cast<CppType>(val->val_), val->is_null_);        \
  }                                                                                          \
                                                                                             \
  VM_OP_HOT void OpPRSet##Name##Null(noisepage::storage::ProjectedRow *pr, uint16_t col_idx, \
                                     noisepage::execution::sql::SqlType *val) {              \
    pr->Set<CppType, true>(col_idx, static_cast<CppType>(val->val_), val->is_null_);         \
  }

#define GEN_PR_SCALAR_GET_CALLS(Name, SqlType, CppType)                                                             \
  VM_OP_HOT void OpPRGet##Name(noisepage::execution::sql::SqlType *out, noisepage::storage::ProjectedRow *pr,       \
                               uint16_t col_idx) {                                                                  \
    /* Read */                                                                                                      \
    auto *ptr = pr->Get<CppType, false>(col_idx, nullptr);                                                          \
    NOISEPAGE_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");                                   \
    /* Set */                                                                                                       \
    out->is_null_ = false;                                                                                          \
    out->val_ = *ptr;                                                                                               \
  }                                                                                                                 \
                                                                                                                    \
  VM_OP_HOT void OpPRGet##Name##Null(noisepage::execution::sql::SqlType *out, noisepage::storage::ProjectedRow *pr, \
                                     uint16_t col_idx) {                                                            \
    /* Read */                                                                                                      \
    bool null = false;                                                                                              \
    auto *ptr = pr->Get<CppType, true>(col_idx, &null);                                                             \
    /* Set */                                                                                                       \
    out->is_null_ = null;                                                                                           \
    out->val_ = null ? 0 : *ptr;                                                                                    \
  }

GEN_PR_SCALAR_SET_CALLS(Bool, BoolVal, bool);
GEN_PR_SCALAR_SET_CALLS(TinyInt, Integer, int8_t);
GEN_PR_SCALAR_SET_CALLS(SmallInt, Integer, int16_t);
GEN_PR_SCALAR_SET_CALLS(Int, Integer, int32_t);
GEN_PR_SCALAR_SET_CALLS(BigInt, Integer, int64_t);
GEN_PR_SCALAR_SET_CALLS(Real, Real, float);
GEN_PR_SCALAR_SET_CALLS(Double, Real, double);
GEN_PR_SCALAR_GET_CALLS(Bool, BoolVal, bool);
GEN_PR_SCALAR_GET_CALLS(TinyInt, Integer, int8_t);
GEN_PR_SCALAR_GET_CALLS(SmallInt, Integer, int16_t);
GEN_PR_SCALAR_GET_CALLS(Int, Integer, int32_t);
GEN_PR_SCALAR_GET_CALLS(BigInt, Integer, int64_t);
GEN_PR_SCALAR_GET_CALLS(Real, Real, float);
GEN_PR_SCALAR_GET_CALLS(Double, Real, double);

VM_OP_HOT void OpPRSetVarlen(noisepage::storage::ProjectedRow *pr, uint16_t col_idx,
                             noisepage::execution::sql::StringVal *val, bool own) {
  const auto varlen = noisepage::execution::sql::StringVal::CreateVarlen(*val, own);
  pr->Set<noisepage::storage::VarlenEntry, false>(col_idx, varlen, val->is_null_);
}

VM_OP_HOT void OpPRSetVarlenNull(noisepage::storage::ProjectedRow *pr, uint16_t col_idx,
                                 noisepage::execution::sql::StringVal *val, bool own) {
  const auto varlen = noisepage::execution::sql::StringVal::CreateVarlen(*val, own);
  pr->Set<noisepage::storage::VarlenEntry, true>(col_idx, varlen, val->is_null_);
}

VM_OP_HOT void OpPRSetDateVal(noisepage::storage::ProjectedRow *pr, uint16_t col_idx,
                              noisepage::execution::sql::DateVal *val) {
  auto pr_val = val->val_.ToNative();
  pr->Set<uint32_t, false>(col_idx, pr_val, val->is_null_);
}

VM_OP_HOT void OpPRSetDateValNull(noisepage::storage::ProjectedRow *pr, uint16_t col_idx,
                                  noisepage::execution::sql::DateVal *val) {
  auto pr_val = val->is_null_ ? 0 : val->val_.ToNative();
  pr->Set<uint32_t, true>(col_idx, pr_val, val->is_null_);
}

VM_OP_HOT void OpPRSetTimestampVal(noisepage::storage::ProjectedRow *pr, uint16_t col_idx,
                                   noisepage::execution::sql::TimestampVal *val) {
  auto pr_val = val->val_.ToNative();
  pr->Set<uint64_t, false>(col_idx, pr_val, val->is_null_);
}

VM_OP_HOT void OpPRSetTimestampValNull(noisepage::storage::ProjectedRow *pr, uint16_t col_idx,
                                       noisepage::execution::sql::TimestampVal *val) {
  auto pr_val = val->is_null_ ? 0 : val->val_.ToNative();
  pr->Set<uint64_t, true>(col_idx, pr_val, val->is_null_);
}

VM_OP_HOT void OpPRGetDateVal(noisepage::execution::sql::DateVal *out, noisepage::storage::ProjectedRow *pr,
                              uint16_t col_idx) {
  // Read
  auto *ptr = pr->Get<uint32_t, false>(col_idx, nullptr);
  NOISEPAGE_ASSERT(ptr != nullptr, "Null pointer when trying to read date");
  // Set
  out->is_null_ = false;
  out->val_ = noisepage::execution::sql::Date::FromNative(*ptr);
}

VM_OP_HOT void OpPRGetDateValNull(noisepage::execution::sql::DateVal *out, noisepage::storage::ProjectedRow *pr,
                                  uint16_t col_idx) {
  // Read
  bool null = false;
  auto *ptr = pr->Get<uint32_t, true>(col_idx, &null);

  // Set
  out->is_null_ = null;
  out->val_ = null ? noisepage::execution::sql::Date() : noisepage::execution::sql::Date::FromNative(*ptr);
}

VM_OP_HOT void OpPRGetTimestampVal(noisepage::execution::sql::TimestampVal *out, noisepage::storage::ProjectedRow *pr,
                                   uint16_t col_idx) {
  // Read
  auto *ptr = pr->Get<uint64_t, false>(col_idx, nullptr);
  NOISEPAGE_ASSERT(ptr != nullptr, "Null pointer when trying to read date");

  // Set
  out->is_null_ = false;
  out->val_ = noisepage::execution::sql::Timestamp::FromNative(*ptr);
}

VM_OP_HOT void OpPRGetTimestampValNull(noisepage::execution::sql::TimestampVal *out,
                                       noisepage::storage::ProjectedRow *pr, uint16_t col_idx) {
  // Read
  bool null = false;
  auto *ptr = pr->Get<uint64_t, true>(col_idx, &null);

  // Set
  out->is_null_ = null;
  out->val_ = null ? noisepage::execution::sql::Timestamp() : noisepage::execution::sql::Timestamp::FromNative(*ptr);
}

VM_OP_HOT void OpPRGetVarlen(noisepage::execution::sql::StringVal *out, noisepage::storage::ProjectedRow *pr,
                             uint16_t col_idx) {
  // Read
  auto *varlen = pr->Get<noisepage::storage::VarlenEntry, false>(col_idx, nullptr);
  NOISEPAGE_ASSERT(varlen != nullptr, "Null pointer when trying to read varlen");

  // Set
  *out = noisepage::execution::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
}

VM_OP_HOT void OpPRGetVarlenNull(noisepage::execution::sql::StringVal *out, noisepage::storage::ProjectedRow *pr,
                                 uint16_t col_idx) {
  // Read
  bool null = false;
  auto *varlen = pr->Get<noisepage::storage::VarlenEntry, true>(col_idx, &null);

  // Set
  if (null) {
    out->is_null_ = null;
  } else {
    *out = noisepage::execution::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
  }
}

// ---------------------------------------------------------------
// StorageInterface Calls
// ---------------------------------------------------------------

VM_OP void OpStorageInterfaceInit(noisepage::execution::sql::StorageInterface *storage_interface,
                                  noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t table_oid,
                                  uint32_t *col_oids, uint32_t num_oids, bool need_indexes);

VM_OP void OpStorageInterfaceGetTablePR(noisepage::storage::ProjectedRow **pr_result,
                                        noisepage::execution::sql::StorageInterface *storage_interface);

VM_OP void OpStorageInterfaceTableUpdate(bool *result, noisepage::execution::sql::StorageInterface *storage_interface,
                                         noisepage::storage::TupleSlot *tuple_slot);

VM_OP void OpStorageInterfaceTableDelete(bool *result, noisepage::execution::sql::StorageInterface *storage_interface,
                                         noisepage::storage::TupleSlot *tuple_slot);

VM_OP void OpStorageInterfaceTableInsert(noisepage::storage::TupleSlot *tuple_slot,
                                         noisepage::execution::sql::StorageInterface *storage_interface);

VM_OP void OpStorageInterfaceGetIndexPR(noisepage::storage::ProjectedRow **pr_result,
                                        noisepage::execution::sql::StorageInterface *storage_interface,
                                        uint32_t index_oid);

VM_OP void OpStorageInterfaceIndexGetSize(uint32_t *result,
                                          noisepage::execution::sql::StorageInterface *storage_interface);

VM_OP void OpStorageInterfaceGetIndexHeapSize(uint32_t *size,
                                              noisepage::execution::sql::StorageInterface *storage_interface);

VM_OP void OpStorageInterfaceIndexInsert(bool *result, noisepage::execution::sql::StorageInterface *storage_interface);

VM_OP void OpStorageInterfaceIndexInsertUnique(bool *result,
                                               noisepage::execution::sql::StorageInterface *storage_interface);

VM_OP void OpStorageInterfaceIndexInsertWithSlot(bool *result,
                                                 noisepage::execution::sql::StorageInterface *storage_interface,
                                                 noisepage::storage::TupleSlot *tuple_slot, bool unique);

VM_OP void OpStorageInterfaceIndexDelete(noisepage::execution::sql::StorageInterface *storage_interface,
                                         noisepage::storage::TupleSlot *tuple_slot);

VM_OP void OpStorageInterfaceFree(noisepage::execution::sql::StorageInterface *storage_interface);

// ---------------------------------
// Date function
// ---------------------------------

VM_OP_WARM void OpExtractYearFromDate(noisepage::execution::sql::Integer *result,
                                      noisepage::execution::sql::DateVal *input) {
  if (input->is_null_) {
    result->is_null_ = true;
  } else {
    result->is_null_ = false;
    result->val_ = input->val_.ExtractYear();
  }
}

VM_OP_WARM void OpAbortTxn(noisepage::execution::exec::ExecutionContext *exec_ctx) {
  exec_ctx->GetTxn()->SetMustAbort();
  throw noisepage::ABORT_EXCEPTION("transaction aborted");
}

// Parameter calls
#define GEN_SCALAR_PARAM_GET(Name, SqlType)                                                                     \
  VM_OP_HOT void OpGetParam##Name(noisepage::execution::sql::SqlType *ret,                                      \
                                  noisepage::execution::exec::ExecutionContext *exec_ctx, uint32_t param_idx) { \
    const auto &cve = exec_ctx->GetParam(param_idx);                                                            \
    if (cve.IsNull()) {                                                                                         \
      ret->is_null_ = true;                                                                                     \
    } else {                                                                                                    \
      *ret = cve.Get##SqlType();                                                                                \
    }                                                                                                           \
  }

GEN_SCALAR_PARAM_GET(Bool, BoolVal)
GEN_SCALAR_PARAM_GET(TinyInt, Integer)
GEN_SCALAR_PARAM_GET(SmallInt, Integer)
GEN_SCALAR_PARAM_GET(Int, Integer)
GEN_SCALAR_PARAM_GET(BigInt, Integer)
GEN_SCALAR_PARAM_GET(Real, Real)
GEN_SCALAR_PARAM_GET(Double, Real)
GEN_SCALAR_PARAM_GET(DateVal, DateVal)
GEN_SCALAR_PARAM_GET(TimestampVal, TimestampVal)
GEN_SCALAR_PARAM_GET(String, StringVal)
#undef GEN_SCALAR_PARAM_GET

// ---------------------------------
// Testing only functions
// ---------------------------------

VM_OP_WARM void OpTestCatalogLookup(uint32_t *oid_var, noisepage::execution::exec::ExecutionContext *exec_ctx,
                                    const uint8_t *table_name_str, uint32_t table_name_length,
                                    const uint8_t *col_name_str, uint32_t col_name_length) {
  // TODO(WAN): wasteful std::string
  noisepage::execution::sql::StringVal table_name{reinterpret_cast<const char *>(table_name_str), table_name_length};
  noisepage::execution::sql::StringVal col_name{reinterpret_cast<const char *>(col_name_str), col_name_length};
  noisepage::catalog::table_oid_t table_oid =
      exec_ctx->GetAccessor()->GetTableOid(std::string(table_name.StringView()));

  uint32_t out_oid;
  if (col_name.GetLength() == 0) {
    out_oid = table_oid.UnderlyingValue();
  } else {
    const auto &schema = exec_ctx->GetAccessor()->GetSchema(table_oid);
    out_oid = schema.GetColumn(std::string(col_name.StringView())).Oid().UnderlyingValue();
  }

  *oid_var = out_oid;
}

VM_OP_WARM void OpTestCatalogIndexLookup(uint32_t *oid_var, noisepage::execution::exec::ExecutionContext *exec_ctx,
                                         const uint8_t *index_name_str, uint32_t index_name_length) {
  // TODO(WAN): wasteful std::string
  noisepage::execution::sql::StringVal table_name{reinterpret_cast<const char *>(index_name_str), index_name_length};
  noisepage::catalog::index_oid_t index_oid =
      exec_ctx->GetAccessor()->GetIndexOid(std::string(table_name.StringView()));
  *oid_var = index_oid.UnderlyingValue();
}

// Macro hygiene
#undef VM_OP_COLD
#undef VM_OP_WARM
#undef VM_OP_HOT
#undef VM_OP

}  // extern "C"
