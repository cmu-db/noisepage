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

VM_OP_HOT void OpNotSql(terrier::execution::sql::BoolVal *const result, const terrier::execution::sql::BoolVal *input) {
  terrier::execution::sql::ComparisonFunctions::NotBoolVal(result, *input);
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
    TERRIER_ASSERT(rhs != 0, "Division-by-zero error!");                                   \
    *result = lhs / rhs;                                                                   \
  }

ALL_NUMERIC_TYPES(ARITHMETIC);

#undef ARITHMETIC

#define INT_MODULAR(type, ...)                                      \
  /* Primitive modulo-remainder (no zero-check) */                  \
  VM_OP_HOT void OpMod##_##type(type *result, type lhs, type rhs) { \
    TERRIER_ASSERT(rhs != 0, "Division-by-zero error!");            \
    *result = lhs % rhs;                                            \
  }

#define FLOAT_MODULAR(type, ...)                                    \
  /* Primitive modulo-remainder (no zero-check) */                  \
  VM_OP_HOT void OpMod##_##type(type *result, type lhs, type rhs) { \
    TERRIER_ASSERT(rhs != 0, "Division-by-zero error!");            \
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

VM_OP_HOT void OpDerefN(terrier::byte *dest, const terrier::byte *const src, uint32_t len) {
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

VM_OP_HOT void OpLea(terrier::byte **dest, terrier::byte *base, uint32_t offset) { *dest = base + offset; }

VM_OP_HOT void OpLeaScaled(terrier::byte **dest, terrier::byte *base, uint32_t index, uint32_t scale, uint32_t offset) {
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

VM_OP_HOT void OpExecutionContextAddRowsAffected(terrier::execution::exec::ExecutionContext *exec_ctx,
                                                 int64_t rows_affected) {
  exec_ctx->AddRowsAffected(rows_affected);
}

VM_OP_WARM void OpExecutionContextGetMemoryPool(terrier::execution::sql::MemoryPool **const memory,
                                                terrier::execution::exec::ExecutionContext *const exec_ctx) {
  *memory = exec_ctx->GetMemoryPool();
}

VM_OP_HOT void OpExecutionContextStartResourceTracker(terrier::execution::exec::ExecutionContext *const exec_ctx,
                                                      terrier::metrics::MetricsComponent component) {
  exec_ctx->StartResourceTracker(component);
}

VM_OP_HOT void OpExecutionContextSetMemoryUseOverride(terrier::execution::exec::ExecutionContext *const exec_ctx,
                                                      uint32_t memory_use) {
  exec_ctx->SetMemoryUseOverride(memory_use);
}

VM_OP_HOT void OpExecutionContextEndResourceTracker(terrier::execution::exec::ExecutionContext *const exec_ctx,
                                                    const terrier::execution::sql::StringVal &name) {
  exec_ctx->EndResourceTracker(name.GetContent(), name.GetLength());
}

VM_OP_HOT void OpExecutionContextEndPipelineTracker(terrier::execution::exec::ExecutionContext *const exec_ctx,
                                                    terrier::execution::query_id_t query_id,
                                                    terrier::execution::pipeline_id_t pipeline_id) {
  exec_ctx->EndPipelineTracker(query_id, pipeline_id);
}

VM_OP_WARM void OpExecutionContextGetTLS(terrier::execution::sql::ThreadStateContainer **const thread_state_container,
                                         terrier::execution::exec::ExecutionContext *const exec_ctx) {
  *thread_state_container = exec_ctx->GetThreadStateContainer();
}

VM_OP_WARM void OpThreadStateContainerAccessCurrentThreadState(
    terrier::byte **state, terrier::execution::sql::ThreadStateContainer *thread_state_container) {
  *state = thread_state_container->AccessCurrentThreadState();
}

VM_OP_WARM void OpThreadStateContainerReset(terrier::execution::sql::ThreadStateContainer *thread_state_container,
                                            const uint32_t size,
                                            terrier::execution::sql::ThreadStateContainer::InitFn init_fn,
                                            terrier::execution::sql::ThreadStateContainer::DestroyFn destroy_fn,
                                            void *ctx) {
  thread_state_container->Reset(size, init_fn, destroy_fn, ctx);
}

VM_OP_WARM void OpThreadStateContainerIterate(terrier::execution::sql::ThreadStateContainer *thread_state_container,
                                              void *const state,
                                              terrier::execution::sql::ThreadStateContainer::IterateFn iterate_fn) {
  thread_state_container->IterateStates(state, iterate_fn);
}

VM_OP_WARM void OpThreadStateContainerClear(terrier::execution::sql::ThreadStateContainer *thread_state_container) {
  thread_state_container->Clear();
}

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

VM_OP void OpTableVectorIteratorInit(terrier::execution::sql::TableVectorIterator *iter,
                                     terrier::execution::exec::ExecutionContext *exec_ctx, uint32_t table_oid,
                                     uint32_t *col_oids, uint32_t num_oids);

VM_OP void OpTableVectorIteratorPerformInit(terrier::execution::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorNext(bool *has_more, terrier::execution::sql::TableVectorIterator *iter) {
  *has_more = iter->Advance();
}

VM_OP void OpTableVectorIteratorFree(terrier::execution::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorGetVPI(terrier::execution::sql::VectorProjectionIterator **vpi,
                                           terrier::execution::sql::TableVectorIterator *iter) {
  *vpi = iter->GetVectorProjectionIterator();
}

VM_OP_HOT void OpParallelScanTable(uint32_t table_oid, uint32_t *col_oids, uint32_t num_oids, void *const query_state,
                                   terrier::execution::exec::ExecutionContext *exec_ctx,
                                   const terrier::execution::sql::TableVectorIterator::ScanFn scanner) {
  terrier::execution::sql::TableVectorIterator::ParallelScan(table_oid, col_oids, num_oids, query_state, exec_ctx,
                                                             scanner);
}

// ---------------------------------------------------------
// Vector Projection Iterator
// ---------------------------------------------------------

VM_OP void OpVPIInit(terrier::execution::sql::VectorProjectionIterator *vpi,
                     terrier::execution::sql::VectorProjection *vp);

VM_OP void OpVPIInitWithList(terrier::execution::sql::VectorProjectionIterator *vpi,
                             terrier::execution::sql::VectorProjection *vp,
                             terrier::execution::sql::TupleIdList *tid_list);

VM_OP void OpVPIFree(terrier::execution::sql::VectorProjectionIterator *vpi);

VM_OP_HOT void OpVPIIsFiltered(bool *is_filtered, const terrier::execution::sql::VectorProjectionIterator *vpi) {
  *is_filtered = vpi->IsFiltered();
}

VM_OP_HOT void OpVPIGetSelectedRowCount(uint32_t *count, const terrier::execution::sql::VectorProjectionIterator *vpi) {
  *count = vpi->GetSelectedTupleCount();
}

VM_OP_HOT void OpVPIGetVectorProjection(terrier::execution::sql::VectorProjection **vector_projection,
                                        const terrier::execution::sql::VectorProjectionIterator *vpi) {
  *vector_projection = vpi->GetVectorProjection();
}

VM_OP_HOT void OpVPIHasNext(bool *has_more, const terrier::execution::sql::VectorProjectionIterator *vpi) {
  *has_more = vpi->HasNext();
}

VM_OP_HOT void OpVPIHasNextFiltered(bool *has_more, const terrier::execution::sql::VectorProjectionIterator *vpi) {
  *has_more = vpi->HasNextFiltered();
}

VM_OP_HOT void OpVPIAdvance(terrier::execution::sql::VectorProjectionIterator *vpi) { vpi->Advance(); }

VM_OP_HOT void OpVPIAdvanceFiltered(terrier::execution::sql::VectorProjectionIterator *vpi) { vpi->AdvanceFiltered(); }

VM_OP_HOT void OpVPISetPosition(terrier::execution::sql::VectorProjectionIterator *vpi, const uint32_t index) {
  vpi->SetPosition<false>(index);
}

VM_OP_HOT void OpVPISetPositionFiltered(terrier::execution::sql::VectorProjectionIterator *const vpi,
                                        const uint32_t index) {
  vpi->SetPosition<true>(index);
}

VM_OP_HOT void OpVPIMatch(terrier::execution::sql::VectorProjectionIterator *vpi, const bool match) {
  vpi->Match(match);
}

VM_OP_HOT void OpVPIReset(terrier::execution::sql::VectorProjectionIterator *vpi) { vpi->Reset(); }

VM_OP_HOT void OpVPIResetFiltered(terrier::execution::sql::VectorProjectionIterator *vpi) { vpi->ResetFiltered(); }

VM_OP_HOT void OpVPIGetSlot(terrier::storage::TupleSlot *slot, terrier::execution::sql::VectorProjectionIterator *vpi) {
  *slot = vpi->GetCurrentSlot();
}

// ---------------------------------------------------------
// VPI Get
// ---------------------------------------------------------

#define GEN_VPI_GET(Name, SqlValueType, CppType)                                                    \
  VM_OP_HOT void OpVPIGet##Name(terrier::execution::sql::SqlValueType *out,                         \
                                terrier::execution::sql::VectorProjectionIterator *const vpi,       \
                                const uint32_t col_idx) {                                           \
    auto *ptr = vpi->GetValue<CppType, false>(col_idx, nullptr);                                    \
    TERRIER_ASSERT(ptr != nullptr, "Null data pointer when trying to read attribute");              \
    out->is_null_ = false;                                                                          \
    out->val_ = *ptr;                                                                               \
  }                                                                                                 \
  VM_OP_HOT void OpVPIGet##Name##Null(terrier::execution::sql::SqlValueType *out,                   \
                                      terrier::execution::sql::VectorProjectionIterator *const vpi, \
                                      const uint32_t col_idx) {                                     \
    bool null = false;                                                                              \
    auto *ptr = vpi->GetValue<CppType, true>(col_idx, &null);                                       \
    TERRIER_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");                     \
    out->is_null_ = null;                                                                           \
    out->val_ = *ptr;                                                                               \
  }

#define GEN_VPI_SET(Name, SqlValueType, CppType)                                                              \
  VM_OP_HOT void OpVPISet##Name(terrier::execution::sql::VectorProjectionIterator *const vpi,                 \
                                terrier::execution::sql::SqlValueType *input, const uint32_t col_idx) {       \
    vpi->SetValue<CppType, false>(col_idx, input->val_, false);                                               \
  }                                                                                                           \
  VM_OP_HOT void OpVPISet##Name##Null(terrier::execution::sql::VectorProjectionIterator *const vpi,           \
                                      terrier::execution::sql::SqlValueType *input, const uint32_t col_idx) { \
    vpi->SetValue<CppType, true>(col_idx, input->val_, input->is_null_);                                      \
  }

GEN_VPI_GET(Bool, BoolVal, bool);
GEN_VPI_GET(TinyInt, Integer, int8_t);
GEN_VPI_GET(SmallInt, Integer, int16_t);
GEN_VPI_GET(Integer, Integer, int32_t);
GEN_VPI_GET(BigInt, Integer, int64_t);
GEN_VPI_GET(Real, Real, float);
GEN_VPI_GET(Double, Real, double);
GEN_VPI_GET(Decimal, DecimalVal, terrier::execution::sql::Decimal64);
GEN_VPI_GET(Date, DateVal, terrier::execution::sql::Date);
GEN_VPI_GET(Timestamp, TimestampVal, terrier::execution::sql::Timestamp);
GEN_VPI_GET(String, StringVal, terrier::storage::VarlenEntry);

GEN_VPI_SET(Bool, BoolVal, bool);
GEN_VPI_SET(TinyInt, Integer, int8_t);
GEN_VPI_SET(SmallInt, Integer, int16_t);
GEN_VPI_SET(Integer, Integer, int32_t);
GEN_VPI_SET(BigInt, Integer, int64_t);
GEN_VPI_SET(Real, Real, float);
GEN_VPI_SET(Double, Real, double);
GEN_VPI_SET(Decimal, DecimalVal, terrier::execution::sql::Decimal64);
GEN_VPI_SET(Date, DateVal, terrier::execution::sql::Date);
GEN_VPI_SET(Timestamp, TimestampVal, terrier::execution::sql::Timestamp);
GEN_VPI_SET(String, StringVal, terrier::storage::VarlenEntry);

#undef GEN_VPI_SET
#undef GEN_VPI_GET

VM_OP_HOT void OpVPIGetPointer(terrier::byte **out, terrier::execution::sql::VectorProjectionIterator *const vpi,
                               const uint32_t col_idx) {
  auto *ptr = vpi->GetValue<terrier::byte *, false>(col_idx, nullptr);
  TERRIER_ASSERT(ptr != nullptr, "Null data pointer when trying to read attribute");
  *out = *ptr;
}

// ---------------------------------------------------------
// Hashing
// ---------------------------------------------------------

VM_OP_HOT void OpHashInt(terrier::hash_t *const hash_val, const terrier::execution::sql::Integer *const input,
                         const terrier::hash_t seed) {
  *hash_val = input->is_null_ ? 0 : terrier::common::HashUtil::HashCrc(input->val_, seed);
}

VM_OP_HOT void OpHashReal(terrier::hash_t *const hash_val, const terrier::execution::sql::Real *const input,
                          const terrier::hash_t seed) {
  *hash_val = input->is_null_ ? 0 : terrier::common::HashUtil::HashCrc(input->val_, seed);
}

VM_OP_HOT void OpHashString(terrier::hash_t *const hash_val, const terrier::execution::sql::StringVal *const input,
                            const terrier::hash_t seed) {
  *hash_val = input->is_null_ ? 0 : input->val_.Hash(seed);
}

VM_OP_HOT void OpHashDate(terrier::hash_t *const hash_val, const terrier::execution::sql::DateVal *const input,
                          const terrier::hash_t seed) {
  *hash_val = input->is_null_ ? 0 : input->val_.Hash(seed);
}

VM_OP_HOT void OpHashTimestamp(terrier::hash_t *const hash_val,
                               const terrier::execution::sql::TimestampVal *const input, const terrier::hash_t seed) {
  *hash_val = input->is_null_ ? 0 : input->val_.Hash(seed);
}

VM_OP_HOT void OpHashCombine(terrier::hash_t *hash_val, terrier::hash_t new_hash_val) {
  *hash_val = terrier::common::HashUtil::CombineHashes(*hash_val, new_hash_val);
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

VM_OP void OpFilterManagerInit(terrier::execution::sql::FilterManager *filter_manager,
                               const terrier::execution::exec::ExecutionSettings &exec_settings);

VM_OP void OpFilterManagerStartNewClause(terrier::execution::sql::FilterManager *filter_manager);

VM_OP void OpFilterManagerInsertFilter(terrier::execution::sql::FilterManager *filter_manager,
                                       terrier::execution::sql::FilterManager::MatchFn clause);

VM_OP void OpFilterManagerRunFilters(terrier::execution::sql::FilterManager *filter,
                                     terrier::execution::sql::VectorProjectionIterator *vpi,
                                     terrier::execution::exec::ExecutionContext *exec_ctx);

VM_OP void OpFilterManagerFree(terrier::execution::sql::FilterManager *filter);

// ---------------------------------------------------------
// Vector Filter Executor
// ---------------------------------------------------------

#define GEN_VECTOR_FILTER(Name)                                                                                      \
  VM_OP_HOT void OpVectorFilter##Name(const terrier::execution::exec::ExecutionSettings &exec_settings,              \
                                      terrier::execution::sql::VectorProjection *vector_projection,                  \
                                      const uint32_t left_col_idx, const uint32_t right_col_idx,                     \
                                      terrier::execution::sql::TupleIdList *tid_list) {                              \
    terrier::execution::sql::VectorFilterExecutor::Select##Name(exec_settings, vector_projection, left_col_idx,      \
                                                                right_col_idx, tid_list);                            \
  }                                                                                                                  \
  VM_OP_HOT void OpVectorFilter##Name##Val(const terrier::execution::exec::ExecutionSettings &exec_settings,         \
                                           terrier::execution::sql::VectorProjection *vector_projection,             \
                                           const uint32_t left_col_idx, const terrier::execution::sql::Val *val,     \
                                           terrier::execution::sql::TupleIdList *tid_list) {                         \
    terrier::execution::sql::VectorFilterExecutor::Select##Name##Val(exec_settings, vector_projection, left_col_idx, \
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

VM_OP_HOT void OpForceBoolTruth(bool *result, terrier::execution::sql::BoolVal *input) {
  *result = input->ForceTruth();
}

VM_OP_HOT void OpInitSqlNull(terrier::execution::sql::Val *result) { *result = terrier::execution::sql::Val(true); }

VM_OP_HOT void OpInitBool(terrier::execution::sql::BoolVal *result, bool input) {
  result->is_null_ = false;
  result->val_ = input;
}

VM_OP_HOT void OpInitInteger(terrier::execution::sql::Integer *result, int64_t input) {
  result->is_null_ = false;
  result->val_ = input;
}

VM_OP_HOT void OpInitInteger64(terrier::execution::sql::Integer *result, int64_t input) {
  result->is_null_ = false;
  result->val_ = input;
}

VM_OP_HOT void OpInitReal(terrier::execution::sql::Real *result, double input) {
  result->is_null_ = false;
  result->val_ = input;
}

VM_OP_HOT void OpInitDate(terrier::execution::sql::DateVal *result, int32_t year, int32_t month, int32_t day) {
  result->is_null_ = false;
  result->val_ = terrier::execution::sql::Date::FromYMD(year, month, day);
}

VM_OP_HOT void OpInitTimestamp(terrier::execution::sql::TimestampVal *result, uint64_t usec) {
  result->is_null_ = false;
  result->val_ = terrier::execution::sql::Timestamp::FromMicroseconds(usec);
}

VM_OP_HOT void OpInitTimestampYMDHMSMU(terrier::execution::sql::TimestampVal *result, int32_t year, int32_t month,
                                       int32_t day, int32_t hour, int32_t minute, int32_t sec, int32_t milli,
                                       int32_t micro) {
  result->is_null_ = false;
  auto res = terrier::execution::sql::Timestamp::FromYMDHMSMU(year, month, day, hour, minute, sec, milli, micro);
  result->val_ = res;
}

VM_OP_HOT void OpInitString(terrier::execution::sql::StringVal *result, const uint8_t *str, uint32_t length) {
  *result = terrier::execution::sql::StringVal(reinterpret_cast<const char *>(str), length);
}

VM_OP_WARM void OpBoolToInteger(terrier::execution::sql::Integer *result,
                                const terrier::execution::sql::BoolVal *input) {
  terrier::execution::sql::CastingFunctions::CastToInteger(result, *input);
}

VM_OP_WARM void OpIntegerToBool(terrier::execution::sql::BoolVal *result,
                                const terrier::execution::sql::Integer *input) {
  terrier::execution::sql::CastingFunctions::CastToBoolVal(result, *input);
}

VM_OP_WARM void OpIntegerToReal(terrier::execution::sql::Real *result, const terrier::execution::sql::Integer *input) {
  terrier::execution::sql::CastingFunctions::CastToReal(result, *input);
}

VM_OP_WARM void OpIntegerToString(terrier::execution::sql::StringVal *result,
                                  terrier::execution::exec::ExecutionContext *exec_ctx,
                                  const terrier::execution::sql::Integer *input) {
  terrier::execution::sql::CastingFunctions::CastToStringVal(result, exec_ctx, *input);
}

VM_OP_WARM void OpRealToBool(terrier::execution::sql::BoolVal *result, const terrier::execution::sql::Real *input) {
  terrier::execution::sql::CastingFunctions::CastToBoolVal(result, *input);
}

VM_OP_WARM void OpRealToInteger(terrier::execution::sql::Integer *result, const terrier::execution::sql::Real *input) {
  terrier::execution::sql::CastingFunctions::CastToInteger(result, *input);
}

VM_OP_WARM void OpRealToString(terrier::execution::sql::StringVal *result,
                               terrier::execution::exec::ExecutionContext *exec_ctx,
                               const terrier::execution::sql::Real *input) {
  terrier::execution::sql::CastingFunctions::CastToStringVal(result, exec_ctx, *input);
}

VM_OP_WARM void OpDateToTimestamp(terrier::execution::sql::TimestampVal *result,
                                  const terrier::execution::sql::DateVal *input) {
  terrier::execution::sql::CastingFunctions::CastToTimestampVal(result, *input);
}

VM_OP_WARM void OpDateToString(terrier::execution::sql::StringVal *result,
                               terrier::execution::exec::ExecutionContext *exec_ctx,
                               const terrier::execution::sql::DateVal *input) {
  terrier::execution::sql::CastingFunctions::CastToStringVal(result, exec_ctx, *input);
}

VM_OP_WARM void OpTimestampToDate(terrier::execution::sql::DateVal *result,
                                  const terrier::execution::sql::TimestampVal *input) {
  terrier::execution::sql::CastingFunctions::CastToDateVal(result, *input);
}

VM_OP_WARM void OpTimestampToString(terrier::execution::sql::StringVal *result,
                                    terrier::execution::exec::ExecutionContext *exec_ctx,
                                    const terrier::execution::sql::TimestampVal *input) {
  terrier::execution::sql::CastingFunctions::CastToStringVal(result, exec_ctx, *input);
}

VM_OP_WARM void OpStringToBool(terrier::execution::sql::BoolVal *result,
                               const terrier::execution::sql::StringVal *input) {
  terrier::execution::sql::CastingFunctions::CastToBoolVal(result, *input);
}

VM_OP_WARM void OpStringToInteger(terrier::execution::sql::Integer *result,
                                  const terrier::execution::sql::StringVal *input) {
  terrier::execution::sql::CastingFunctions::CastToInteger(result, *input);
}

VM_OP_WARM void OpStringToReal(terrier::execution::sql::Real *result, const terrier::execution::sql::StringVal *input) {
  terrier::execution::sql::CastingFunctions::CastToReal(result, *input);
}

VM_OP_WARM void OpStringToDate(terrier::execution::sql::DateVal *result,
                               const terrier::execution::sql::StringVal *input) {
  terrier::execution::sql::CastingFunctions::CastToDateVal(result, *input);
}

VM_OP_WARM void OpStringToTimestamp(terrier::execution::sql::TimestampVal *result,
                                    const terrier::execution::sql::StringVal *input) {
  terrier::execution::sql::CastingFunctions::CastToTimestampVal(result, *input);
}

#define GEN_SQL_COMPARISONS(NAME, TYPE)                                                       \
  VM_OP_HOT void OpGreaterThan##NAME(terrier::execution::sql::BoolVal *const result,          \
                                     const terrier::execution::sql::TYPE *const left,         \
                                     const terrier::execution::sql::TYPE *const right) {      \
    terrier::execution::sql::ComparisonFunctions::Gt##TYPE(result, *left, *right);            \
  }                                                                                           \
  VM_OP_HOT void OpGreaterThanEqual##NAME(terrier::execution::sql::BoolVal *const result,     \
                                          const terrier::execution::sql::TYPE *const left,    \
                                          const terrier::execution::sql::TYPE *const right) { \
    terrier::execution::sql::ComparisonFunctions::Ge##TYPE(result, *left, *right);            \
  }                                                                                           \
  VM_OP_HOT void OpEqual##NAME(terrier::execution::sql::BoolVal *const result,                \
                               const terrier::execution::sql::TYPE *const left,               \
                               const terrier::execution::sql::TYPE *const right) {            \
    terrier::execution::sql::ComparisonFunctions::Eq##TYPE(result, *left, *right);            \
  }                                                                                           \
  VM_OP_HOT void OpLessThan##NAME(terrier::execution::sql::BoolVal *const result,             \
                                  const terrier::execution::sql::TYPE *const left,            \
                                  const terrier::execution::sql::TYPE *const right) {         \
    terrier::execution::sql::ComparisonFunctions::Lt##TYPE(result, *left, *right);            \
  }                                                                                           \
  VM_OP_HOT void OpLessThanEqual##NAME(terrier::execution::sql::BoolVal *const result,        \
                                       const terrier::execution::sql::TYPE *const left,       \
                                       const terrier::execution::sql::TYPE *const right) {    \
    terrier::execution::sql::ComparisonFunctions::Le##TYPE(result, *left, *right);            \
  }                                                                                           \
  VM_OP_HOT void OpNotEqual##NAME(terrier::execution::sql::BoolVal *const result,             \
                                  const terrier::execution::sql::TYPE *const left,            \
                                  const terrier::execution::sql::TYPE *const right) {         \
    terrier::execution::sql::ComparisonFunctions::Ne##TYPE(result, *left, *right);            \
  }

GEN_SQL_COMPARISONS(Bool, BoolVal)
GEN_SQL_COMPARISONS(Integer, Integer)
GEN_SQL_COMPARISONS(Real, Real)
GEN_SQL_COMPARISONS(Date, DateVal)
GEN_SQL_COMPARISONS(Timestamp, TimestampVal)
GEN_SQL_COMPARISONS(String, StringVal)

#undef GEN_SQL_COMPARISONS

VM_OP_WARM void OpAbsInteger(terrier::execution::sql::Integer *const result,
                             const terrier::execution::sql::Integer *const left) {
  terrier::execution::sql::ArithmeticFunctions::Abs(result, *left);
}

VM_OP_WARM void OpAbsReal(terrier::execution::sql::Real *const result,
                          const terrier::execution::sql::Real *const left) {
  terrier::execution::sql::ArithmeticFunctions::Abs(result, *left);
}

VM_OP_HOT void OpAddInteger(terrier::execution::sql::Integer *const result,
                            const terrier::execution::sql::Integer *const left,
                            const terrier::execution::sql::Integer *const right) {
  UNUSED_ATTRIBUTE bool overflow;
  terrier::execution::sql::ArithmeticFunctions::Add(result, *left, *right, &overflow);
}

VM_OP_HOT void OpSubInteger(terrier::execution::sql::Integer *const result,
                            const terrier::execution::sql::Integer *const left,
                            const terrier::execution::sql::Integer *const right) {
  UNUSED_ATTRIBUTE bool overflow;
  terrier::execution::sql::ArithmeticFunctions::Sub(result, *left, *right, &overflow);
}

VM_OP_HOT void OpMulInteger(terrier::execution::sql::Integer *const result,
                            const terrier::execution::sql::Integer *const left,
                            const terrier::execution::sql::Integer *const right) {
  UNUSED_ATTRIBUTE bool overflow;
  terrier::execution::sql::ArithmeticFunctions::Mul(result, *left, *right, &overflow);
}

VM_OP_HOT void OpDivInteger(terrier::execution::sql::Integer *const result,
                            const terrier::execution::sql::Integer *const left,
                            const terrier::execution::sql::Integer *const right) {
  UNUSED_ATTRIBUTE bool div_by_zero = false;
  terrier::execution::sql::ArithmeticFunctions::IntDiv(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpModInteger(terrier::execution::sql::Integer *const result,
                            const terrier::execution::sql::Integer *const left,
                            const terrier::execution::sql::Integer *const right) {
  UNUSED_ATTRIBUTE bool div_by_zero = false;
  terrier::execution::sql::ArithmeticFunctions::IntMod(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpAddReal(terrier::execution::sql::Real *const result, const terrier::execution::sql::Real *const left,
                         const terrier::execution::sql::Real *const right) {
  terrier::execution::sql::ArithmeticFunctions::Add(result, *left, *right);
}

VM_OP_HOT void OpSubReal(terrier::execution::sql::Real *const result, const terrier::execution::sql::Real *const left,
                         const terrier::execution::sql::Real *const right) {
  terrier::execution::sql::ArithmeticFunctions::Sub(result, *left, *right);
}

VM_OP_HOT void OpMulReal(terrier::execution::sql::Real *const result, const terrier::execution::sql::Real *const left,
                         const terrier::execution::sql::Real *const right) {
  terrier::execution::sql::ArithmeticFunctions::Mul(result, *left, *right);
}

VM_OP_HOT void OpDivReal(terrier::execution::sql::Real *const result, const terrier::execution::sql::Real *const left,
                         const terrier::execution::sql::Real *const right) {
  UNUSED_ATTRIBUTE bool div_by_zero = false;
  terrier::execution::sql::ArithmeticFunctions::Div(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpModReal(terrier::execution::sql::Real *const result, const terrier::execution::sql::Real *const left,
                         const terrier::execution::sql::Real *const right) {
  UNUSED_ATTRIBUTE bool div_by_zero = false;
  terrier::execution::sql::ArithmeticFunctions::Mod(result, *left, *right, &div_by_zero);
}

// ---------------------------------------------------------
// SQL Aggregations
// ---------------------------------------------------------

VM_OP void OpAggregationHashTableInit(terrier::execution::sql::AggregationHashTable *agg_hash_table,
                                      terrier::execution::exec::ExecutionContext *exec_ctx,
                                      terrier::execution::sql::MemoryPool *memory, uint32_t payload_size);

VM_OP_HOT void OpAggregationHashTableAllocTuple(terrier::byte **result,
                                                terrier::execution::sql::AggregationHashTable *agg_hash_table,
                                                const terrier::hash_t hash_val) {
  *result = agg_hash_table->AllocInputTuple(hash_val);
}

VM_OP_HOT void OpAggregationHashTableAllocTuplePartitioned(
    terrier::byte **result, terrier::execution::sql::AggregationHashTable *agg_hash_table,
    const terrier::hash_t hash_val) {
  *result = agg_hash_table->AllocInputTuplePartitioned(hash_val);
}

VM_OP_HOT void OpAggregationHashTableLinkHashTableEntry(terrier::execution::sql::AggregationHashTable *agg_hash_table,
                                                        terrier::execution::sql::HashTableEntry *entry) {
  agg_hash_table->Insert(entry);
}

VM_OP_HOT void OpAggregationHashTableLookup(terrier::byte **result,
                                            terrier::execution::sql::AggregationHashTable *const agg_hash_table,
                                            const terrier::hash_t hash_val,
                                            const terrier::execution::sql::AggregationHashTable::KeyEqFn key_eq_fn,
                                            const void *probe_tuple) {
  *result = agg_hash_table->Lookup(hash_val, key_eq_fn, probe_tuple);
}

VM_OP_HOT void OpAggregationHashTableProcessBatch(
    terrier::execution::sql::AggregationHashTable *const agg_hash_table,
    terrier::execution::sql::VectorProjectionIterator *vpi, const uint32_t num_keys, const uint32_t key_cols[],
    const terrier::execution::sql::AggregationHashTable::VectorInitAggFn init_fn,
    const terrier::execution::sql::AggregationHashTable::VectorAdvanceAggFn advance_fn, const bool partitioned) {
  agg_hash_table->ProcessBatch(vpi, {key_cols, key_cols + num_keys}, init_fn, advance_fn, partitioned);
}

VM_OP_HOT void OpAggregationHashTableTransferPartitions(
    terrier::execution::sql::AggregationHashTable *const agg_hash_table,
    terrier::execution::sql::ThreadStateContainer *const thread_state_container, const uint32_t agg_ht_offset,
    const terrier::execution::sql::AggregationHashTable::MergePartitionFn merge_partition_fn) {
  agg_hash_table->TransferMemoryAndPartitions(thread_state_container, agg_ht_offset, merge_partition_fn);
}

VM_OP void OpAggregationHashTableBuildAllHashTablePartitions(
    terrier::execution::sql::AggregationHashTable *agg_hash_table, void *query_state);

VM_OP void OpAggregationHashTableRepartition(terrier::execution::sql::AggregationHashTable *agg_hash_table);

VM_OP void OpAggregationHashTableMergePartitions(
    terrier::execution::sql::AggregationHashTable *agg_hash_table,
    terrier::execution::sql::AggregationHashTable *target_agg_hash_table, void *query_state,
    terrier::execution::sql::AggregationHashTable::MergePartitionFn merge_partition_fn);

VM_OP_HOT void OpAggregationHashTableParallelPartitionedScan(
    terrier::execution::sql::AggregationHashTable *const agg_hash_table, void *const query_state,
    terrier::execution::sql::ThreadStateContainer *const thread_state_container,
    const terrier::execution::sql::AggregationHashTable::ScanPartitionFn scan_partition_fn) {
  agg_hash_table->ExecuteParallelPartitionedScan(query_state, thread_state_container, scan_partition_fn);
}

VM_OP void OpAggregationHashTableFree(terrier::execution::sql::AggregationHashTable *agg_hash_table);

VM_OP void OpAggregationHashTableIteratorInit(terrier::execution::sql::AHTIterator *iter,
                                              terrier::execution::sql::AggregationHashTable *agg_hash_table);

VM_OP_HOT void OpAggregationHashTableIteratorHasNext(bool *has_more, terrier::execution::sql::AHTIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationHashTableIteratorNext(terrier::execution::sql::AHTIterator *iter) { iter->Next(); }

VM_OP_HOT void OpAggregationHashTableIteratorGetRow(const terrier::byte **row,
                                                    terrier::execution::sql::AHTIterator *iter) {
  *row = iter->GetCurrentAggregateRow();
}

VM_OP void OpAggregationHashTableIteratorFree(terrier::execution::sql::AHTIterator *iter);

VM_OP_HOT void OpAggregationOverflowPartitionIteratorHasNext(
    bool *has_more, terrier::execution::sql::AHTOverflowPartitionIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorNext(terrier::execution::sql::AHTOverflowPartitionIterator *iter) {
  iter->Next();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetHash(
    terrier::hash_t *hash_val, terrier::execution::sql::AHTOverflowPartitionIterator *iter) {
  *hash_val = iter->GetRowHash();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetRow(
    const terrier::byte **row, terrier::execution::sql::AHTOverflowPartitionIterator *iter) {
  *row = iter->GetRow();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetRowEntry(
    terrier::execution::sql::HashTableEntry **entry, terrier::execution::sql::AHTOverflowPartitionIterator *iter) {
  *entry = iter->GetEntryForRow();
}

// ---------------------------------------------------------
// COUNT
// ---------------------------------------------------------

VM_OP_HOT void OpCountAggregateInit(terrier::execution::sql::CountAggregate *agg) {
  new (agg) terrier::execution::sql::CountAggregate();
}

VM_OP_HOT void OpCountAggregateAdvance(terrier::execution::sql::CountAggregate *agg,
                                       const terrier::execution::sql::Val *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpCountAggregateMerge(terrier::execution::sql::CountAggregate *agg_1,
                                     const terrier::execution::sql::CountAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountAggregateReset(terrier::execution::sql::CountAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpCountAggregateGetResult(terrier::execution::sql::Integer *result,
                                         const terrier::execution::sql::CountAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountAggregateFree(terrier::execution::sql::CountAggregate *agg) { agg->~CountAggregate(); }

// ---------------------------------------------------------
// COUNT(*)
// ---------------------------------------------------------

VM_OP_HOT void OpCountStarAggregateInit(terrier::execution::sql::CountStarAggregate *agg) {
  new (agg) terrier::execution::sql::CountStarAggregate();
}

VM_OP_HOT void OpCountStarAggregateAdvance(terrier::execution::sql::CountStarAggregate *agg,
                                           const terrier::execution::sql::Val *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpCountStarAggregateMerge(terrier::execution::sql::CountStarAggregate *agg_1,
                                         const terrier::execution::sql::CountStarAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountStarAggregateReset(terrier::execution::sql::CountStarAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpCountStarAggregateGetResult(terrier::execution::sql::Integer *result,
                                             const terrier::execution::sql::CountStarAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountStarAggregateFree(terrier::execution::sql::CountStarAggregate *agg) {
  agg->~CountStarAggregate();
}

// ---------------------------------------------------------
// SUM
// ---------------------------------------------------------

VM_OP_HOT void OpIntegerSumAggregateInit(terrier::execution::sql::IntegerSumAggregate *agg) {
  new (agg) terrier::execution::sql::IntegerSumAggregate();
}

VM_OP_HOT void OpIntegerSumAggregateAdvance(terrier::execution::sql::IntegerSumAggregate *agg,
                                            const terrier::execution::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerSumAggregateMerge(terrier::execution::sql::IntegerSumAggregate *agg_1,
                                          const terrier::execution::sql::IntegerSumAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerSumAggregateReset(terrier::execution::sql::IntegerSumAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerSumAggregateGetResult(terrier::execution::sql::Integer *result,
                                              const terrier::execution::sql::IntegerSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpIntegerSumAggregateFree(terrier::execution::sql::IntegerSumAggregate *agg) {
  agg->~IntegerSumAggregate();
}

VM_OP_HOT void OpRealSumAggregateInit(terrier::execution::sql::RealSumAggregate *agg) {
  new (agg) terrier::execution::sql::RealSumAggregate();
}

VM_OP_HOT void OpRealSumAggregateAdvance(terrier::execution::sql::RealSumAggregate *agg,
                                         const terrier::execution::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealSumAggregateMerge(terrier::execution::sql::RealSumAggregate *agg_1,
                                       const terrier::execution::sql::RealSumAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealSumAggregateReset(terrier::execution::sql::RealSumAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealSumAggregateGetResult(terrier::execution::sql::Real *result,
                                           const terrier::execution::sql::RealSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpRealSumAggregateFree(terrier::execution::sql::RealSumAggregate *agg) { agg->~RealSumAggregate(); }

// ---------------------------------------------------------
// MAX
// ---------------------------------------------------------

VM_OP_HOT void OpIntegerMaxAggregateInit(terrier::execution::sql::IntegerMaxAggregate *agg) {
  new (agg) terrier::execution::sql::IntegerMaxAggregate();
}

VM_OP_HOT void OpIntegerMaxAggregateAdvance(terrier::execution::sql::IntegerMaxAggregate *agg,
                                            const terrier::execution::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerMaxAggregateMerge(terrier::execution::sql::IntegerMaxAggregate *agg_1,
                                          const terrier::execution::sql::IntegerMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMaxAggregateReset(terrier::execution::sql::IntegerMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerMaxAggregateGetResult(terrier::execution::sql::Integer *result,
                                              const terrier::execution::sql::IntegerMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpIntegerMaxAggregateFree(terrier::execution::sql::IntegerMaxAggregate *agg) {
  agg->~IntegerMaxAggregate();
}

VM_OP_HOT void OpRealMaxAggregateInit(terrier::execution::sql::RealMaxAggregate *agg) {
  new (agg) terrier::execution::sql::RealMaxAggregate();
}

VM_OP_HOT void OpRealMaxAggregateAdvance(terrier::execution::sql::RealMaxAggregate *agg,
                                         const terrier::execution::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealMaxAggregateMerge(terrier::execution::sql::RealMaxAggregate *agg_1,
                                       const terrier::execution::sql::RealMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealMaxAggregateReset(terrier::execution::sql::RealMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealMaxAggregateGetResult(terrier::execution::sql::Real *result,
                                           const terrier::execution::sql::RealMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpRealMaxAggregateFree(terrier::execution::sql::RealMaxAggregate *agg) { agg->~RealMaxAggregate(); }

VM_OP_HOT void OpDateMaxAggregateInit(terrier::execution::sql::DateMaxAggregate *agg) {
  new (agg) terrier::execution::sql::DateMaxAggregate();
}

VM_OP_HOT void OpDateMaxAggregateAdvance(terrier::execution::sql::DateMaxAggregate *agg,
                                         const terrier::execution::sql::DateVal *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpDateMaxAggregateMerge(terrier::execution::sql::DateMaxAggregate *agg_1,
                                       const terrier::execution::sql::DateMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpDateMaxAggregateReset(terrier::execution::sql::DateMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpDateMaxAggregateGetResult(terrier::execution::sql::DateVal *result,
                                           const terrier::execution::sql::DateMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpDateMaxAggregateFree(terrier::execution::sql::DateMaxAggregate *agg) { agg->~DateMaxAggregate(); }

VM_OP_HOT void OpStringMaxAggregateInit(terrier::execution::sql::StringMaxAggregate *agg) {
  new (agg) terrier::execution::sql::StringMaxAggregate();
}

VM_OP_HOT void OpStringMaxAggregateAdvance(terrier::execution::sql::StringMaxAggregate *agg,
                                           const terrier::execution::sql::StringVal *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpStringMaxAggregateMerge(terrier::execution::sql::StringMaxAggregate *agg_1,
                                         const terrier::execution::sql::StringMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpStringMaxAggregateReset(terrier::execution::sql::StringMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpStringMaxAggregateGetResult(terrier::execution::sql::StringVal *result,
                                             const terrier::execution::sql::StringMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpStringMaxAggregateFree(terrier::execution::sql::StringMaxAggregate *agg) {
  agg->~StringMaxAggregate();
}

// ---------------------------------------------------------
// MIN
// ---------------------------------------------------------

VM_OP_HOT void OpIntegerMinAggregateInit(terrier::execution::sql::IntegerMinAggregate *agg) {
  new (agg) terrier::execution::sql::IntegerMinAggregate();
}

VM_OP_HOT void OpIntegerMinAggregateAdvance(terrier::execution::sql::IntegerMinAggregate *agg,
                                            const terrier::execution::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerMinAggregateMerge(terrier::execution::sql::IntegerMinAggregate *agg_1,
                                          const terrier::execution::sql::IntegerMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMinAggregateReset(terrier::execution::sql::IntegerMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerMinAggregateGetResult(terrier::execution::sql::Integer *result,
                                              const terrier::execution::sql::IntegerMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpIntegerMinAggregateFree(terrier::execution::sql::IntegerMinAggregate *agg) {
  agg->~IntegerMinAggregate();
}

VM_OP_HOT void OpRealMinAggregateInit(terrier::execution::sql::RealMinAggregate *agg) {
  new (agg) terrier::execution::sql::RealMinAggregate();
}

VM_OP_HOT void OpRealMinAggregateAdvance(terrier::execution::sql::RealMinAggregate *agg,
                                         const terrier::execution::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealMinAggregateMerge(terrier::execution::sql::RealMinAggregate *agg_1,
                                       const terrier::execution::sql::RealMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealMinAggregateReset(terrier::execution::sql::RealMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealMinAggregateGetResult(terrier::execution::sql::Real *result,
                                           const terrier::execution::sql::RealMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpRealMinAggregateFree(terrier::execution::sql::RealMinAggregate *agg) { agg->~RealMinAggregate(); }

VM_OP_HOT void OpDateMinAggregateInit(terrier::execution::sql::DateMinAggregate *agg) {
  new (agg) terrier::execution::sql::DateMinAggregate();
}

VM_OP_HOT void OpDateMinAggregateAdvance(terrier::execution::sql::DateMinAggregate *agg,
                                         const terrier::execution::sql::DateVal *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpDateMinAggregateMerge(terrier::execution::sql::DateMinAggregate *agg_1,
                                       const terrier::execution::sql::DateMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpDateMinAggregateReset(terrier::execution::sql::DateMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpDateMinAggregateGetResult(terrier::execution::sql::DateVal *result,
                                           const terrier::execution::sql::DateMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpDateMinAggregateFree(terrier::execution::sql::DateMinAggregate *agg) { agg->~DateMinAggregate(); }

VM_OP_HOT void OpStringMinAggregateInit(terrier::execution::sql::StringMinAggregate *agg) {
  new (agg) terrier::execution::sql::StringMinAggregate();
}

VM_OP_HOT void OpStringMinAggregateAdvance(terrier::execution::sql::StringMinAggregate *agg,
                                           const terrier::execution::sql::StringVal *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpStringMinAggregateMerge(terrier::execution::sql::StringMinAggregate *agg_1,
                                         const terrier::execution::sql::StringMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpStringMinAggregateReset(terrier::execution::sql::StringMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpStringMinAggregateGetResult(terrier::execution::sql::StringVal *result,
                                             const terrier::execution::sql::StringMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpStringMinAggregateFree(terrier::execution::sql::StringMinAggregate *agg) {
  agg->~StringMinAggregate();
}

// ---------------------------------------------------------
// AVG
// ---------------------------------------------------------

VM_OP_HOT void OpAvgAggregateInit(terrier::execution::sql::AvgAggregate *agg) {
  new (agg) terrier::execution::sql::AvgAggregate();
}

VM_OP_HOT void OpAvgAggregateAdvanceInteger(terrier::execution::sql::AvgAggregate *agg,
                                            const terrier::execution::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpAvgAggregateAdvanceReal(terrier::execution::sql::AvgAggregate *agg,
                                         const terrier::execution::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpAvgAggregateMerge(terrier::execution::sql::AvgAggregate *agg_1,
                                   const terrier::execution::sql::AvgAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpAvgAggregateReset(terrier::execution::sql::AvgAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpAvgAggregateGetResult(terrier::execution::sql::Real *result,
                                       const terrier::execution::sql::AvgAggregate *agg) {
  *result = agg->GetResultAvg();
}

VM_OP_HOT void OpAvgAggregateFree(terrier::execution::sql::AvgAggregate *agg) { agg->~AvgAggregate(); }

// ---------------------------------------------------------
// Hash Joins
// ---------------------------------------------------------

VM_OP void OpJoinHashTableInit(terrier::execution::sql::JoinHashTable *join_hash_table,
                               terrier::execution::exec::ExecutionContext *exec_ctx,
                               terrier::execution::sql::MemoryPool *memory, uint32_t tuple_size);

VM_OP_HOT void OpJoinHashTableAllocTuple(terrier::byte **result,
                                         terrier::execution::sql::JoinHashTable *join_hash_table,
                                         terrier::hash_t hash) {
  *result = join_hash_table->AllocInputTuple(hash);
}

VM_OP void OpJoinHashTableBuild(terrier::execution::sql::JoinHashTable *join_hash_table);

VM_OP void OpJoinHashTableBuildParallel(terrier::execution::sql::JoinHashTable *join_hash_table,
                                        terrier::execution::sql::ThreadStateContainer *thread_state_container,
                                        uint32_t jht_offset);

VM_OP_HOT void OpJoinHashTableLookup(terrier::execution::sql::JoinHashTable *join_hash_table,
                                     terrier::execution::sql::HashTableEntryIterator *ht_entry_iter,
                                     const terrier::hash_t hash_val) {
  *ht_entry_iter = join_hash_table->Lookup<false>(hash_val);
}

VM_OP void OpJoinHashTableFree(terrier::execution::sql::JoinHashTable *join_hash_table);

VM_OP_HOT void OpHashTableEntryIteratorHasNext(bool *has_next,
                                               terrier::execution::sql::HashTableEntryIterator *ht_entry_iter) {
  *has_next = ht_entry_iter->HasNext();
}

VM_OP_HOT void OpHashTableEntryIteratorGetRow(const terrier::byte **row,
                                              terrier::execution::sql::HashTableEntryIterator *ht_entry_iter) {
  *row = ht_entry_iter->GetMatchPayload();
}

// ---------------------------------------------------------
// Sorting
// ---------------------------------------------------------

VM_OP void OpSorterInit(terrier::execution::sql::Sorter *sorter, terrier::execution::sql::MemoryPool *memory,
                        terrier::execution::sql::Sorter::ComparisonFunction cmp_fn, uint32_t tuple_size);

VM_OP_HOT void OpSorterAllocTuple(terrier::byte **result, terrier::execution::sql::Sorter *sorter) {
  *result = sorter->AllocInputTuple();
}

VM_OP_HOT void OpSorterAllocTupleTopK(terrier::byte **result, terrier::execution::sql::Sorter *sorter, uint64_t top_k) {
  *result = sorter->AllocInputTupleTopK(top_k);
}

VM_OP_HOT void OpSorterAllocTupleTopKFinish(terrier::execution::sql::Sorter *sorter, uint64_t top_k) {
  sorter->AllocInputTupleTopKFinish(top_k);
}

VM_OP void OpSorterSort(terrier::execution::sql::Sorter *sorter);

VM_OP void OpSorterSortParallel(terrier::execution::sql::Sorter *sorter,
                                terrier::execution::sql::ThreadStateContainer *thread_state_container,
                                uint32_t sorter_offset);

VM_OP void OpSorterSortTopKParallel(terrier::execution::sql::Sorter *sorter,
                                    terrier::execution::sql::ThreadStateContainer *thread_state_container,
                                    uint32_t sorter_offset, uint64_t top_k);

VM_OP void OpSorterFree(terrier::execution::sql::Sorter *sorter);

VM_OP void OpSorterIteratorInit(terrier::execution::sql::SorterIterator *iter, terrier::execution::sql::Sorter *sorter);

VM_OP_HOT void OpSorterIteratorHasNext(bool *has_more, terrier::execution::sql::SorterIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpSorterIteratorNext(terrier::execution::sql::SorterIterator *iter) { iter->Next(); }

VM_OP_HOT void OpSorterIteratorGetRow(const terrier::byte **row, terrier::execution::sql::SorterIterator *iter) {
  *row = iter->GetRow();
}

VM_OP_WARM void OpSorterIteratorSkipRows(terrier::execution::sql::SorterIterator *iter, const uint64_t n) {
  iter->AdvanceBy(n);
}

VM_OP void OpSorterIteratorFree(terrier::execution::sql::SorterIterator *iter);

// ---------------------------------------------------------
// Output
// ---------------------------------------------------------

VM_OP_WARM void OpResultBufferAllocOutputRow(terrier::byte **result, terrier::execution::exec::ExecutionContext *ctx) {
  *result = ctx->GetOutputBuffer()->AllocOutputSlot();
}

VM_OP_WARM void OpResultBufferFinalize(terrier::execution::exec::ExecutionContext *ctx) {
  ctx->GetOutputBuffer()->Finalize();
}

// ---------------------------------------------------------
// CSV Reader
// ---------------------------------------------------------

#if 0
VM_OP void OpCSVReaderInit(terrier::execution::util::CSVReader *reader, const uint8_t *file_name, uint32_t len);

VM_OP void OpCSVReaderPerformInit(bool *result, terrier::execution::util::CSVReader *reader);

VM_OP_WARM void OpCSVReaderAdvance(bool *has_more, terrier::execution::util::CSVReader *reader) {
  *has_more = reader->Advance();
}

VM_OP_WARM void OpCSVReaderGetField(terrier::execution::util::CSVReader *reader, const uint32_t field_index,
                                    terrier::execution::sql::StringVal *result) {
  // TODO(pmenon): There's an extra copy here. Revisit if it's a performance issue.
  const std::string field_string = reader->GetRowCell(field_index)->AsString();
  *result = terrier::execution::sql::StringVal(terrier::storage::VarlenEntry::Create(field_string));
}

VM_OP_WARM void OpCSVReaderGetRecordNumber(uint32_t *result, terrier::execution::util::CSVReader *reader) {
  *result = reader->GetRecordNumber();
}

VM_OP void OpCSVReaderClose(terrier::execution::util::CSVReader *reader);
#endif

// ---------------------------------------------------------
// Trig functions
// ---------------------------------------------------------

VM_OP_WARM void OpPi(terrier::execution::sql::Real *result) {
  terrier::execution::sql::ArithmeticFunctions::Pi(result);
}

VM_OP_WARM void OpE(terrier::execution::sql::Real *result) { terrier::execution::sql::ArithmeticFunctions::E(result); }

VM_OP_WARM void OpAcos(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *input) {
  terrier::execution::sql::ArithmeticFunctions::Acos(result, *input);
}

VM_OP_WARM void OpAsin(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *input) {
  terrier::execution::sql::ArithmeticFunctions::Asin(result, *input);
}

VM_OP_WARM void OpAtan(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *input) {
  terrier::execution::sql::ArithmeticFunctions::Atan(result, *input);
}

VM_OP_WARM void OpAtan2(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *arg_1,
                        const terrier::execution::sql::Real *arg_2) {
  terrier::execution::sql::ArithmeticFunctions::Atan2(result, *arg_1, *arg_2);
}

VM_OP_WARM void OpCos(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *input) {
  terrier::execution::sql::ArithmeticFunctions::Cos(result, *input);
}

VM_OP_WARM void OpCot(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *input) {
  terrier::execution::sql::ArithmeticFunctions::Cot(result, *input);
}

VM_OP_WARM void OpSin(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *input) {
  terrier::execution::sql::ArithmeticFunctions::Sin(result, *input);
}

VM_OP_WARM void OpTan(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *input) {
  terrier::execution::sql::ArithmeticFunctions::Tan(result, *input);
}

VM_OP_WARM void OpCosh(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Cosh(result, *v);
}

VM_OP_WARM void OpTanh(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Tanh(result, *v);
}

VM_OP_WARM void OpSinh(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Sinh(result, *v);
}

VM_OP_WARM void OpSqrt(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Sqrt(result, *v);
}

VM_OP_WARM void OpCbrt(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Cbrt(result, *v);
}

VM_OP_WARM void OpExp(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Exp(result, *v);
}

VM_OP_WARM void OpCeil(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Ceil(result, *v);
}

VM_OP_WARM void OpFloor(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Floor(result, *v);
}

VM_OP_WARM void OpTruncate(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Truncate(result, *v);
}

VM_OP_WARM void OpLn(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Ln(result, *v);
}

VM_OP_WARM void OpLog2(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Log2(result, *v);
}

VM_OP_WARM void OpLog10(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Log10(result, *v);
}

VM_OP_WARM void OpSign(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Sign(result, *v);
}

VM_OP_WARM void OpRadians(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Radians(result, *v);
}

VM_OP_WARM void OpDegrees(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Degrees(result, *v);
}

VM_OP_WARM void OpRound(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v) {
  terrier::execution::sql::ArithmeticFunctions::Round(result, *v);
}

VM_OP_WARM void OpRound2(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v,
                         const terrier::execution::sql::Integer *precision) {
  terrier::execution::sql::ArithmeticFunctions::Round2(result, *v, *precision);
}

VM_OP_WARM void OpLog(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *base,
                      const terrier::execution::sql::Real *val) {
  terrier::execution::sql::ArithmeticFunctions::Log(result, *base, *val);
}

VM_OP_WARM void OpPow(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *base,
                      const terrier::execution::sql::Real *val) {
  terrier::execution::sql::ArithmeticFunctions::Pow(result, *base, *val);
}

// ---------------------------------------------------------
// Null/Not Null predicates
// ---------------------------------------------------------

VM_OP_WARM void OpValIsNull(bool *result, const terrier::execution::sql::Val *val) {
  *result = terrier::execution::sql::IsNullPredicate::IsNull(*val);
}

VM_OP_WARM void OpValIsNotNull(bool *result, const terrier::execution::sql::Val *val) {
  *result = terrier::execution::sql::IsNullPredicate::IsNotNull(*val);
}

// ---------------------------------------------------------
// Internal mini-runner functions
// ---------------------------------------------------------

VM_OP_WARM void OpNpRunnersEmitInt(terrier::execution::exec::ExecutionContext *ctx,
                                   const terrier::execution::sql::Integer *num_tuples,
                                   const terrier::execution::sql::Integer *num_cols,
                                   const terrier::execution::sql::Integer *num_int_cols,
                                   const terrier::execution::sql::Integer *num_real_cols) {
  terrier::execution::sql::MiniRunnersFunctions::EmitTuples(ctx, *num_tuples, *num_cols, *num_int_cols, *num_real_cols);
}

VM_OP_WARM void OpNpRunnersEmitReal(terrier::execution::exec::ExecutionContext *ctx,
                                    const terrier::execution::sql::Integer *num_tuples,
                                    const terrier::execution::sql::Integer *num_cols,
                                    const terrier::execution::sql::Integer *num_int_cols,
                                    const terrier::execution::sql::Integer *num_real_cols) {
  terrier::execution::sql::MiniRunnersFunctions::EmitTuples(ctx, *num_tuples, *num_cols, *num_int_cols, *num_real_cols);
}

VM_OP_WARM void OpNpRunnersDummyInt(UNUSED_ATTRIBUTE terrier::execution::exec::ExecutionContext *ctx) {}

VM_OP_WARM void OpNpRunnersDummyReal(UNUSED_ATTRIBUTE terrier::execution::exec::ExecutionContext *ctx) {}

// ---------------------------------------------------------
// String functions
// ---------------------------------------------------------
VM_OP_WARM void OpChr(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                      const terrier::execution::sql::Integer *n) {
  terrier::execution::sql::StringFunctions::Chr(result, ctx, *n);
}

VM_OP_WARM void OpASCII(terrier::execution::sql::Integer *result, terrier::execution::exec::ExecutionContext *ctx,
                        const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::ASCII(result, ctx, *str);
}

VM_OP_WARM void OpCharLength(terrier::execution::sql::Integer *result, terrier::execution::exec::ExecutionContext *ctx,
                             const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::CharLength(result, ctx, *str);
}

VM_OP_WARM void OpConcat(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                         const terrier::execution::sql::StringVal *left,
                         const terrier::execution::sql::StringVal *right) {
  terrier::execution::sql::StringFunctions::Concat(result, ctx, *left, *right);
}

VM_OP_WARM void OpLeft(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                       const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *n) {
  terrier::execution::sql::StringFunctions::Left(result, ctx, *str, *n);
}

VM_OP_WARM void OpLike(terrier::execution::sql::BoolVal *result, const terrier::execution::sql::StringVal *str,
                       const terrier::execution::sql::StringVal *pattern) {
  terrier::execution::sql::StringFunctions::Like(result, nullptr, *str, *pattern);
}

VM_OP_WARM void OpLength(terrier::execution::sql::Integer *result, terrier::execution::exec::ExecutionContext *ctx,
                         const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::Length(result, ctx, *str);
}

VM_OP_WARM void OpLower(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                        const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::Lower(result, ctx, *str);
}

VM_OP_WARM void OpPosition(terrier::execution::sql::Integer *result, terrier::execution::exec::ExecutionContext *ctx,
                           const terrier::execution::sql::StringVal *search_str,
                           const terrier::execution::sql::StringVal *search_sub_str) {
  terrier::execution::sql::StringFunctions::Position(result, ctx, *search_str, *search_sub_str);
}

VM_OP_WARM void OpLPad3Arg(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                           const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *len,
                           const terrier::execution::sql::StringVal *pad) {
  terrier::execution::sql::StringFunctions::Lpad(result, ctx, *str, *len, *pad);
}

VM_OP_WARM void OpLPad2Arg(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                           const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *len) {
  terrier::execution::sql::StringFunctions::Lpad(result, ctx, *str, *len);
}

VM_OP_WARM void OpLTrim2Arg(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                            const terrier::execution::sql::StringVal *str,
                            const terrier::execution::sql::StringVal *chars) {
  terrier::execution::sql::StringFunctions::Ltrim(result, ctx, *str, *chars);
}

VM_OP_WARM void OpLTrim1Arg(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                            const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::Ltrim(result, ctx, *str);
}

VM_OP_WARM void OpRepeat(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                         const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *n) {
  terrier::execution::sql::StringFunctions::Repeat(result, ctx, *str, *n);
}

VM_OP_WARM void OpReverse(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                          const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::Reverse(result, ctx, *str);
}

VM_OP_WARM void OpRight(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                        const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *n) {
  terrier::execution::sql::StringFunctions::Right(result, ctx, *str, *n);
}

VM_OP_WARM void OpRPad3Arg(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                           const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *len,
                           const terrier::execution::sql::StringVal *pad) {
  terrier::execution::sql::StringFunctions::Rpad(result, ctx, *str, *len, *pad);
}

VM_OP_WARM void OpRPad2Arg(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                           const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *len) {
  terrier::execution::sql::StringFunctions::Rpad(result, ctx, *str, *len);
}

VM_OP_WARM void OpRTrim2Arg(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                            const terrier::execution::sql::StringVal *str,
                            const terrier::execution::sql::StringVal *chars) {
  terrier::execution::sql::StringFunctions::Rtrim(result, ctx, *str, *chars);
}

VM_OP_WARM void OpRTrim1Arg(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                            const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::Rtrim(result, ctx, *str);
}

VM_OP_WARM void OpSplitPart(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                            const terrier::execution::sql::StringVal *str,
                            const terrier::execution::sql::StringVal *delim,
                            const terrier::execution::sql::Integer *field) {
  terrier::execution::sql::StringFunctions::SplitPart(result, ctx, *str, *delim, *field);
}

VM_OP_WARM void OpSubstring(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                            const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *pos,
                            const terrier::execution::sql::Integer *len) {
  terrier::execution::sql::StringFunctions::Substring(result, ctx, *str, *pos, *len);
}

VM_OP_WARM void OpStartsWith(terrier::execution::sql::BoolVal *result, terrier::execution::exec::ExecutionContext *ctx,
                             const terrier::execution::sql::StringVal *str,
                             const terrier::execution::sql::StringVal *start) {
  terrier::execution::sql::StringFunctions::StartsWith(result, ctx, *str, *start);
}

VM_OP_WARM void OpTrim(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                       const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::Trim(result, ctx, *str);
}

VM_OP_WARM void OpTrim2(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                        const terrier::execution::sql::StringVal *str,
                        const terrier::execution::sql::StringVal *chars) {
  terrier::execution::sql::StringFunctions::Trim(result, ctx, *str, *chars);
}

VM_OP_WARM void OpUpper(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                        const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::Upper(result, ctx, *str);
}

VM_OP_WARM void OpVersion(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result) {
  terrier::execution::sql::SystemFunctions::Version(ctx, result);
}

VM_OP_WARM void OpInitCap(terrier::execution::sql::StringVal *result, terrier::execution::exec::ExecutionContext *ctx,
                          const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::InitCap(result, ctx, *str);
}

// ---------------------------------------------------------------
// Index Iterator
// ---------------------------------------------------------------
VM_OP void OpIndexIteratorInit(terrier::execution::sql::IndexIterator *iter,
                               terrier::execution::exec::ExecutionContext *exec_ctx, uint32_t num_attrs,
                               uint32_t table_oid, uint32_t index_oid, uint32_t *col_oids, uint32_t num_oids);
VM_OP void OpIndexIteratorFree(terrier::execution::sql::IndexIterator *iter);

VM_OP void OpIndexIteratorPerformInit(terrier::execution::sql::IndexIterator *iter);

VM_OP_WARM void OpIndexIteratorScanKey(terrier::execution::sql::IndexIterator *iter) { iter->ScanKey(); }

VM_OP_WARM void OpIndexIteratorScanAscending(terrier::execution::sql::IndexIterator *iter,
                                             terrier::storage::index::ScanType scan_type, uint32_t limit) {
  iter->ScanAscending(scan_type, limit);
}

VM_OP_WARM void OpIndexIteratorScanDescending(terrier::execution::sql::IndexIterator *iter) { iter->ScanDescending(); }

VM_OP_WARM void OpIndexIteratorScanLimitDescending(terrier::execution::sql::IndexIterator *iter, uint32_t limit) {
  iter->ScanLimitDescending(limit);
}

VM_OP_WARM void OpIndexIteratorAdvance(bool *has_more, terrier::execution::sql::IndexIterator *iter) {
  *has_more = iter->Advance();
}

VM_OP_WARM void OpIndexIteratorGetPR(terrier::storage::ProjectedRow **pr,
                                     terrier::execution::sql::IndexIterator *iter) {
  *pr = iter->PR();
}

VM_OP_WARM void OpIndexIteratorGetLoPR(terrier::storage::ProjectedRow **pr,
                                       terrier::execution::sql::IndexIterator *iter) {
  *pr = iter->LoPR();
}

VM_OP_WARM void OpIndexIteratorGetHiPR(terrier::storage::ProjectedRow **pr,
                                       terrier::execution::sql::IndexIterator *iter) {
  *pr = iter->HiPR();
}

VM_OP_WARM void OpIndexIteratorGetTablePR(terrier::storage::ProjectedRow **pr,
                                          terrier::execution::sql::IndexIterator *iter) {
  *pr = iter->TablePR();
}

VM_OP_WARM void OpIndexIteratorGetSlot(terrier::storage::TupleSlot *slot,
                                       terrier::execution::sql::IndexIterator *iter) {
  *slot = iter->CurrentSlot();
}

#define GEN_PR_SCALAR_SET_CALLS(Name, SqlType, CppType)                                    \
  VM_OP_HOT void OpPRSet##Name(terrier::storage::ProjectedRow *pr, uint16_t col_idx,       \
                               terrier::execution::sql::SqlType *val) {                    \
    pr->Set<CppType, false>(col_idx, static_cast<CppType>(val->val_), val->is_null_);      \
  }                                                                                        \
                                                                                           \
  VM_OP_HOT void OpPRSet##Name##Null(terrier::storage::ProjectedRow *pr, uint16_t col_idx, \
                                     terrier::execution::sql::SqlType *val) {              \
    pr->Set<CppType, true>(col_idx, static_cast<CppType>(val->val_), val->is_null_);       \
  }

#define GEN_PR_SCALAR_GET_CALLS(Name, SqlType, CppType)                                                         \
  VM_OP_HOT void OpPRGet##Name(terrier::execution::sql::SqlType *out, terrier::storage::ProjectedRow *pr,       \
                               uint16_t col_idx) {                                                              \
    /* Read */                                                                                                  \
    auto *ptr = pr->Get<CppType, false>(col_idx, nullptr);                                                      \
    TERRIER_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");                                 \
    /* Set */                                                                                                   \
    out->is_null_ = false;                                                                                      \
    out->val_ = *ptr;                                                                                           \
  }                                                                                                             \
                                                                                                                \
  VM_OP_HOT void OpPRGet##Name##Null(terrier::execution::sql::SqlType *out, terrier::storage::ProjectedRow *pr, \
                                     uint16_t col_idx) {                                                        \
    /* Read */                                                                                                  \
    bool null = false;                                                                                          \
    auto *ptr = pr->Get<CppType, true>(col_idx, &null);                                                         \
    /* Set */                                                                                                   \
    out->is_null_ = null;                                                                                       \
    out->val_ = null ? 0 : *ptr;                                                                                \
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

VM_OP_HOT void OpPRSetVarlen(terrier::storage::ProjectedRow *pr, uint16_t col_idx,
                             terrier::execution::sql::StringVal *val, bool own) {
  const auto varlen = terrier::execution::sql::StringVal::CreateVarlen(*val, own);
  pr->Set<terrier::storage::VarlenEntry, false>(col_idx, varlen, val->is_null_);
}

VM_OP_HOT void OpPRSetVarlenNull(terrier::storage::ProjectedRow *pr, uint16_t col_idx,
                                 terrier::execution::sql::StringVal *val, bool own) {
  const auto varlen = terrier::execution::sql::StringVal::CreateVarlen(*val, own);
  pr->Set<terrier::storage::VarlenEntry, true>(col_idx, varlen, val->is_null_);
}

VM_OP_HOT void OpPRSetDateVal(terrier::storage::ProjectedRow *pr, uint16_t col_idx,
                              terrier::execution::sql::DateVal *val) {
  auto pr_val = val->val_.ToNative();
  pr->Set<uint32_t, false>(col_idx, pr_val, val->is_null_);
}

VM_OP_HOT void OpPRSetDateValNull(terrier::storage::ProjectedRow *pr, uint16_t col_idx,
                                  terrier::execution::sql::DateVal *val) {
  auto pr_val = val->is_null_ ? 0 : val->val_.ToNative();
  pr->Set<uint32_t, true>(col_idx, pr_val, val->is_null_);
}

VM_OP_HOT void OpPRSetTimestampVal(terrier::storage::ProjectedRow *pr, uint16_t col_idx,
                                   terrier::execution::sql::TimestampVal *val) {
  auto pr_val = val->val_.ToNative();
  pr->Set<uint64_t, false>(col_idx, pr_val, val->is_null_);
}

VM_OP_HOT void OpPRSetTimestampValNull(terrier::storage::ProjectedRow *pr, uint16_t col_idx,
                                       terrier::execution::sql::TimestampVal *val) {
  auto pr_val = val->is_null_ ? 0 : val->val_.ToNative();
  pr->Set<uint64_t, true>(col_idx, pr_val, val->is_null_);
}

VM_OP_HOT void OpPRGetDateVal(terrier::execution::sql::DateVal *out, terrier::storage::ProjectedRow *pr,
                              uint16_t col_idx) {
  // Read
  auto *ptr = pr->Get<uint32_t, false>(col_idx, nullptr);
  TERRIER_ASSERT(ptr != nullptr, "Null pointer when trying to read date");
  // Set
  out->is_null_ = false;
  out->val_ = terrier::execution::sql::Date::FromNative(*ptr);
}

VM_OP_HOT void OpPRGetDateValNull(terrier::execution::sql::DateVal *out, terrier::storage::ProjectedRow *pr,
                                  uint16_t col_idx) {
  // Read
  bool null = false;
  auto *ptr = pr->Get<uint32_t, true>(col_idx, &null);

  // Set
  out->is_null_ = null;
  out->val_ = null ? terrier::execution::sql::Date() : terrier::execution::sql::Date::FromNative(*ptr);
}

VM_OP_HOT void OpPRGetTimestampVal(terrier::execution::sql::TimestampVal *out, terrier::storage::ProjectedRow *pr,
                                   uint16_t col_idx) {
  // Read
  auto *ptr = pr->Get<uint64_t, false>(col_idx, nullptr);
  TERRIER_ASSERT(ptr != nullptr, "Null pointer when trying to read date");

  // Set
  out->is_null_ = false;
  out->val_ = terrier::execution::sql::Timestamp::FromNative(*ptr);
}

VM_OP_HOT void OpPRGetTimestampValNull(terrier::execution::sql::TimestampVal *out, terrier::storage::ProjectedRow *pr,
                                       uint16_t col_idx) {
  // Read
  bool null = false;
  auto *ptr = pr->Get<uint64_t, true>(col_idx, &null);

  // Set
  out->is_null_ = null;
  out->val_ = null ? terrier::execution::sql::Timestamp() : terrier::execution::sql::Timestamp::FromNative(*ptr);
}

VM_OP_HOT void OpPRGetVarlen(terrier::execution::sql::StringVal *out, terrier::storage::ProjectedRow *pr,
                             uint16_t col_idx) {
  // Read
  auto *varlen = pr->Get<terrier::storage::VarlenEntry, false>(col_idx, nullptr);
  TERRIER_ASSERT(varlen != nullptr, "Null pointer when trying to read varlen");

  // Set
  *out = terrier::execution::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
}

VM_OP_HOT void OpPRGetVarlenNull(terrier::execution::sql::StringVal *out, terrier::storage::ProjectedRow *pr,
                                 uint16_t col_idx) {
  // Read
  bool null = false;
  auto *varlen = pr->Get<terrier::storage::VarlenEntry, true>(col_idx, &null);

  // Set
  if (null) {
    out->is_null_ = null;
  } else {
    *out = terrier::execution::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
  }
}

// ---------------------------------------------------------------
// StorageInterface Calls
// ---------------------------------------------------------------

VM_OP void OpStorageInterfaceInit(terrier::execution::sql::StorageInterface *storage_interface,
                                  terrier::execution::exec::ExecutionContext *exec_ctx, uint32_t table_oid,
                                  uint32_t *col_oids, uint32_t num_oids, bool need_indexes);

VM_OP void OpStorageInterfaceGetTablePR(terrier::storage::ProjectedRow **pr_result,
                                        terrier::execution::sql::StorageInterface *storage_interface);

VM_OP void OpStorageInterfaceTableUpdate(bool *result, terrier::execution::sql::StorageInterface *storage_interface,
                                         terrier::storage::TupleSlot *tuple_slot);

VM_OP void OpStorageInterfaceTableDelete(bool *result, terrier::execution::sql::StorageInterface *storage_interface,
                                         terrier::storage::TupleSlot *tuple_slot);

VM_OP void OpStorageInterfaceTableInsert(terrier::storage::TupleSlot *tuple_slot,
                                         terrier::execution::sql::StorageInterface *storage_interface);

VM_OP void OpStorageInterfaceGetIndexPR(terrier::storage::ProjectedRow **pr_result,
                                        terrier::execution::sql::StorageInterface *storage_interface,
                                        uint32_t index_oid);

VM_OP void OpStorageInterfaceGetIndexHeapSize(uint32_t *size,
                                              terrier::execution::sql::StorageInterface *storage_interface);

VM_OP void OpStorageInterfaceIndexInsert(bool *result, terrier::execution::sql::StorageInterface *storage_interface);

VM_OP void OpStorageInterfaceIndexInsertUnique(bool *result,
                                               terrier::execution::sql::StorageInterface *storage_interface);

VM_OP void OpStorageInterfaceIndexInsertWithSlot(bool *result,
                                                 terrier::execution::sql::StorageInterface *storage_interface,
                                                 terrier::storage::TupleSlot *tuple_slot, bool unique);

VM_OP void OpStorageInterfaceIndexDelete(terrier::execution::sql::StorageInterface *storage_interface,
                                         terrier::storage::TupleSlot *tuple_slot);

VM_OP void OpStorageInterfaceFree(terrier::execution::sql::StorageInterface *storage_interface);

// ---------------------------------
// Date function
// ---------------------------------

VM_OP_WARM void OpExtractYearFromDate(terrier::execution::sql::Integer *result,
                                      terrier::execution::sql::DateVal *input) {
  if (input->is_null_) {
    result->is_null_ = true;
  } else {
    result->is_null_ = false;
    result->val_ = input->val_.ExtractYear();
  }
}

VM_OP_WARM void OpAbortTxn(terrier::execution::exec::ExecutionContext *exec_ctx) {
  exec_ctx->GetTxn()->SetMustAbort();
  throw terrier::ABORT_EXCEPTION("transaction aborted");
}

// Parameter calls
#define GEN_SCALAR_PARAM_GET(Name, SqlType)                                                                   \
  VM_OP_HOT void OpGetParam##Name(terrier::execution::sql::SqlType *ret,                                      \
                                  terrier::execution::exec::ExecutionContext *exec_ctx, uint32_t param_idx) { \
    const auto &cve = exec_ctx->GetParam(param_idx);                                                          \
    if (cve.IsNull()) {                                                                                       \
      ret->is_null_ = true;                                                                                   \
    } else {                                                                                                  \
      *ret = cve.Get##SqlType();                                                                              \
    }                                                                                                         \
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

VM_OP_WARM void OpTestCatalogLookup(uint32_t *oid_var, terrier::execution::exec::ExecutionContext *exec_ctx,
                                    const uint8_t *table_name_str, uint32_t table_name_length,
                                    const uint8_t *col_name_str, uint32_t col_name_length) {
  // TODO(WAN): wasteful std::string
  terrier::execution::sql::StringVal table_name{reinterpret_cast<const char *>(table_name_str), table_name_length};
  terrier::execution::sql::StringVal col_name{reinterpret_cast<const char *>(col_name_str), col_name_length};
  terrier::catalog::table_oid_t table_oid = exec_ctx->GetAccessor()->GetTableOid(std::string(table_name.StringView()));

  uint32_t out_oid;
  if (col_name.GetLength() == 0) {
    out_oid = table_oid.UnderlyingValue();
  } else {
    const auto &schema = exec_ctx->GetAccessor()->GetSchema(table_oid);
    out_oid = schema.GetColumn(std::string(col_name.StringView())).Oid().UnderlyingValue();
  }

  *oid_var = out_oid;
}

VM_OP_WARM void OpTestCatalogIndexLookup(uint32_t *oid_var, terrier::execution::exec::ExecutionContext *exec_ctx,
                                         const uint8_t *index_name_str, uint32_t index_name_length) {
  // TODO(WAN): wasteful std::string
  terrier::execution::sql::StringVal table_name{reinterpret_cast<const char *>(index_name_str), index_name_length};
  terrier::catalog::index_oid_t index_oid = exec_ctx->GetAccessor()->GetIndexOid(std::string(table_name.StringView()));
  *oid_var = index_oid.UnderlyingValue();
}

// Macro hygiene
#undef VM_OP_COLD
#undef VM_OP_WARM
#undef VM_OP_HOT
#undef VM_OP

}  // extern "C"
