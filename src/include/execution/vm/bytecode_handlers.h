#pragma once

#include <cstdint>
#include "execution/sql/projected_columns_iterator.h"

#include "execution/util/common.h"

#include "execution/exec/execution_context.h"
#include "execution/sql/aggregation_hash_table.h"
#include "execution/sql/aggregators.h"
#include "execution/sql/filter_manager.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/join_hash_table.h"
#include "execution/sql/sorter.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql/thread_state_container.h"
#include "execution/sql/value_functions.h"
#include "execution/util/hash.h"
#include "execution/util/macros.h"

// All VM bytecode op handlers must use this macro
#define VM_OP

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

INT_TYPES(COMPARISONS);

#undef COMPARISONS

VM_OP_HOT void OpNot(bool *const result, const bool input) { *result = !input; }

// ---------------------------------------------------------
// Primitive arithmetic
// ---------------------------------------------------------

#define ARITHMETIC(type, ...)                                                                                 \
  /* Primitive addition */                                                                                    \
  VM_OP_HOT void OpAdd##_##type(type *result, type lhs, type rhs) { *result = static_cast<type>(lhs + rhs); } \
                                                                                                              \
  /* Primitive subtraction */                                                                                 \
  VM_OP_HOT void OpSub##_##type(type *result, type lhs, type rhs) { *result = static_cast<type>(lhs - rhs); } \
                                                                                                              \
  /* Primitive multiplication */                                                                              \
  VM_OP_HOT void OpMul##_##type(type *result, type lhs, type rhs) { *result = static_cast<type>(lhs * rhs); } \
                                                                                                              \
  /* Primitive division (no zero-check) */                                                                    \
  VM_OP_HOT void OpDiv##_##type(type *result, type lhs, type rhs) {                                           \
    TPL_ASSERT(rhs != 0, "Division-by-zero error!");                                                          \
    *result = static_cast<type>(lhs / rhs);                                                                   \
  }                                                                                                           \
                                                                                                              \
  /* Primitive modulo-remainder (no zero-check) */                                                            \
  VM_OP_HOT void OpRem##_##type(type *result, type lhs, type rhs) {                                           \
    TPL_ASSERT(rhs != 0, "Division-by-zero error!");                                                          \
    *result = static_cast<type>(lhs % rhs);                                                                   \
  }                                                                                                           \
                                                                                                              \
  /* Primitive negation */                                                                                    \
  VM_OP_HOT void OpNeg##_##type(type *result, type input) { *result = static_cast<type>(-input); }

INT_TYPES(ARITHMETIC);

#undef ARITHMETIC

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

VM_OP_HOT void OpDeref1(i8 *dest, const i8 *const src) { *dest = *src; }

VM_OP_HOT void OpDeref2(i16 *dest, const i16 *const src) { *dest = *src; }

VM_OP_HOT void OpDeref4(i32 *dest, const i32 *const src) { *dest = *src; }

VM_OP_HOT void OpDeref8(i64 *dest, const i64 *const src) { *dest = *src; }

VM_OP_HOT void OpDerefN(byte *dest, const byte *const src, u32 len) { std::memcpy(dest, src, len); }

VM_OP_HOT void OpAssign1(i8 *dest, i8 src) { *dest = src; }

VM_OP_HOT void OpAssign2(i16 *dest, i16 src) { *dest = src; }

VM_OP_HOT void OpAssign4(i32 *dest, i32 src) { *dest = src; }

VM_OP_HOT void OpAssign8(i64 *dest, i64 src) { *dest = src; }

VM_OP_HOT void OpAssignImm1(i8 *dest, i8 src) { *dest = src; }

VM_OP_HOT void OpAssignImm2(i16 *dest, i16 src) { *dest = src; }

VM_OP_HOT void OpAssignImm4(i32 *dest, i32 src) { *dest = src; }

VM_OP_HOT void OpAssignImm8(i64 *dest, i64 src) { *dest = src; }

VM_OP_HOT void OpLea(byte **dest, byte *base, u32 offset) { *dest = base + offset; }

VM_OP_HOT void OpLeaScaled(byte **dest, byte *base, u32 index, u32 scale, u32 offset) {
  *dest = base + (scale * index) + offset;
}

VM_OP_HOT bool OpJump() { return true; }

VM_OP_HOT bool OpJumpIfTrue(bool cond) { return cond; }

VM_OP_HOT bool OpJumpIfFalse(bool cond) { return !cond; }

VM_OP_HOT void OpCall(UNUSED u16 func_id, UNUSED u16 num_args) {}

VM_OP_HOT void OpReturn() {}

// ---------------------------------------------------------
// Execution Context
// ---------------------------------------------------------

VM_OP_HOT void OpExecutionContextGetMemoryPool(tpl::sql::MemoryPool **const memory,
                                               tpl::exec::ExecutionContext *const exec_ctx) {
  *memory = exec_ctx->GetMemoryPool();
}

void OpThreadStateContainerInit(tpl::sql::ThreadStateContainer *thread_state_container, tpl::sql::MemoryPool *memory);

VM_OP_HOT void OpThreadStateContainerReset(tpl::sql::ThreadStateContainer *thread_state_container, u32 size,
                                           tpl::sql::ThreadStateContainer::InitFn init_fn,
                                           tpl::sql::ThreadStateContainer::DestroyFn destroy_fn, void *ctx) {
  thread_state_container->Reset(size, init_fn, destroy_fn, ctx);
}

void OpThreadStateContainerFree(tpl::sql::ThreadStateContainer *thread_state_container);

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorInit(tpl::sql::TableVectorIterator *iter, u32 db_oid, u32 ns_oid, u32 table_oid,
                               tpl::exec::ExecutionContext *exec_ctx);

void OpTableVectorIteratorPerformInit(tpl::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorNext(bool *has_more, tpl::sql::TableVectorIterator *iter) {
  *has_more = iter->Advance();
}

void OpTableVectorIteratorFree(tpl::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorGetPCI(tpl::sql::ProjectedColumnsIterator **pci,
                                           tpl::sql::TableVectorIterator *iter) {
  *pci = iter->projected_columns_iterator();
}

VM_OP_HOT void OpParallelScanTable(const u32 db_oid, const u32 table_oid, tpl::exec::ExecutionContext *const ctx,
                                   tpl::sql::ThreadStateContainer *const thread_state_container,
                                   const tpl::sql::TableVectorIterator::ScanFn scanner) {
  tpl::sql::TableVectorIterator::ParallelScan(db_oid, table_oid, ctx, thread_state_container, scanner);
}

VM_OP_HOT void OpPCIIsFiltered(bool *is_filtered, tpl::sql::ProjectedColumnsIterator *pci) {
  *is_filtered = pci->IsFiltered();
}

VM_OP_HOT void OpPCIHasNext(bool *has_more, tpl::sql::ProjectedColumnsIterator *pci) { *has_more = pci->HasNext(); }

VM_OP_HOT void OpPCIHasNextFiltered(bool *has_more, tpl::sql::ProjectedColumnsIterator *pci) {
  *has_more = pci->HasNextFiltered();
}

VM_OP_HOT void OpPCIAdvance(tpl::sql::ProjectedColumnsIterator *pci) { pci->Advance(); }

VM_OP_HOT void OpPCIAdvanceFiltered(tpl::sql::ProjectedColumnsIterator *pci) { pci->AdvanceFiltered(); }

VM_OP_HOT void OpPCIMatch(tpl::sql::ProjectedColumnsIterator *pci, bool match) { pci->Match(match); }

VM_OP_HOT void OpPCIReset(tpl::sql::ProjectedColumnsIterator *pci) { pci->Reset(); }

VM_OP_HOT void OpPCIResetFiltered(tpl::sql::ProjectedColumnsIterator *pci) { pci->ResetFiltered(); }

VM_OP_HOT void OpPCIGetSmallInt(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx) {
  // Read
  auto *ptr = iter->Get<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetInteger(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx) {
  // Read
  auto *ptr = iter->Get<i32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetBigInt(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx) {
  // Read
  auto *ptr = iter->Get<i64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetReal(tpl::sql::Real *out, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx) {
  // Read
  auto *ptr = iter->Get<f32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read real value");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDouble(tpl::sql::Real *out, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx) {
  // Read
  auto *ptr = iter->Get<f64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read double value");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDecimal(tpl::sql::Decimal *out, UNUSED tpl::sql::ProjectedColumnsIterator *iter,
                               UNUSED u32 col_idx) {
  // Set
  out->is_null = false;
  out->val = 0;
}

VM_OP_HOT void OpPCIGetSmallIntNull(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i16, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetIntegerNull(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetBigIntNull(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i64, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetRealNull(tpl::sql::Real *out, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read real value");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDoubleNull(tpl::sql::Real *out, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f64, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read double value");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDecimalNull(tpl::sql::Decimal *out, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx) {
  out->val = 0;
  out->is_null = false;
}

void OpPCIFilterEqual(u32 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_id, i8 type, i64 val);

void OpPCIFilterGreaterThan(u32 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_id, i8 type, i64 val);

void OpPCIFilterGreaterThanEqual(u32 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_id, i8 type, i64 val);

void OpPCIFilterLessThan(u32 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_id, i8 type, i64 val);

void OpPCIFilterLessThanEqual(u32 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_id, i8 type, i64 val);

void OpPCIFilterNotEqual(u32 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_id, i8 type, i64 val);

// ---------------------------------------------------------
// Hashing
// ---------------------------------------------------------

VM_OP_HOT void OpHashInt(hash_t *hash_val, tpl::sql::Integer *input) {
  *hash_val = tpl::util::Hasher::Hash(reinterpret_cast<u8 *>(&input->val), sizeof(input->val));
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashReal(hash_t *hash_val, tpl::sql::Real *input) {
  *hash_val = tpl::util::Hasher::Hash(reinterpret_cast<u8 *>(&input->val), sizeof(input->val));
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashString(hash_t *hash_val, tpl::sql::VarBuffer *input) {
  *hash_val = tpl::util::Hasher::Hash(input->str, input->len);
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashCombine(hash_t *hash_val, hash_t new_hash_val) {
  *hash_val = tpl::util::Hasher::CombineHashes(*hash_val, new_hash_val);
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

void OpFilterManagerInit(tpl::sql::FilterManager *filter_manager);

void OpFilterManagerStartNewClause(tpl::sql::FilterManager *filter_manager);

void OpFilterManagerInsertFlavor(tpl::sql::FilterManager *filter_manager, tpl::sql::FilterManager::MatchFn flavor);

void OpFilterManagerFinalize(tpl::sql::FilterManager *filter_manager);

void OpFilterManagerRunFilters(tpl::sql::FilterManager *filter_manager, tpl::sql::ProjectedColumnsIterator *pci);

void OpFilterManagerFree(tpl::sql::FilterManager *filter_manager);

// ---------------------------------------------------------
// Scalar SQL comparisons
// ---------------------------------------------------------

VM_OP_HOT void OpForceBoolTruth(bool *result, tpl::sql::BoolVal *input) { *result = input->ForceTruth(); }

VM_OP_HOT void OpInitBool(tpl::sql::BoolVal *result, bool input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpInitInteger(tpl::sql::Integer *result, i32 input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpInitReal(tpl::sql::Real *result, double input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpGreaterThanInteger(tpl::sql::BoolVal *const result, const tpl::sql::Integer *const left,
                                    const tpl::sql::Integer *const right) {
  result->val = (left->val > right->val);
  result->is_null = (left->is_null || right->is_null);
}

VM_OP_HOT void OpGreaterThanEqualInteger(tpl::sql::BoolVal *const result, tpl::sql::Integer *left,
                                         tpl::sql::Integer *right) {
  result->val = (left->val >= right->val);
  result->is_null = (left->is_null || right->is_null);
}

VM_OP_HOT void OpEqualInteger(tpl::sql::BoolVal *const result, const tpl::sql::Integer *const left,
                              const tpl::sql::Integer *const right) {
  result->val = (left->val == right->val);
  result->is_null = (left->is_null || right->is_null);
}

VM_OP_HOT void OpLessThanInteger(tpl::sql::BoolVal *const result, const tpl::sql::Integer *const left,
                                 const tpl::sql::Integer *const right) {
  result->val = (left->val < right->val);
  result->is_null = (left->is_null || right->is_null);
}

VM_OP_HOT void OpLessThanEqualInteger(tpl::sql::BoolVal *const result, const tpl::sql::Integer *const left,
                                      const tpl::sql::Integer *const right) {
  result->val = (left->val <= right->val);
  result->is_null = (left->is_null || right->is_null);
}

VM_OP_HOT void OpNotEqualInteger(tpl::sql::BoolVal *const result, const tpl::sql::Integer *const left,
                                 const tpl::sql::Integer *const right) {
  result->val = (left->val != right->val);
  result->is_null = (left->is_null || right->is_null);
}

// ---------------------------------------------------------
// SQL Aggregations
// ---------------------------------------------------------

void OpAggregationHashTableInit(tpl::sql::AggregationHashTable *agg_hash_table, tpl::sql::MemoryPool *memory,
                                u32 payload_size);

VM_OP_HOT void OpAggregationHashTableInsert(byte **result, tpl::sql::AggregationHashTable *agg_hash_table,
                                            hash_t hash_val) {
  *result = agg_hash_table->Insert(hash_val);
}

VM_OP_HOT void OpAggregationHashTableLookup(byte **result, tpl::sql::AggregationHashTable *const agg_hash_table,
                                            const hash_t hash_val,
                                            const tpl::sql::AggregationHashTable::KeyEqFn key_eq_fn,
                                            tpl::sql::ProjectedColumnsIterator *iters[]) {
  *result = agg_hash_table->Lookup(hash_val, key_eq_fn, iters);
}

VM_OP_HOT void OpAggregationHashTableProcessBatch(tpl::sql::AggregationHashTable *const agg_hash_table,
                                                  tpl::sql::ProjectedColumnsIterator *iters[],
                                                  const tpl::sql::AggregationHashTable::HashFn hash_fn,
                                                  const tpl::sql::AggregationHashTable::KeyEqFn key_eq_fn,
                                                  const tpl::sql::AggregationHashTable::InitAggFn init_agg_fn,
                                                  const tpl::sql::AggregationHashTable::AdvanceAggFn merge_agg_fn) {
  agg_hash_table->ProcessBatch(iters, hash_fn, key_eq_fn, init_agg_fn, merge_agg_fn);
}

void OpAggregationHashTableFree(tpl::sql::AggregationHashTable *agg_hash_table);

//
// COUNT
//

VM_OP_HOT void OpCountAggregateInit(tpl::sql::CountAggregate *agg) { new (agg) tpl::sql::CountAggregate(); }

VM_OP_HOT void OpCountAggregateAdvance(tpl::sql::CountAggregate *agg, tpl::sql::Val *val) { agg->Advance(val); }

VM_OP_HOT void OpCountAggregateMerge(tpl::sql::CountAggregate *agg_1, tpl::sql::CountAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountAggregateReset(tpl::sql::CountAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpCountAggregateGetResult(tpl::sql::Integer *result, tpl::sql::CountAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountAggregateFree(tpl::sql::CountAggregate *agg) { agg->~CountAggregate(); }

//
// COUNT(*)
//

VM_OP_HOT void OpCountStarAggregateInit(tpl::sql::CountStarAggregate *agg) { new (agg) tpl::sql::CountStarAggregate(); }

VM_OP_HOT void OpCountStarAggregateAdvance(tpl::sql::CountStarAggregate *agg, tpl::sql::Val *val) { agg->Advance(val); }

VM_OP_HOT void OpCountStarAggregateMerge(tpl::sql::CountStarAggregate *agg_1, tpl::sql::CountStarAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountStarAggregateReset(tpl::sql::CountStarAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpCountStarAggregateGetResult(tpl::sql::Integer *result, tpl::sql::CountStarAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountStarAggregateFree(tpl::sql::CountStarAggregate *agg) { agg->~CountStarAggregate(); }

//
// SUM(int_type)
//

VM_OP_HOT void OpIntegerSumAggregateInit(tpl::sql::IntegerSumAggregate *agg) {
  new (agg) tpl::sql::IntegerSumAggregate();
}

VM_OP_HOT void OpIntegerSumAggregateAdvance(tpl::sql::IntegerSumAggregate *agg, tpl::sql::Integer *val) {
  agg->Advance(val);
}

VM_OP_HOT void OpIntegerSumAggregateAdvanceNullable(tpl::sql::IntegerSumAggregate *agg, tpl::sql::Integer *val) {
  agg->AdvanceNullable(val);
}

VM_OP_HOT void OpIntegerSumAggregateMerge(tpl::sql::IntegerSumAggregate *agg_1, tpl::sql::IntegerSumAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerSumAggregateReset(tpl::sql::IntegerSumAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerSumAggregateGetResult(tpl::sql::Integer *result, tpl::sql::IntegerSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpIntegerSumAggregateFree(tpl::sql::IntegerSumAggregate *agg) { agg->~IntegerSumAggregate(); }

//
// MAX(int_type)
//

VM_OP_HOT void OpIntegerMaxAggregateInit(tpl::sql::IntegerMaxAggregate *agg) {
  new (agg) tpl::sql::IntegerMaxAggregate();
}

VM_OP_HOT void OpIntegerMaxAggregateAdvance(tpl::sql::IntegerMaxAggregate *agg, tpl::sql::Integer *val) {
  agg->Advance(val);
}

VM_OP_HOT void OpIntegerMaxAggregateAdvanceNullable(tpl::sql::IntegerMaxAggregate *agg, tpl::sql::Integer *val) {
  agg->AdvanceNullable(val);
}

VM_OP_HOT void OpIntegerMaxAggregateMerge(tpl::sql::IntegerMaxAggregate *agg_1, tpl::sql::IntegerMaxAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMaxAggregateReset(tpl::sql::IntegerMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerMaxAggregateGetResult(tpl::sql::Integer *result, tpl::sql::IntegerMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpIntegerMaxAggregateFree(tpl::sql::IntegerMaxAggregate *agg) { agg->~IntegerMaxAggregate(); }

//
// MIN(int_type)
//

VM_OP_HOT void OpIntegerMinAggregateInit(tpl::sql::IntegerMinAggregate *agg) {
  new (agg) tpl::sql::IntegerMinAggregate();
}

VM_OP_HOT void OpIntegerMinAggregateAdvance(tpl::sql::IntegerMinAggregate *agg, tpl::sql::Integer *val) {
  agg->Advance(val);
}

VM_OP_HOT void OpIntegerMinAggregateAdvanceNullable(tpl::sql::IntegerMinAggregate *agg, tpl::sql::Integer *val) {
  agg->AdvanceNullable(val);
}

VM_OP_HOT void OpIntegerMinAggregateMerge(tpl::sql::IntegerMinAggregate *agg_1, tpl::sql::IntegerMinAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMinAggregateReset(tpl::sql::IntegerMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerMinAggregateGetResult(tpl::sql::Integer *result, tpl::sql::IntegerMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpIntegerMinAggregateFree(tpl::sql::IntegerMinAggregate *agg) { agg->~IntegerMinAggregate(); }

//
// AVG(int_type)
//

VM_OP_HOT void OpIntegerAvgAggregateInit(tpl::sql::IntegerAvgAggregate *agg) {
  new (agg) tpl::sql::IntegerAvgAggregate();
}

VM_OP_HOT void OpIntegerAvgAggregateAdvance(tpl::sql::IntegerAvgAggregate *agg, tpl::sql::Integer *val) {
  agg->Advance(val);
}

VM_OP_HOT void OpIntegerAvgAggregateAdvanceNullable(tpl::sql::IntegerAvgAggregate *agg, tpl::sql::Integer *val) {
  agg->AdvanceNullable(val);
}

VM_OP_HOT void OpIntegerAvgAggregateMerge(tpl::sql::IntegerAvgAggregate *agg_1, tpl::sql::IntegerAvgAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerAvgAggregateReset(tpl::sql::IntegerAvgAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerAvgAggregateGetResult(tpl::sql::Integer *result, tpl::sql::IntegerAvgAggregate *agg) {
  *result = agg->GetResultAvg();
}

VM_OP_HOT void OpIntegerAvgAggregateFree(tpl::sql::IntegerAvgAggregate *agg) { agg->~IntegerAvgAggregate(); }

// ---------------------------------------------------------
// Hash Joins
// ---------------------------------------------------------

void OpJoinHashTableInit(tpl::sql::JoinHashTable *join_hash_table, tpl::sql::MemoryPool *memory, u32 tuple_size);

VM_OP_HOT void OpJoinHashTableAllocTuple(byte **result, tpl::sql::JoinHashTable *join_hash_table, hash_t hash) {
  *result = join_hash_table->AllocInputTuple(hash);
}

void OpJoinHashTableBuild(tpl::sql::JoinHashTable *join_hash_table);

void OpJoinHashTableBuildParallel(tpl::sql::JoinHashTable *join_hash_table,
                                  tpl::sql::ThreadStateContainer *thread_state_container, u32 jht_offset);

void OpJoinHashTableFree(tpl::sql::JoinHashTable *join_hash_table);

// ---------------------------------------------------------
// Sorting
// ---------------------------------------------------------

void OpSorterInit(tpl::sql::Sorter *sorter, tpl::sql::MemoryPool *memory, tpl::sql::Sorter::ComparisonFunction cmp_fn,
                  u32 tuple_size);

VM_OP_HOT void OpSorterAllocTuple(byte **result, tpl::sql::Sorter *sorter) { *result = sorter->AllocInputTuple(); }

VM_OP_HOT void OpSorterAllocTupleTopK(byte **result, tpl::sql::Sorter *sorter, u64 top_k) {
  *result = sorter->AllocInputTupleTopK(top_k);
}

VM_OP_HOT void OpSorterAllocTupleTopKFinish(tpl::sql::Sorter *sorter, u64 top_k) {
  sorter->AllocInputTupleTopKFinish(top_k);
}

void OpSorterSort(tpl::sql::Sorter *sorter);

void OpSorterSortParallel(tpl::sql::Sorter *sorter, tpl::sql::ThreadStateContainer *thread_state_container,
                          u32 sorter_offset);

void OpSorterSortTopKParallel(tpl::sql::Sorter *sorter, tpl::sql::ThreadStateContainer *thread_state_container,
                              u32 sorter_offset, u64 top_k);

void OpSorterFree(tpl::sql::Sorter *sorter);

void OpSorterIteratorInit(tpl::sql::SorterIterator *iter, tpl::sql::Sorter *sorter);

VM_OP_HOT void OpSorterIteratorHasNext(bool *has_more, tpl::sql::SorterIterator *iter) { *has_more = iter->HasNext(); }

VM_OP_HOT void OpSorterIteratorNext(tpl::sql::SorterIterator *iter) { iter->Next(); }

VM_OP_HOT void OpSorterIteratorGetRow(const byte **row, tpl::sql::SorterIterator *iter) { *row = iter->GetRow(); }

void OpSorterIteratorFree(tpl::sql::SorterIterator *iter);

// ---------------------------------------------------------
// Trig functions
// ---------------------------------------------------------

VM_OP_HOT void OpAcos(tpl::sql::Real *result, tpl::sql::Real *input) { tpl::sql::ACos::Execute<true>(input, result); }

VM_OP_HOT void OpAsin(tpl::sql::Real *result, tpl::sql::Real *input) { tpl::sql::ASin::Execute<true>(input, result); }

VM_OP_HOT void OpAtan(tpl::sql::Real *result, tpl::sql::Real *input) { tpl::sql::ATan::Execute<true>(input, result); }

VM_OP_HOT void OpAtan2(tpl::sql::Real *result, tpl::sql::Real *arg_1, tpl::sql::Real *arg_2) {
  tpl::sql::ATan2::Execute<true>(arg_1, arg_2, result);
}

VM_OP_HOT void OpCos(tpl::sql::Real *result, tpl::sql::Real *input) { tpl::sql::Cos::Execute<true>(input, result); }

VM_OP_HOT void OpCot(tpl::sql::Real *result, tpl::sql::Real *input) { tpl::sql::Cot::Execute<true>(input, result); }

VM_OP_HOT void OpSin(tpl::sql::Real *result, tpl::sql::Real *input) { tpl::sql::Sin::Execute<true>(input, result); }

VM_OP_HOT void OpTan(tpl::sql::Real *result, tpl::sql::Real *input) { tpl::sql::Tan::Execute<true>(input, result); }

// ---------------------------------------------------------------
// Index Iterator
// ---------------------------------------------------------------
void OpIndexIteratorInit(tpl::sql::IndexIterator *iter, uint32_t index_oid, tpl::exec::ExecutionContext *exec_ctx);
void OpIndexIteratorScanKey(tpl::sql::IndexIterator *iter, byte *key);
void OpIndexIteratorFree(tpl::sql::IndexIterator *iter);

VM_OP_HOT void OpIndexIteratorHasNext(bool *has_more, tpl::sql::IndexIterator *iter) { *has_more = iter->HasNext(); }

VM_OP_HOT void OpIndexIteratorAdvance(tpl::sql::IndexIterator *iter) { iter->Advance(); }

VM_OP_HOT void OpIndexIteratorGetSmallInt(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u32 col_idx) {
  // Read
  auto *ptr = iter->Get<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetInteger(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u32 col_idx) {
  // Read
  auto *ptr = iter->Get<i32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetBigInt(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u32 col_idx) {
  // Read
  auto *ptr = iter->Get<i64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDecimal(tpl::sql::Decimal *out, UNUSED tpl::sql::IndexIterator *iter,
                                         UNUSED u32 col_idx) {
  // Set
  out->is_null = false;
  out->val = 0;
}

VM_OP_HOT void OpIndexIteratorGetSmallIntNull(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i16, true>(col_idx, &null);
  // TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetIntegerNull(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i32, true>(col_idx, &null);
  // TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetBigIntNull(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u32 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i64, true>(col_idx, &null);
  // TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDecimalNull(tpl::sql::Decimal *out, tpl::sql::IndexIterator *iter, u32 col_idx) {
  out->val = 0;
  out->is_null = false;
}

// ---------------------------------------------------------------
// Insert Calls
// ---------------------------------------------------------------

void OpInsert(tpl::exec::ExecutionContext *exec_ctx, u32 db_oid, u32 ns_oid, u32 table_oid, byte *values_ptr);

// ---------------------------------------------------------------
// Output Calls
// ---------------------------------------------------------------

void OpOutputAlloc(tpl::exec::ExecutionContext *exec_ctx, byte **result);

void OpOutputAdvance(tpl::exec::ExecutionContext *exec_ctx);

void OpOutputSetNull(tpl::exec::ExecutionContext *exec_ctx, u32 idx);

void OpOutputFinalize(tpl::exec::ExecutionContext *exec_ctx);

}  // extern "C"
