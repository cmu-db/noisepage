#pragma once

#include <cstdint>
#include "execution/sql/projected_columns_iterator.h"

#include "execution/util/common.h"

#include "execution/exec/execution_context.h"
#include "execution/sql/aggregation_hash_table.h"
#include "execution/sql/aggregators.h"
#include "execution/sql/filter_manager.h"
#include "execution/sql/functions/arithmetic_functions.h"
#include "execution/sql/functions/comparison_functions.h"
#include "execution/sql/functions/is_null_predicate.h"
#include "execution/sql/functions/string_functions.h"
#include "execution/sql/index_iterator.h"
#include "execution/sql/join_hash_table.h"
#include "execution/sql/sorter.h"
#include "execution/sql/table_vector_iterator.h"
#include "execution/sql/thread_state_container.h"
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

#define MODULAR(type, ...)                                          \
  /* Primitive modulo-remainder (no zero-check) */                  \
  VM_OP_HOT void OpRem##_##type(type *result, type lhs, type rhs) { \
    TPL_ASSERT(rhs != 0, "Division-by-zero error!");                \
    *result = static_cast<type>(lhs % rhs);                         \
  }

INT_TYPES(MODULAR)

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
  /* Primitive negation */                                                                                    \
  VM_OP_HOT void OpNeg##_##type(type *result, type input) { *result = static_cast<type>(-input); }            \
                                                                                                              \
  /* Primitive division (no zero-check) */                                                                    \
  VM_OP_HOT void OpDiv##_##type(type *result, type lhs, type rhs) {                                           \
    TPL_ASSERT(rhs != 0, "Division-by-zero error!");                                                          \
    *result = static_cast<type>(lhs / rhs);                                                                   \
  }

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

VM_OP_HOT void OpAssignImm4F(f32 *dest, f32 src) { *dest = src; }

VM_OP_HOT void OpAssignImm8F(f64 *dest, f64 src) { *dest = src; }

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

VM_OP_HOT void OpExecutionContextGetMemoryPool(terrier::execution::sql::MemoryPool **const memory,
                                               terrier::execution::exec::ExecutionContext *const exec_ctx) {
  *memory = exec_ctx->GetMemoryPool();
}

void OpThreadStateContainerInit(terrier::execution::sql::ThreadStateContainer *thread_state_container,
                                terrier::execution::sql::MemoryPool *memory);

VM_OP_HOT void OpThreadStateContainerReset(terrier::execution::sql::ThreadStateContainer *thread_state_container,
                                           u32 size, terrier::execution::sql::ThreadStateContainer::InitFn init_fn,
                                           terrier::execution::sql::ThreadStateContainer::DestroyFn destroy_fn,
                                           void *ctx) {
  thread_state_container->Reset(size, init_fn, destroy_fn, ctx);
}

VM_OP_HOT void OpThreadStateContainerIterate(terrier::execution::sql::ThreadStateContainer *thread_state_container,
                                             void *const state,
                                             terrier::execution::sql::ThreadStateContainer::IterateFn iterate_fn) {
  thread_state_container->IterateStates(state, iterate_fn);
}

void OpThreadStateContainerFree(terrier::execution::sql::ThreadStateContainer *thread_state_container);

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorConstruct(terrier::execution::sql::TableVectorIterator *iter, u32 table_oid,
                                    terrier::execution::exec::ExecutionContext *exec_ctx);

VM_OP_HOT void OpTableVectorIteratorAddCol(terrier::execution::sql::TableVectorIterator *iter, u32 col_oid) {
  iter->AddCol(col_oid);
}

void OpTableVectorIteratorPerformInit(terrier::execution::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorNext(bool *has_more, terrier::execution::sql::TableVectorIterator *iter) {
  *has_more = iter->Advance();
}

void OpTableVectorIteratorFree(terrier::execution::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorGetPCI(terrier::execution::sql::ProjectedColumnsIterator **pci,
                                           terrier::execution::sql::TableVectorIterator *iter) {
  *pci = iter->projected_columns_iterator();
}

VM_OP_HOT void OpParallelScanTable(const u32 db_oid, const u32 table_oid, void *const query_state,
                                   terrier::execution::sql::ThreadStateContainer *const thread_states,
                                   const terrier::execution::sql::TableVectorIterator::ScanFn scanner) {
  terrier::execution::sql::TableVectorIterator::ParallelScan(db_oid, table_oid, query_state, thread_states, scanner);
}

VM_OP_HOT void OpPCIIsFiltered(bool *is_filtered, terrier::execution::sql::ProjectedColumnsIterator *pci) {
  *is_filtered = pci->IsFiltered();
}

VM_OP_HOT void OpPCIHasNext(bool *has_more, terrier::execution::sql::ProjectedColumnsIterator *pci) {
  *has_more = pci->HasNext();
}

VM_OP_HOT void OpPCIHasNextFiltered(bool *has_more, terrier::execution::sql::ProjectedColumnsIterator *pci) {
  *has_more = pci->HasNextFiltered();
}

VM_OP_HOT void OpPCIAdvance(terrier::execution::sql::ProjectedColumnsIterator *pci) { pci->Advance(); }

VM_OP_HOT void OpPCIAdvanceFiltered(terrier::execution::sql::ProjectedColumnsIterator *pci) { pci->AdvanceFiltered(); }

VM_OP_HOT void OpPCIMatch(terrier::execution::sql::ProjectedColumnsIterator *pci, bool match) { pci->Match(match); }

VM_OP_HOT void OpPCIReset(terrier::execution::sql::ProjectedColumnsIterator *pci) { pci->Reset(); }

VM_OP_HOT void OpPCIResetFiltered(terrier::execution::sql::ProjectedColumnsIterator *pci) { pci->ResetFiltered(); }

VM_OP_HOT void OpPCIGetTinyInt(terrier::execution::sql::Integer *out,
                               terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i8, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetSmallInt(terrier::execution::sql::Integer *out,
                                terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetInteger(terrier::execution::sql::Integer *out,
                               terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetBigInt(terrier::execution::sql::Integer *out,
                              terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetReal(terrier::execution::sql::Real *out, terrier::execution::sql::ProjectedColumnsIterator *iter,
                            u16 col_idx) {
  // Read
  auto *ptr = iter->Get<f32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read real value");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDouble(terrier::execution::sql::Real *out,
                              terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<f64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read double value");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDecimal(terrier::execution::sql::Decimal *out,
                               UNUSED terrier::execution::sql::ProjectedColumnsIterator *iter, UNUSED u16 col_idx) {
  // TODO(Amadou): Implement once the representation of Decimal is settled upon.
  // The sql::Decimal class does not seem to match the storage layer's DECIMAL type as it needs a precision and
  // a scale.
  out->is_null = false;
  out->val = 0;
}

VM_OP_HOT void OpPCIGetDate(terrier::execution::sql::Date *out, terrier::execution::sql::ProjectedColumnsIterator *iter,
                            u16 col_idx) {
  // Read
  auto *ptr = iter->Get<u32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read date");

  // Set
  out->is_null = false;
  out->int_val = *ptr;
}

VM_OP_HOT void OpPCIGetVarlen(terrier::execution::sql::StringVal *out,
                              terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *varlen = iter->Get<terrier::storage::VarlenEntry, false>(col_idx, nullptr);
  TPL_ASSERT(varlen != nullptr, "Null pointer when trying to read varlen");

  // Set
  *out = terrier::execution::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
}

VM_OP_HOT void OpPCIGetTinyIntNull(terrier::execution::sql::Integer *out,
                                   terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i8, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetSmallIntNull(terrier::execution::sql::Integer *out,
                                    terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i16, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetIntegerNull(terrier::execution::sql::Integer *out,
                                   terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetBigIntNull(terrier::execution::sql::Integer *out,
                                  terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i64, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetRealNull(terrier::execution::sql::Real *out,
                                terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read real value");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDoubleNull(terrier::execution::sql::Real *out,
                                  terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f64, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read double value");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDecimalNull(terrier::execution::sql::Decimal *out,
                                   terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  out->val = 0;
  out->is_null = false;
}

VM_OP_HOT void OpPCIGetDateNull(terrier::execution::sql::Date *out,
                                terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<u32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read date");

  // Set
  out->is_null = null;
  out->int_val = *ptr;
}

VM_OP_HOT void OpPCIGetVarlenNull(terrier::execution::sql::StringVal *out,
                                  terrier::execution::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *varlen = iter->Get<terrier::storage::VarlenEntry, true>(col_idx, &null);
  TPL_ASSERT(varlen != nullptr, "Null pointer when trying to read varlen");

  // Set
  if (null) {
    out->is_null = null;
  } else {
    *out = terrier::execution::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
  }
}

void OpPCIFilterEqual(u64 *size, terrier::execution::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type,
                      i64 val);

void OpPCIFilterGreaterThan(u64 *size, terrier::execution::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type,
                            i64 val);

void OpPCIFilterGreaterThanEqual(u64 *size, terrier::execution::sql::ProjectedColumnsIterator *iter, u32 col_idx,
                                 i8 type, i64 val);

void OpPCIFilterLessThan(u64 *size, terrier::execution::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type,
                         i64 val);

void OpPCIFilterLessThanEqual(u64 *size, terrier::execution::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type,
                              i64 val);

void OpPCIFilterNotEqual(u64 *size, terrier::execution::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type,
                         i64 val);

// ---------------------------------------------------------
// Hashing
// ---------------------------------------------------------

VM_OP_HOT void OpHashInt(hash_t *hash_val, terrier::execution::sql::Integer *input) {
  *hash_val = terrier::execution::util::Hasher::Hash<terrier::execution::util::HashMethod::Crc>(input->val);
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashReal(hash_t *hash_val, terrier::execution::sql::Real *input) {
  *hash_val = terrier::execution::util::Hasher::Hash<terrier::execution::util::HashMethod::Crc>(input->val);
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashString(hash_t *hash_val, terrier::execution::sql::StringVal *input) {
  *hash_val = terrier::execution::util::Hasher::Hash<terrier::execution::util::HashMethod::xxHash3>(
      reinterpret_cast<const u8 *>(input->Content()), input->len);
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashCombine(hash_t *hash_val, hash_t new_hash_val) {
  *hash_val = terrier::execution::util::Hasher::CombineHashes(*hash_val, new_hash_val);
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

void OpFilterManagerInit(terrier::execution::sql::FilterManager *filter_manager);

void OpFilterManagerStartNewClause(terrier::execution::sql::FilterManager *filter_manager);

void OpFilterManagerInsertFlavor(terrier::execution::sql::FilterManager *filter_manager,
                                 terrier::execution::sql::FilterManager::MatchFn flavor);

void OpFilterManagerFinalize(terrier::execution::sql::FilterManager *filter_manager);

void OpFilterManagerRunFilters(terrier::execution::sql::FilterManager *filter_manager,
                               terrier::execution::sql::ProjectedColumnsIterator *pci);

void OpFilterManagerFree(terrier::execution::sql::FilterManager *filter_manager);

// ---------------------------------------------------------
// Scalar SQL comparisons
// ---------------------------------------------------------

VM_OP_HOT void OpForceBoolTruth(bool *result, terrier::execution::sql::BoolVal *input) {
  *result = input->ForceTruth();
}

VM_OP_HOT void OpInitBool(terrier::execution::sql::BoolVal *result, bool input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpInitInteger(terrier::execution::sql::Integer *result, i32 input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpInitReal(terrier::execution::sql::Real *result, double input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpInitDate(terrier::execution::sql::Date *result, i16 year, u8 month, u8 day) {
  result->is_null = false;
  result->ymd = date::year(year) / month / day;
}

VM_OP_HOT void OpInitString(terrier::execution::sql::StringVal *result, u64 length, uintptr_t data) {
  *result = terrier::execution::sql::StringVal(reinterpret_cast<char *>(data), static_cast<u32>(length));
}

VM_OP_HOT void OpInitVarlen(terrier::execution::sql::StringVal *result, uintptr_t data) {
  auto *varlen = reinterpret_cast<terrier::storage::VarlenEntry *>(data);
  *result = terrier::execution::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
}

#define GEN_SQL_COMPARISONS(TYPE)                                                             \
  VM_OP_HOT void OpGreaterThan##TYPE(terrier::execution::sql::BoolVal *const result,          \
                                     const terrier::execution::sql::TYPE *const left,         \
                                     const terrier::execution::sql::TYPE *const right) {      \
    terrier::execution::sql::ComparisonFunctions::Gt##TYPE(result, *left, *right);            \
  }                                                                                           \
  VM_OP_HOT void OpGreaterThanEqual##TYPE(terrier::execution::sql::BoolVal *const result,     \
                                          const terrier::execution::sql::TYPE *const left,    \
                                          const terrier::execution::sql::TYPE *const right) { \
    terrier::execution::sql::ComparisonFunctions::Ge##TYPE(result, *left, *right);            \
  }                                                                                           \
  VM_OP_HOT void OpEqual##TYPE(terrier::execution::sql::BoolVal *const result,                \
                               const terrier::execution::sql::TYPE *const left,               \
                               const terrier::execution::sql::TYPE *const right) {            \
    terrier::execution::sql::ComparisonFunctions::Eq##TYPE(result, *left, *right);            \
  }                                                                                           \
  VM_OP_HOT void OpLessThan##TYPE(terrier::execution::sql::BoolVal *const result,             \
                                  const terrier::execution::sql::TYPE *const left,            \
                                  const terrier::execution::sql::TYPE *const right) {         \
    terrier::execution::sql::ComparisonFunctions::Lt##TYPE(result, *left, *right);            \
  }                                                                                           \
  VM_OP_HOT void OpLessThanEqual##TYPE(terrier::execution::sql::BoolVal *const result,        \
                                       const terrier::execution::sql::TYPE *const left,       \
                                       const terrier::execution::sql::TYPE *const right) {    \
    terrier::execution::sql::ComparisonFunctions::Le##TYPE(result, *left, *right);            \
  }                                                                                           \
  VM_OP_HOT void OpNotEqual##TYPE(terrier::execution::sql::BoolVal *const result,             \
                                  const terrier::execution::sql::TYPE *const left,            \
                                  const terrier::execution::sql::TYPE *const right) {         \
    terrier::execution::sql::ComparisonFunctions::Ne##TYPE(result, *left, *right);            \
  }

GEN_SQL_COMPARISONS(Integer)
GEN_SQL_COMPARISONS(Real)
GEN_SQL_COMPARISONS(StringVal)
GEN_SQL_COMPARISONS(Date)
#undef GEN_SQL_COMPARISONS

// ----------------------------------
// SQL arithmetic
// ---------------------------------
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
  UNUSED bool overflow;
  terrier::execution::sql::ArithmeticFunctions::Add(result, *left, *right, &overflow);
}

VM_OP_HOT void OpSubInteger(terrier::execution::sql::Integer *const result,
                            const terrier::execution::sql::Integer *const left,
                            const terrier::execution::sql::Integer *const right) {
  UNUSED bool overflow;
  terrier::execution::sql::ArithmeticFunctions::Sub(result, *left, *right, &overflow);
}

VM_OP_HOT void OpMulInteger(terrier::execution::sql::Integer *const result,
                            const terrier::execution::sql::Integer *const left,
                            const terrier::execution::sql::Integer *const right) {
  UNUSED bool overflow;
  terrier::execution::sql::ArithmeticFunctions::Mul(result, *left, *right, &overflow);
}

VM_OP_HOT void OpDivInteger(terrier::execution::sql::Integer *const result,
                            const terrier::execution::sql::Integer *const left,
                            const terrier::execution::sql::Integer *const right) {
  UNUSED bool div_by_zero = false;
  terrier::execution::sql::ArithmeticFunctions::IntDiv(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpRemInteger(terrier::execution::sql::Integer *const result,
                            const terrier::execution::sql::Integer *const left,
                            const terrier::execution::sql::Integer *const right) {
  UNUSED bool div_by_zero = false;
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
  UNUSED bool div_by_zero = false;
  terrier::execution::sql::ArithmeticFunctions::Div(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpRemReal(terrier::execution::sql::Real *const result, const terrier::execution::sql::Real *const left,
                         const terrier::execution::sql::Real *const right) {
  UNUSED bool div_by_zero = false;
  terrier::execution::sql::ArithmeticFunctions::Mod(result, *left, *right, &div_by_zero);
}

// ---------------------------------------------------------
// SQL Aggregations
// ---------------------------------------------------------

void OpAggregationHashTableInit(terrier::execution::sql::AggregationHashTable *agg_hash_table,
                                terrier::execution::sql::MemoryPool *memory, u32 payload_size);

VM_OP_HOT void OpAggregationHashTableInsert(byte **result,
                                            terrier::execution::sql::AggregationHashTable *agg_hash_table,
                                            hash_t hash_val) {
  *result = agg_hash_table->Insert(hash_val);
}

VM_OP_HOT void OpAggregationHashTableLookup(byte **result,
                                            terrier::execution::sql::AggregationHashTable *const agg_hash_table,
                                            const hash_t hash_val,
                                            const terrier::execution::sql::AggregationHashTable::KeyEqFn key_eq_fn,
                                            void *iters[]) {
  *result = agg_hash_table->Lookup(hash_val, key_eq_fn, iters);
}

VM_OP_HOT void OpAggregationHashTableProcessBatch(
    terrier::execution::sql::AggregationHashTable *const agg_hash_table,
    terrier::execution::sql::ProjectedColumnsIterator *iters[],
    const terrier::execution::sql::AggregationHashTable::HashFn hash_fn,
    const terrier::execution::sql::AggregationHashTable::KeyEqFn key_eq_fn,
    const terrier::execution::sql::AggregationHashTable::InitAggFn init_agg_fn,
    const terrier::execution::sql::AggregationHashTable::AdvanceAggFn merge_agg_fn) {
  agg_hash_table->ProcessBatch(iters, hash_fn, key_eq_fn, init_agg_fn, merge_agg_fn);
}

VM_OP_HOT void OpAggregationHashTableTransferPartitions(
    terrier::execution::sql::AggregationHashTable *const agg_hash_table,
    terrier::execution::sql::ThreadStateContainer *const thread_state_container, const u32 agg_ht_offset,
    const terrier::execution::sql::AggregationHashTable::MergePartitionFn merge_partition_fn) {
  agg_hash_table->TransferMemoryAndPartitions(thread_state_container, agg_ht_offset, merge_partition_fn);
}

VM_OP_HOT void OpAggregationHashTableParallelPartitionedScan(
    terrier::execution::sql::AggregationHashTable *const agg_hash_table, void *const query_state,
    terrier::execution::sql::ThreadStateContainer *const thread_state_container,
    const terrier::execution::sql::AggregationHashTable::ScanPartitionFn scan_partition_fn) {
  agg_hash_table->ExecuteParallelPartitionedScan(query_state, thread_state_container, scan_partition_fn);
}

void OpAggregationHashTableFree(terrier::execution::sql::AggregationHashTable *agg_hash_table);

void OpAggregationHashTableIteratorInit(terrier::execution::sql::AggregationHashTableIterator *iter,
                                        terrier::execution::sql::AggregationHashTable *agg_hash_table);

VM_OP_HOT void OpAggregationHashTableIteratorHasNext(bool *has_more,
                                                     terrier::execution::sql::AggregationHashTableIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationHashTableIteratorNext(terrier::execution::sql::AggregationHashTableIterator *iter) {
  iter->Next();
}

VM_OP_HOT void OpAggregationHashTableIteratorGetRow(const byte **row,
                                                    terrier::execution::sql::AggregationHashTableIterator *iter) {
  *row = iter->GetCurrentAggregateRow();
}

void OpAggregationHashTableIteratorFree(terrier::execution::sql::AggregationHashTableIterator *iter);

VM_OP_HOT void OpAggregationOverflowPartitionIteratorHasNext(
    bool *has_more, terrier::execution::sql::AggregationOverflowPartitionIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorNext(
    terrier::execution::sql::AggregationOverflowPartitionIterator *iter) {
  iter->Next();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetHash(
    hash_t *hash_val, terrier::execution::sql::AggregationOverflowPartitionIterator *iter) {
  *hash_val = iter->GetHash();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetRow(
    const byte **row, terrier::execution::sql::AggregationOverflowPartitionIterator *iter) {
  *row = iter->GetPayload();
}

//
// COUNT
//

VM_OP_HOT void OpCountAggregateInit(terrier::execution::sql::CountAggregate *agg) {
  new (agg) terrier::execution::sql::CountAggregate();
}

VM_OP_HOT void OpCountAggregateAdvance(terrier::execution::sql::CountAggregate *agg,
                                       terrier::execution::sql::Val *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpCountAggregateMerge(terrier::execution::sql::CountAggregate *agg_1,
                                     terrier::execution::sql::CountAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountAggregateReset(terrier::execution::sql::CountAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpCountAggregateGetResult(terrier::execution::sql::Integer *result,
                                         terrier::execution::sql::CountAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountAggregateFree(terrier::execution::sql::CountAggregate *agg) { agg->~CountAggregate(); }

//
// COUNT(*)
//

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

//
// SUM(int_type)
//

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

//
// MAX(int_type)
//

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

//
// MIN(int_type)
//

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

//
// SUM(real)
//

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

//
// MAX(real_type)
//

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

//
// MIN(real_type)
//

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

//
// AVG
//

VM_OP_HOT void OpAvgAggregateInit(terrier::execution::sql::AvgAggregate *agg) {
  new (agg) terrier::execution::sql::AvgAggregate();
}

VM_OP_HOT void OpIntegerAvgAggregateAdvance(terrier::execution::sql::AvgAggregate *agg,
                                            const terrier::execution::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealAvgAggregateAdvance(terrier::execution::sql::AvgAggregate *agg,
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

void OpJoinHashTableInit(terrier::execution::sql::JoinHashTable *join_hash_table,
                         terrier::execution::sql::MemoryPool *memory, u32 tuple_size);

VM_OP_HOT void OpJoinHashTableAllocTuple(byte **result, terrier::execution::sql::JoinHashTable *join_hash_table,
                                         hash_t hash) {
  *result = join_hash_table->AllocInputTuple(hash);
}

void OpJoinHashTableBuild(terrier::execution::sql::JoinHashTable *join_hash_table);

void OpJoinHashTableBuildParallel(terrier::execution::sql::JoinHashTable *join_hash_table,
                                  terrier::execution::sql::ThreadStateContainer *thread_state_container,
                                  u32 jht_offset);

VM_OP_HOT void OpJoinHashTableIterInit(terrier::execution::sql::JoinHashTableIterator *result,
                                       terrier::execution::sql::JoinHashTable *join_hash_table, hash_t hash) {
  *result = join_hash_table->Lookup<false>(hash);
}

VM_OP_HOT void OpJoinHashTableIterHasNext(bool *has_more, terrier::execution::sql::JoinHashTableIterator *iterator,
                                          terrier::execution::sql::JoinHashTableIterator::KeyEq key_eq,
                                          void *opaque_ctx, void *probe_tuple) {
  *has_more = iterator->HasNext(key_eq, opaque_ctx, probe_tuple);
}

VM_OP_HOT void OpJoinHashTableIterGetRow(const byte **result,
                                         terrier::execution::sql::JoinHashTableIterator *iterator) {
  *result = iterator->NextMatch()->payload;
}

VM_OP_HOT void OpJoinHashTableIterClose(terrier::execution::sql::JoinHashTableIterator *iterator) {
  iterator->~JoinHashTableIterator();
}

void OpJoinHashTableFree(terrier::execution::sql::JoinHashTable *join_hash_table);

// ---------------------------------------------------------
// Sorting
// ---------------------------------------------------------

void OpSorterInit(terrier::execution::sql::Sorter *sorter, terrier::execution::sql::MemoryPool *memory,
                  terrier::execution::sql::Sorter::ComparisonFunction cmp_fn, u32 tuple_size);

VM_OP_HOT void OpSorterAllocTuple(byte **result, terrier::execution::sql::Sorter *sorter) {
  *result = sorter->AllocInputTuple();
}

VM_OP_HOT void OpSorterAllocTupleTopK(byte **result, terrier::execution::sql::Sorter *sorter, u64 top_k) {
  *result = sorter->AllocInputTupleTopK(top_k);
}

VM_OP_HOT void OpSorterAllocTupleTopKFinish(terrier::execution::sql::Sorter *sorter, u64 top_k) {
  sorter->AllocInputTupleTopKFinish(top_k);
}

void OpSorterSort(terrier::execution::sql::Sorter *sorter);

void OpSorterSortParallel(terrier::execution::sql::Sorter *sorter,
                          terrier::execution::sql::ThreadStateContainer *thread_state_container, u32 sorter_offset);

void OpSorterSortTopKParallel(terrier::execution::sql::Sorter *sorter,
                              terrier::execution::sql::ThreadStateContainer *thread_state_container, u32 sorter_offset,
                              u64 top_k);

void OpSorterFree(terrier::execution::sql::Sorter *sorter);

void OpSorterIteratorInit(terrier::execution::sql::SorterIterator *iter, terrier::execution::sql::Sorter *sorter);

VM_OP_HOT void OpSorterIteratorHasNext(bool *has_more, terrier::execution::sql::SorterIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpSorterIteratorNext(terrier::execution::sql::SorterIterator *iter) { iter->Next(); }

VM_OP_HOT void OpSorterIteratorGetRow(const byte **row, terrier::execution::sql::SorterIterator *iter) {
  *row = iter->GetRow();
}

void OpSorterIteratorFree(terrier::execution::sql::SorterIterator *iter);

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

VM_OP_WARM void OpRoundUpTo(terrier::execution::sql::Real *result, const terrier::execution::sql::Real *v,
                            const terrier::execution::sql::Integer *scale) {
  terrier::execution::sql::ArithmeticFunctions::RoundUpTo(result, *v, *scale);
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

VM_OP_WARM void OpValIsNull(terrier::execution::sql::BoolVal *result, const terrier::execution::sql::Val *val) {
  terrier::execution::sql::IsNullPredicate::IsNull(result, *val);
}

VM_OP_WARM void OpValIsNotNull(terrier::execution::sql::BoolVal *result, const terrier::execution::sql::Val *val) {
  terrier::execution::sql::IsNullPredicate::IsNotNull(result, *val);
}

// ---------------------------------------------------------
// String functions
// ---------------------------------------------------------

VM_OP_WARM void OpCharLength(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::Integer *result,
                             const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::CharLength(ctx, result, *str);
}

VM_OP_WARM void OpLeft(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                       const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *n) {
  terrier::execution::sql::StringFunctions::Left(ctx, result, *str, *n);
}

VM_OP_WARM void OpLength(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::Integer *result,
                         const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::Length(ctx, result, *str);
}

VM_OP_WARM void OpLower(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                        const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::Lower(ctx, result, *str);
}

VM_OP_WARM void OpLPad(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                       const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *len,
                       const terrier::execution::sql::StringVal *pad) {
  terrier::execution::sql::StringFunctions::Lpad(ctx, result, *str, *len, *pad);
}

VM_OP_WARM void OpLTrim(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                        const terrier::execution::sql::StringVal *str,
                        const terrier::execution::sql::StringVal *chars) {
  terrier::execution::sql::StringFunctions::Ltrim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpRepeat(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                         const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *n) {
  terrier::execution::sql::StringFunctions::Repeat(ctx, result, *str, *n);
}

VM_OP_WARM void OpReverse(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                          const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::Reverse(ctx, result, *str);
}

VM_OP_WARM void OpRight(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                        const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *n) {
  terrier::execution::sql::StringFunctions::Right(ctx, result, *str, *n);
}

VM_OP_WARM void OpRPad(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                       const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *n,
                       const terrier::execution::sql::StringVal *pad) {
  terrier::execution::sql::StringFunctions::Rpad(ctx, result, *str, *n, *pad);
}

VM_OP_WARM void OpRTrim(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                        const terrier::execution::sql::StringVal *str,
                        const terrier::execution::sql::StringVal *chars) {
  terrier::execution::sql::StringFunctions::Rtrim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpSplitPart(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                            const terrier::execution::sql::StringVal *str,
                            const terrier::execution::sql::StringVal *delim,
                            const terrier::execution::sql::Integer *field) {
  terrier::execution::sql::StringFunctions::SplitPart(ctx, result, *str, *delim, *field);
}

VM_OP_WARM void OpSubstring(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                            const terrier::execution::sql::StringVal *str, const terrier::execution::sql::Integer *pos,
                            const terrier::execution::sql::Integer *len) {
  terrier::execution::sql::StringFunctions::Substring(ctx, result, *str, *pos, *len);
}

VM_OP_WARM void OpTrim(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                       const terrier::execution::sql::StringVal *str, const terrier::execution::sql::StringVal *chars) {
  terrier::execution::sql::StringFunctions::Trim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpUpper(terrier::execution::exec::ExecutionContext *ctx, terrier::execution::sql::StringVal *result,
                        const terrier::execution::sql::StringVal *str) {
  terrier::execution::sql::StringFunctions::Upper(ctx, result, *str);
}
// ---------------------------------------------------------------
// Index Iterator
// ---------------------------------------------------------------
void OpIndexIteratorConstruct(terrier::execution::sql::IndexIterator *iter, uint32_t table_oid, uint32_t index_oid,
                              terrier::execution::exec::ExecutionContext *exec_ctx);
void OpIndexIteratorFree(terrier::execution::sql::IndexIterator *iter);

void OpIndexIteratorPerformInit(terrier::execution::sql::IndexIterator *iter);

VM_OP_HOT void OpIndexIteratorAddCol(terrier::execution::sql::IndexIterator *iter, uint32_t col_oid) {
  iter->AddCol(col_oid);
}

VM_OP_HOT void OpIndexIteratorScanKey(terrier::execution::sql::IndexIterator *iter) { iter->ScanKey(); }

VM_OP_HOT void OpIndexIteratorAdvance(bool *has_more, terrier::execution::sql::IndexIterator *iter) {
  *has_more = iter->Advance();
}

VM_OP_HOT void OpIndexIteratorGetTinyInt(terrier::execution::sql::Integer *out,
                                         terrier::execution::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetSmallInt(terrier::execution::sql::Integer *out,
                                          terrier::execution::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetInteger(terrier::execution::sql::Integer *out,
                                         terrier::execution::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetBigInt(terrier::execution::sql::Integer *out,
                                        terrier::execution::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetReal(terrier::execution::sql::Real *out, terrier::execution::sql::IndexIterator *iter,
                                      u16 col_idx) {
  // Read
  auto *ptr = iter->Get<f32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDouble(terrier::execution::sql::Real *out,
                                        terrier::execution::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<f64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDecimal(terrier::execution::sql::Decimal *out,
                                         UNUSED terrier::execution::sql::IndexIterator *iter, UNUSED u16 col_idx) {
  // Set
  out->is_null = false;
  out->val = 0;
}

VM_OP_HOT void OpIndexIteratorGetTinyIntNull(terrier::execution::sql::Integer *out,
                                             terrier::execution::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i8, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetSmallIntNull(terrier::execution::sql::Integer *out,
                                              terrier::execution::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i16, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetIntegerNull(terrier::execution::sql::Integer *out,
                                             terrier::execution::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i32, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetBigIntNull(terrier::execution::sql::Integer *out,
                                            terrier::execution::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i64, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetRealNull(terrier::execution::sql::Real *out,
                                          terrier::execution::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f32, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDoubleNull(terrier::execution::sql::Real *out,
                                            terrier::execution::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f64, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDecimalNull(terrier::execution::sql::Decimal *out,
                                             terrier::execution::sql::IndexIterator *iter, u16 col_idx) {
  out->val = 0;
  out->is_null = false;
}

VM_OP_HOT void OpIndexIteratorSetKeyTinyInt(terrier::execution::sql::IndexIterator *iter, u16 col_idx,
                                            terrier::execution::sql::Integer *val) {
  iter->SetKey(col_idx, static_cast<i8>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeySmallInt(terrier::execution::sql::IndexIterator *iter, u16 col_idx,
                                             terrier::execution::sql::Integer *val) {
  iter->SetKey(col_idx, static_cast<i16>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeyInt(terrier::execution::sql::IndexIterator *iter, u16 col_idx,
                                        terrier::execution::sql::Integer *val) {
  iter->SetKey(col_idx, static_cast<i32>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeyBigInt(terrier::execution::sql::IndexIterator *iter, u16 col_idx,
                                           terrier::execution::sql::Integer *val) {
  iter->SetKey(col_idx, static_cast<i64>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeyReal(terrier::execution::sql::IndexIterator *iter, u16 col_idx,
                                         terrier::execution::sql::Real *val) {
  iter->SetKey(col_idx, static_cast<f32>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeyDouble(terrier::execution::sql::IndexIterator *iter, u16 col_idx,
                                           terrier::execution::sql::Real *val) {
  iter->SetKey(col_idx, static_cast<f64>(val->val), val->is_null);
}

// ---------------------------------------------------------------
// Insert Calls
// ---------------------------------------------------------------

void OpInsert(terrier::execution::exec::ExecutionContext *exec_ctx, u32 table_oid, byte *values_ptr);

// ---------------------------------------------------------------
// Output Calls
// ---------------------------------------------------------------

void OpOutputAlloc(terrier::execution::exec::ExecutionContext *exec_ctx, byte **result);

void OpOutputAdvance(terrier::execution::exec::ExecutionContext *exec_ctx);

void OpOutputSetNull(terrier::execution::exec::ExecutionContext *exec_ctx, u32 idx);

void OpOutputFinalize(terrier::execution::exec::ExecutionContext *exec_ctx);

}  // extern "C"
