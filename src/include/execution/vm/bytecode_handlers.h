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

VM_OP_HOT void OpExecutionContextGetMemoryPool(terrier::sql::MemoryPool **const memory,
                                               terrier::exec::ExecutionContext *const exec_ctx) {
  *memory = exec_ctx->GetMemoryPool();
}

void OpThreadStateContainerInit(terrier::sql::ThreadStateContainer *thread_state_container, terrier::sql::MemoryPool *memory);

VM_OP_HOT void OpThreadStateContainerReset(terrier::sql::ThreadStateContainer *thread_state_container, u32 size,
                                           terrier::sql::ThreadStateContainer::InitFn init_fn,
                                           terrier::sql::ThreadStateContainer::DestroyFn destroy_fn, void *ctx) {
  thread_state_container->Reset(size, init_fn, destroy_fn, ctx);
}

VM_OP_HOT void OpThreadStateContainerIterate(terrier::sql::ThreadStateContainer *thread_state_container, void *const state,
                                             terrier::sql::ThreadStateContainer::IterateFn iterate_fn) {
  thread_state_container->IterateStates(state, iterate_fn);
}

void OpThreadStateContainerFree(terrier::sql::ThreadStateContainer *thread_state_container);

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorConstruct(terrier::sql::TableVectorIterator *iter, u32 table_oid,
                                    terrier::exec::ExecutionContext *exec_ctx);

VM_OP_HOT void OpTableVectorIteratorAddCol(terrier::sql::TableVectorIterator *iter, u32 col_oid) { iter->AddCol(col_oid); }

void OpTableVectorIteratorPerformInit(terrier::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorNext(bool *has_more, terrier::sql::TableVectorIterator *iter) {
  *has_more = iter->Advance();
}

void OpTableVectorIteratorFree(terrier::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorGetPCI(terrier::sql::ProjectedColumnsIterator **pci,
                                           terrier::sql::TableVectorIterator *iter) {
  *pci = iter->projected_columns_iterator();
}

VM_OP_HOT void OpParallelScanTable(const u32 db_oid, const u32 table_oid, void *const query_state,
                                   terrier::sql::ThreadStateContainer *const thread_states,
                                   const terrier::sql::TableVectorIterator::ScanFn scanner) {
  terrier::sql::TableVectorIterator::ParallelScan(db_oid, table_oid, query_state, thread_states, scanner);
}

VM_OP_HOT void OpPCIIsFiltered(bool *is_filtered, terrier::sql::ProjectedColumnsIterator *pci) {
  *is_filtered = pci->IsFiltered();
}

VM_OP_HOT void OpPCIHasNext(bool *has_more, terrier::sql::ProjectedColumnsIterator *pci) { *has_more = pci->HasNext(); }

VM_OP_HOT void OpPCIHasNextFiltered(bool *has_more, terrier::sql::ProjectedColumnsIterator *pci) {
  *has_more = pci->HasNextFiltered();
}

VM_OP_HOT void OpPCIAdvance(terrier::sql::ProjectedColumnsIterator *pci) { pci->Advance(); }

VM_OP_HOT void OpPCIAdvanceFiltered(terrier::sql::ProjectedColumnsIterator *pci) { pci->AdvanceFiltered(); }

VM_OP_HOT void OpPCIMatch(terrier::sql::ProjectedColumnsIterator *pci, bool match) { pci->Match(match); }

VM_OP_HOT void OpPCIReset(terrier::sql::ProjectedColumnsIterator *pci) { pci->Reset(); }

VM_OP_HOT void OpPCIResetFiltered(terrier::sql::ProjectedColumnsIterator *pci) { pci->ResetFiltered(); }

VM_OP_HOT void OpPCIGetTinyInt(terrier::sql::Integer *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i8, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetSmallInt(terrier::sql::Integer *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetInteger(terrier::sql::Integer *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetBigInt(terrier::sql::Integer *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetReal(terrier::sql::Real *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<f32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read real value");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDouble(terrier::sql::Real *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<f64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read double value");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDecimal(terrier::sql::Decimal *out, UNUSED terrier::sql::ProjectedColumnsIterator *iter,
                               UNUSED u16 col_idx) {
  // TODO(Amadou): Implement once the representation of Decimal is settled upon.
  // The sql::Decimal class does not seem to match the storage layer's DECIMAL type as it needs a precision and
  // a scale.
  out->is_null = false;
  out->val = 0;
}

VM_OP_HOT void OpPCIGetDate(terrier::sql::Date *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<u32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read date");

  // Set
  out->is_null = false;
  out->int_val = *ptr;
}

VM_OP_HOT void OpPCIGetVarlen(terrier::sql::StringVal *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *varlen = iter->Get<terrier::storage::VarlenEntry, false>(col_idx, nullptr);
  TPL_ASSERT(varlen != nullptr, "Null pointer when trying to read varlen");

  // Set
  *out = terrier::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
}

VM_OP_HOT void OpPCIGetTinyIntNull(terrier::sql::Integer *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i8, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetSmallIntNull(terrier::sql::Integer *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i16, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetIntegerNull(terrier::sql::Integer *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetBigIntNull(terrier::sql::Integer *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i64, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetRealNull(terrier::sql::Real *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read real value");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDoubleNull(terrier::sql::Real *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f64, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read double value");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDecimalNull(terrier::sql::Decimal *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  out->val = 0;
  out->is_null = false;
}

VM_OP_HOT void OpPCIGetDateNull(terrier::sql::Date *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<u32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read date");

  // Set
  out->is_null = null;
  out->int_val = *ptr;
}

VM_OP_HOT void OpPCIGetVarlenNull(terrier::sql::StringVal *out, terrier::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *varlen = iter->Get<terrier::storage::VarlenEntry, true>(col_idx, &null);
  TPL_ASSERT(varlen != nullptr, "Null pointer when trying to read varlen");

  // Set
  if (null) {
    out->is_null = null;
  } else {
    *out = terrier::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
  }
}

void OpPCIFilterEqual(u64 *size, terrier::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val);

void OpPCIFilterGreaterThan(u64 *size, terrier::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val);

void OpPCIFilterGreaterThanEqual(u64 *size, terrier::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val);

void OpPCIFilterLessThan(u64 *size, terrier::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val);

void OpPCIFilterLessThanEqual(u64 *size, terrier::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val);

void OpPCIFilterNotEqual(u64 *size, terrier::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val);

// ---------------------------------------------------------
// Hashing
// ---------------------------------------------------------

VM_OP_HOT void OpHashInt(hash_t *hash_val, terrier::sql::Integer *input) {
  *hash_val = terrier::util::Hasher::Hash<terrier::util::HashMethod::Crc>(input->val);
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashReal(hash_t *hash_val, terrier::sql::Real *input) {
  *hash_val = terrier::util::Hasher::Hash<terrier::util::HashMethod::Crc>(input->val);
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashString(hash_t *hash_val, terrier::sql::StringVal *input) {
  *hash_val = terrier::util::Hasher::Hash<terrier::util::HashMethod::xxHash3>(reinterpret_cast<const u8 *>(input->Content()),
                                                                      input->len);
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashCombine(hash_t *hash_val, hash_t new_hash_val) {
  *hash_val = terrier::util::Hasher::CombineHashes(*hash_val, new_hash_val);
}

// ---------------------------------------------------------
// Filter Manager
// ---------------------------------------------------------

void OpFilterManagerInit(terrier::sql::FilterManager *filter_manager);

void OpFilterManagerStartNewClause(terrier::sql::FilterManager *filter_manager);

void OpFilterManagerInsertFlavor(terrier::sql::FilterManager *filter_manager, terrier::sql::FilterManager::MatchFn flavor);

void OpFilterManagerFinalize(terrier::sql::FilterManager *filter_manager);

void OpFilterManagerRunFilters(terrier::sql::FilterManager *filter_manager, terrier::sql::ProjectedColumnsIterator *pci);

void OpFilterManagerFree(terrier::sql::FilterManager *filter_manager);

// ---------------------------------------------------------
// Scalar SQL comparisons
// ---------------------------------------------------------

VM_OP_HOT void OpForceBoolTruth(bool *result, terrier::sql::BoolVal *input) { *result = input->ForceTruth(); }

VM_OP_HOT void OpInitBool(terrier::sql::BoolVal *result, bool input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpInitInteger(terrier::sql::Integer *result, i32 input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpInitReal(terrier::sql::Real *result, double input) {
  result->is_null = false;
  result->val = input;
}

VM_OP_HOT void OpInitDate(terrier::sql::Date *result, i16 year, u8 month, u8 day) {
  result->is_null = false;
  result->ymd = date::year(year) / month / day;
}

VM_OP_HOT void OpInitString(terrier::sql::StringVal *result, u64 length, uintptr_t data) {
  *result = terrier::sql::StringVal(reinterpret_cast<char *>(data), static_cast<u32>(length));
}

VM_OP_HOT void OpInitVarlen(terrier::sql::StringVal *result, uintptr_t data) {
  auto *varlen = reinterpret_cast<terrier::storage::VarlenEntry *>(data);
  *result = terrier::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
}

#define GEN_SQL_COMPARISONS(TYPE)                                                                            \
  VM_OP_HOT void OpGreaterThan##TYPE(terrier::sql::BoolVal *const result, const terrier::sql::TYPE *const left,      \
                                     const terrier::sql::TYPE *const right) {                                    \
    terrier::sql::ComparisonFunctions::Gt##TYPE(result, *left, *right);                                          \
  }                                                                                                          \
  VM_OP_HOT void OpGreaterThanEqual##TYPE(terrier::sql::BoolVal *const result, const terrier::sql::TYPE *const left, \
                                          const terrier::sql::TYPE *const right) {                               \
    terrier::sql::ComparisonFunctions::Ge##TYPE(result, *left, *right);                                          \
  }                                                                                                          \
  VM_OP_HOT void OpEqual##TYPE(terrier::sql::BoolVal *const result, const terrier::sql::TYPE *const left,            \
                               const terrier::sql::TYPE *const right) {                                          \
    terrier::sql::ComparisonFunctions::Eq##TYPE(result, *left, *right);                                          \
  }                                                                                                          \
  VM_OP_HOT void OpLessThan##TYPE(terrier::sql::BoolVal *const result, const terrier::sql::TYPE *const left,         \
                                  const terrier::sql::TYPE *const right) {                                       \
    terrier::sql::ComparisonFunctions::Lt##TYPE(result, *left, *right);                                          \
  }                                                                                                          \
  VM_OP_HOT void OpLessThanEqual##TYPE(terrier::sql::BoolVal *const result, const terrier::sql::TYPE *const left,    \
                                       const terrier::sql::TYPE *const right) {                                  \
    terrier::sql::ComparisonFunctions::Le##TYPE(result, *left, *right);                                          \
  }                                                                                                          \
  VM_OP_HOT void OpNotEqual##TYPE(terrier::sql::BoolVal *const result, const terrier::sql::TYPE *const left,         \
                                  const terrier::sql::TYPE *const right) {                                       \
    terrier::sql::ComparisonFunctions::Ne##TYPE(result, *left, *right);                                          \
  }

GEN_SQL_COMPARISONS(Integer)
GEN_SQL_COMPARISONS(Real)
GEN_SQL_COMPARISONS(StringVal)
GEN_SQL_COMPARISONS(Date)
#undef GEN_SQL_COMPARISONS

// ----------------------------------
// SQL arithmetic
// ---------------------------------
VM_OP_WARM void OpAbsInteger(terrier::sql::Integer *const result, const terrier::sql::Integer *const left) {
  terrier::sql::ArithmeticFunctions::Abs(result, *left);
}

VM_OP_WARM void OpAbsReal(terrier::sql::Real *const result, const terrier::sql::Real *const left) {
  terrier::sql::ArithmeticFunctions::Abs(result, *left);
}

VM_OP_HOT void OpAddInteger(terrier::sql::Integer *const result, const terrier::sql::Integer *const left,
                            const terrier::sql::Integer *const right) {
  UNUSED bool overflow;
  terrier::sql::ArithmeticFunctions::Add(result, *left, *right, &overflow);
}

VM_OP_HOT void OpSubInteger(terrier::sql::Integer *const result, const terrier::sql::Integer *const left,
                            const terrier::sql::Integer *const right) {
  UNUSED bool overflow;
  terrier::sql::ArithmeticFunctions::Sub(result, *left, *right, &overflow);
}

VM_OP_HOT void OpMulInteger(terrier::sql::Integer *const result, const terrier::sql::Integer *const left,
                            const terrier::sql::Integer *const right) {
  UNUSED bool overflow;
  terrier::sql::ArithmeticFunctions::Mul(result, *left, *right, &overflow);
}

VM_OP_HOT void OpDivInteger(terrier::sql::Integer *const result, const terrier::sql::Integer *const left,
                            const terrier::sql::Integer *const right) {
  UNUSED bool div_by_zero = false;
  terrier::sql::ArithmeticFunctions::IntDiv(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpRemInteger(terrier::sql::Integer *const result, const terrier::sql::Integer *const left,
                            const terrier::sql::Integer *const right) {
  UNUSED bool div_by_zero = false;
  terrier::sql::ArithmeticFunctions::IntMod(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpAddReal(terrier::sql::Real *const result, const terrier::sql::Real *const left,
                         const terrier::sql::Real *const right) {
  terrier::sql::ArithmeticFunctions::Add(result, *left, *right);
}

VM_OP_HOT void OpSubReal(terrier::sql::Real *const result, const terrier::sql::Real *const left,
                         const terrier::sql::Real *const right) {
  terrier::sql::ArithmeticFunctions::Sub(result, *left, *right);
}

VM_OP_HOT void OpMulReal(terrier::sql::Real *const result, const terrier::sql::Real *const left,
                         const terrier::sql::Real *const right) {
  terrier::sql::ArithmeticFunctions::Mul(result, *left, *right);
}

VM_OP_HOT void OpDivReal(terrier::sql::Real *const result, const terrier::sql::Real *const left,
                         const terrier::sql::Real *const right) {
  UNUSED bool div_by_zero = false;
  terrier::sql::ArithmeticFunctions::Div(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpRemReal(terrier::sql::Real *const result, const terrier::sql::Real *const left,
                         const terrier::sql::Real *const right) {
  UNUSED bool div_by_zero = false;
  terrier::sql::ArithmeticFunctions::Mod(result, *left, *right, &div_by_zero);
}

// ---------------------------------------------------------
// SQL Aggregations
// ---------------------------------------------------------

void OpAggregationHashTableInit(terrier::sql::AggregationHashTable *agg_hash_table, terrier::sql::MemoryPool *memory,
                                u32 payload_size);

VM_OP_HOT void OpAggregationHashTableInsert(byte **result, terrier::sql::AggregationHashTable *agg_hash_table,
                                            hash_t hash_val) {
  *result = agg_hash_table->Insert(hash_val);
}

VM_OP_HOT void OpAggregationHashTableLookup(byte **result, terrier::sql::AggregationHashTable *const agg_hash_table,
                                            const hash_t hash_val,
                                            const terrier::sql::AggregationHashTable::KeyEqFn key_eq_fn, void *iters[]) {
  *result = agg_hash_table->Lookup(hash_val, key_eq_fn, iters);
}

VM_OP_HOT void OpAggregationHashTableProcessBatch(terrier::sql::AggregationHashTable *const agg_hash_table,
                                                  terrier::sql::ProjectedColumnsIterator *iters[],
                                                  const terrier::sql::AggregationHashTable::HashFn hash_fn,
                                                  const terrier::sql::AggregationHashTable::KeyEqFn key_eq_fn,
                                                  const terrier::sql::AggregationHashTable::InitAggFn init_agg_fn,
                                                  const terrier::sql::AggregationHashTable::AdvanceAggFn merge_agg_fn) {
  agg_hash_table->ProcessBatch(iters, hash_fn, key_eq_fn, init_agg_fn, merge_agg_fn);
}

VM_OP_HOT void OpAggregationHashTableTransferPartitions(
    terrier::sql::AggregationHashTable *const agg_hash_table, terrier::sql::ThreadStateContainer *const thread_state_container,
    const u32 agg_ht_offset, const terrier::sql::AggregationHashTable::MergePartitionFn merge_partition_fn) {
  agg_hash_table->TransferMemoryAndPartitions(thread_state_container, agg_ht_offset, merge_partition_fn);
}

VM_OP_HOT void OpAggregationHashTableParallelPartitionedScan(
    terrier::sql::AggregationHashTable *const agg_hash_table, void *const query_state,
    terrier::sql::ThreadStateContainer *const thread_state_container,
    const terrier::sql::AggregationHashTable::ScanPartitionFn scan_partition_fn) {
  agg_hash_table->ExecuteParallelPartitionedScan(query_state, thread_state_container, scan_partition_fn);
}

void OpAggregationHashTableFree(terrier::sql::AggregationHashTable *agg_hash_table);

void OpAggregationHashTableIteratorInit(terrier::sql::AggregationHashTableIterator *iter,
                                        terrier::sql::AggregationHashTable *agg_hash_table);

VM_OP_HOT void OpAggregationHashTableIteratorHasNext(bool *has_more, terrier::sql::AggregationHashTableIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationHashTableIteratorNext(terrier::sql::AggregationHashTableIterator *iter) { iter->Next(); }

VM_OP_HOT void OpAggregationHashTableIteratorGetRow(const byte **row, terrier::sql::AggregationHashTableIterator *iter) {
  *row = iter->GetCurrentAggregateRow();
}

void OpAggregationHashTableIteratorFree(terrier::sql::AggregationHashTableIterator *iter);

VM_OP_HOT void OpAggregationOverflowPartitionIteratorHasNext(bool *has_more,
                                                             terrier::sql::AggregationOverflowPartitionIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorNext(terrier::sql::AggregationOverflowPartitionIterator *iter) {
  iter->Next();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetHash(hash_t *hash_val,
                                                             terrier::sql::AggregationOverflowPartitionIterator *iter) {
  *hash_val = iter->GetHash();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetRow(const byte **row,
                                                            terrier::sql::AggregationOverflowPartitionIterator *iter) {
  *row = iter->GetPayload();
}

//
// COUNT
//

VM_OP_HOT void OpCountAggregateInit(terrier::sql::CountAggregate *agg) { new (agg) terrier::sql::CountAggregate(); }

VM_OP_HOT void OpCountAggregateAdvance(terrier::sql::CountAggregate *agg, terrier::sql::Val *val) { agg->Advance(*val); }

VM_OP_HOT void OpCountAggregateMerge(terrier::sql::CountAggregate *agg_1, terrier::sql::CountAggregate *agg_2) {
  TPL_ASSERT(agg_2 != nullptr, "Null aggregate!");
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountAggregateReset(terrier::sql::CountAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpCountAggregateGetResult(terrier::sql::Integer *result, terrier::sql::CountAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountAggregateFree(terrier::sql::CountAggregate *agg) { agg->~CountAggregate(); }

//
// COUNT(*)
//

VM_OP_HOT void OpCountStarAggregateInit(terrier::sql::CountStarAggregate *agg) { new (agg) terrier::sql::CountStarAggregate(); }

VM_OP_HOT void OpCountStarAggregateAdvance(terrier::sql::CountStarAggregate *agg, const terrier::sql::Val *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpCountStarAggregateMerge(terrier::sql::CountStarAggregate *agg_1,
                                         const terrier::sql::CountStarAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountStarAggregateReset(terrier::sql::CountStarAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpCountStarAggregateGetResult(terrier::sql::Integer *result, const terrier::sql::CountStarAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountStarAggregateFree(terrier::sql::CountStarAggregate *agg) { agg->~CountStarAggregate(); }

//
// SUM(int_type)
//

VM_OP_HOT void OpIntegerSumAggregateInit(terrier::sql::IntegerSumAggregate *agg) {
  new (agg) terrier::sql::IntegerSumAggregate();
}

VM_OP_HOT void OpIntegerSumAggregateAdvance(terrier::sql::IntegerSumAggregate *agg, const terrier::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerSumAggregateMerge(terrier::sql::IntegerSumAggregate *agg_1,
                                          const terrier::sql::IntegerSumAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerSumAggregateReset(terrier::sql::IntegerSumAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerSumAggregateGetResult(terrier::sql::Integer *result, const terrier::sql::IntegerSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpIntegerSumAggregateFree(terrier::sql::IntegerSumAggregate *agg) { agg->~IntegerSumAggregate(); }

//
// MAX(int_type)
//

VM_OP_HOT void OpIntegerMaxAggregateInit(terrier::sql::IntegerMaxAggregate *agg) {
  new (agg) terrier::sql::IntegerMaxAggregate();
}

VM_OP_HOT void OpIntegerMaxAggregateAdvance(terrier::sql::IntegerMaxAggregate *agg, const terrier::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerMaxAggregateMerge(terrier::sql::IntegerMaxAggregate *agg_1,
                                          const terrier::sql::IntegerMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMaxAggregateReset(terrier::sql::IntegerMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerMaxAggregateGetResult(terrier::sql::Integer *result, const terrier::sql::IntegerMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpIntegerMaxAggregateFree(terrier::sql::IntegerMaxAggregate *agg) { agg->~IntegerMaxAggregate(); }

//
// MIN(int_type)
//

VM_OP_HOT void OpIntegerMinAggregateInit(terrier::sql::IntegerMinAggregate *agg) {
  new (agg) terrier::sql::IntegerMinAggregate();
}

VM_OP_HOT void OpIntegerMinAggregateAdvance(terrier::sql::IntegerMinAggregate *agg, const terrier::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerMinAggregateMerge(terrier::sql::IntegerMinAggregate *agg_1,
                                          const terrier::sql::IntegerMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMinAggregateReset(terrier::sql::IntegerMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerMinAggregateGetResult(terrier::sql::Integer *result, const terrier::sql::IntegerMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpIntegerMinAggregateFree(terrier::sql::IntegerMinAggregate *agg) { agg->~IntegerMinAggregate(); }

//
// SUM(real)
//

VM_OP_HOT void OpRealSumAggregateInit(terrier::sql::RealSumAggregate *agg) { new (agg) terrier::sql::RealSumAggregate(); }

VM_OP_HOT void OpRealSumAggregateAdvance(terrier::sql::RealSumAggregate *agg, const terrier::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealSumAggregateMerge(terrier::sql::RealSumAggregate *agg_1, const terrier::sql::RealSumAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealSumAggregateReset(terrier::sql::RealSumAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealSumAggregateGetResult(terrier::sql::Real *result, const terrier::sql::RealSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpRealSumAggregateFree(terrier::sql::RealSumAggregate *agg) { agg->~RealSumAggregate(); }

//
// MAX(real_type)
//

VM_OP_HOT void OpRealMaxAggregateInit(terrier::sql::RealMaxAggregate *agg) { new (agg) terrier::sql::RealMaxAggregate(); }

VM_OP_HOT void OpRealMaxAggregateAdvance(terrier::sql::RealMaxAggregate *agg, const terrier::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealMaxAggregateMerge(terrier::sql::RealMaxAggregate *agg_1, const terrier::sql::RealMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealMaxAggregateReset(terrier::sql::RealMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealMaxAggregateGetResult(terrier::sql::Real *result, const terrier::sql::RealMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpRealMaxAggregateFree(terrier::sql::RealMaxAggregate *agg) { agg->~RealMaxAggregate(); }

//
// MIN(real_type)
//

VM_OP_HOT void OpRealMinAggregateInit(terrier::sql::RealMinAggregate *agg) { new (agg) terrier::sql::RealMinAggregate(); }

VM_OP_HOT void OpRealMinAggregateAdvance(terrier::sql::RealMinAggregate *agg, const terrier::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealMinAggregateMerge(terrier::sql::RealMinAggregate *agg_1, const terrier::sql::RealMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealMinAggregateReset(terrier::sql::RealMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealMinAggregateGetResult(terrier::sql::Real *result, const terrier::sql::RealMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpRealMinAggregateFree(terrier::sql::RealMinAggregate *agg) { agg->~RealMinAggregate(); }

//
// AVG
//

VM_OP_HOT void OpAvgAggregateInit(terrier::sql::AvgAggregate *agg) { new (agg) terrier::sql::AvgAggregate(); }

VM_OP_HOT void OpIntegerAvgAggregateAdvance(terrier::sql::AvgAggregate *agg, const terrier::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealAvgAggregateAdvance(terrier::sql::AvgAggregate *agg, const terrier::sql::Real *val) { agg->Advance(*val); }

VM_OP_HOT void OpAvgAggregateMerge(terrier::sql::AvgAggregate *agg_1, const terrier::sql::AvgAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpAvgAggregateReset(terrier::sql::AvgAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpAvgAggregateGetResult(terrier::sql::Real *result, const terrier::sql::AvgAggregate *agg) {
  *result = agg->GetResultAvg();
}

VM_OP_HOT void OpAvgAggregateFree(terrier::sql::AvgAggregate *agg) { agg->~AvgAggregate(); }
// ---------------------------------------------------------
// Hash Joins
// ---------------------------------------------------------

void OpJoinHashTableInit(terrier::sql::JoinHashTable *join_hash_table, terrier::sql::MemoryPool *memory, u32 tuple_size);

VM_OP_HOT void OpJoinHashTableAllocTuple(byte **result, terrier::sql::JoinHashTable *join_hash_table, hash_t hash) {
  *result = join_hash_table->AllocInputTuple(hash);
}

void OpJoinHashTableBuild(terrier::sql::JoinHashTable *join_hash_table);

void OpJoinHashTableBuildParallel(terrier::sql::JoinHashTable *join_hash_table,
                                  terrier::sql::ThreadStateContainer *thread_state_container, u32 jht_offset);

VM_OP_HOT void OpJoinHashTableIterInit(terrier::sql::JoinHashTableIterator *result,
                                       terrier::sql::JoinHashTable *join_hash_table, hash_t hash) {
  *result = join_hash_table->Lookup<false>(hash);
}

VM_OP_HOT void OpJoinHashTableIterHasNext(bool *has_more, terrier::sql::JoinHashTableIterator *iterator,
                                          terrier::sql::JoinHashTableIterator::KeyEq key_eq, void *opaque_ctx,
                                          void *probe_tuple) {
  *has_more = iterator->HasNext(key_eq, opaque_ctx, probe_tuple);
}

VM_OP_HOT void OpJoinHashTableIterGetRow(const byte **result, terrier::sql::JoinHashTableIterator *iterator) {
  *result = iterator->NextMatch()->payload;
}

VM_OP_HOT void OpJoinHashTableIterClose(terrier::sql::JoinHashTableIterator *iterator) {
  iterator->~JoinHashTableIterator();
}

void OpJoinHashTableFree(terrier::sql::JoinHashTable *join_hash_table);

// ---------------------------------------------------------
// Sorting
// ---------------------------------------------------------

void OpSorterInit(terrier::sql::Sorter *sorter, terrier::sql::MemoryPool *memory, terrier::sql::Sorter::ComparisonFunction cmp_fn,
                  u32 tuple_size);

VM_OP_HOT void OpSorterAllocTuple(byte **result, terrier::sql::Sorter *sorter) { *result = sorter->AllocInputTuple(); }

VM_OP_HOT void OpSorterAllocTupleTopK(byte **result, terrier::sql::Sorter *sorter, u64 top_k) {
  *result = sorter->AllocInputTupleTopK(top_k);
}

VM_OP_HOT void OpSorterAllocTupleTopKFinish(terrier::sql::Sorter *sorter, u64 top_k) {
  sorter->AllocInputTupleTopKFinish(top_k);
}

void OpSorterSort(terrier::sql::Sorter *sorter);

void OpSorterSortParallel(terrier::sql::Sorter *sorter, terrier::sql::ThreadStateContainer *thread_state_container,
                          u32 sorter_offset);

void OpSorterSortTopKParallel(terrier::sql::Sorter *sorter, terrier::sql::ThreadStateContainer *thread_state_container,
                              u32 sorter_offset, u64 top_k);

void OpSorterFree(terrier::sql::Sorter *sorter);

void OpSorterIteratorInit(terrier::sql::SorterIterator *iter, terrier::sql::Sorter *sorter);

VM_OP_HOT void OpSorterIteratorHasNext(bool *has_more, terrier::sql::SorterIterator *iter) { *has_more = iter->HasNext(); }

VM_OP_HOT void OpSorterIteratorNext(terrier::sql::SorterIterator *iter) { iter->Next(); }

VM_OP_HOT void OpSorterIteratorGetRow(const byte **row, terrier::sql::SorterIterator *iter) { *row = iter->GetRow(); }

void OpSorterIteratorFree(terrier::sql::SorterIterator *iter);

// ---------------------------------------------------------
// Trig functions
// ---------------------------------------------------------

VM_OP_WARM void OpPi(terrier::sql::Real *result) { terrier::sql::ArithmeticFunctions::Pi(result); }

VM_OP_WARM void OpE(terrier::sql::Real *result) { terrier::sql::ArithmeticFunctions::E(result); }

VM_OP_WARM void OpAcos(terrier::sql::Real *result, const terrier::sql::Real *input) {
  terrier::sql::ArithmeticFunctions::Acos(result, *input);
}

VM_OP_WARM void OpAsin(terrier::sql::Real *result, const terrier::sql::Real *input) {
  terrier::sql::ArithmeticFunctions::Asin(result, *input);
}

VM_OP_WARM void OpAtan(terrier::sql::Real *result, const terrier::sql::Real *input) {
  terrier::sql::ArithmeticFunctions::Atan(result, *input);
}

VM_OP_WARM void OpAtan2(terrier::sql::Real *result, const terrier::sql::Real *arg_1, const terrier::sql::Real *arg_2) {
  terrier::sql::ArithmeticFunctions::Atan2(result, *arg_1, *arg_2);
}

VM_OP_WARM void OpCos(terrier::sql::Real *result, const terrier::sql::Real *input) {
  terrier::sql::ArithmeticFunctions::Cos(result, *input);
}

VM_OP_WARM void OpCot(terrier::sql::Real *result, const terrier::sql::Real *input) {
  terrier::sql::ArithmeticFunctions::Cot(result, *input);
}

VM_OP_WARM void OpSin(terrier::sql::Real *result, const terrier::sql::Real *input) {
  terrier::sql::ArithmeticFunctions::Sin(result, *input);
}

VM_OP_WARM void OpTan(terrier::sql::Real *result, const terrier::sql::Real *input) {
  terrier::sql::ArithmeticFunctions::Tan(result, *input);
}

VM_OP_WARM void OpCosh(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Cosh(result, *v);
}

VM_OP_WARM void OpTanh(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Tanh(result, *v);
}

VM_OP_WARM void OpSinh(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Sinh(result, *v);
}

VM_OP_WARM void OpSqrt(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Sqrt(result, *v);
}

VM_OP_WARM void OpCbrt(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Cbrt(result, *v);
}

VM_OP_WARM void OpExp(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Exp(result, *v);
}

VM_OP_WARM void OpCeil(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Ceil(result, *v);
}

VM_OP_WARM void OpFloor(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Floor(result, *v);
}

VM_OP_WARM void OpTruncate(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Truncate(result, *v);
}

VM_OP_WARM void OpLn(terrier::sql::Real *result, const terrier::sql::Real *v) { terrier::sql::ArithmeticFunctions::Ln(result, *v); }

VM_OP_WARM void OpLog2(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Log2(result, *v);
}

VM_OP_WARM void OpLog10(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Log10(result, *v);
}

VM_OP_WARM void OpSign(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Sign(result, *v);
}

VM_OP_WARM void OpRadians(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Radians(result, *v);
}

VM_OP_WARM void OpDegrees(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Degrees(result, *v);
}

VM_OP_WARM void OpRound(terrier::sql::Real *result, const terrier::sql::Real *v) {
  terrier::sql::ArithmeticFunctions::Round(result, *v);
}

VM_OP_WARM void OpRoundUpTo(terrier::sql::Real *result, const terrier::sql::Real *v, const terrier::sql::Integer *scale) {
  terrier::sql::ArithmeticFunctions::RoundUpTo(result, *v, *scale);
}

VM_OP_WARM void OpLog(terrier::sql::Real *result, const terrier::sql::Real *base, const terrier::sql::Real *val) {
  terrier::sql::ArithmeticFunctions::Log(result, *base, *val);
}

VM_OP_WARM void OpPow(terrier::sql::Real *result, const terrier::sql::Real *base, const terrier::sql::Real *val) {
  terrier::sql::ArithmeticFunctions::Pow(result, *base, *val);
}

// ---------------------------------------------------------
// Null/Not Null predicates
// ---------------------------------------------------------

VM_OP_WARM void OpValIsNull(terrier::sql::BoolVal *result, const terrier::sql::Val *val) {
  terrier::sql::IsNullPredicate::IsNull(result, *val);
}

VM_OP_WARM void OpValIsNotNull(terrier::sql::BoolVal *result, const terrier::sql::Val *val) {
  terrier::sql::IsNullPredicate::IsNotNull(result, *val);
}

// ---------------------------------------------------------
// String functions
// ---------------------------------------------------------

VM_OP_WARM void OpCharLength(terrier::exec::ExecutionContext *ctx, terrier::sql::Integer *result,
                             const terrier::sql::StringVal *str) {
  terrier::sql::StringFunctions::CharLength(ctx, result, *str);
}

VM_OP_WARM void OpLeft(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result, const terrier::sql::StringVal *str,
                       const terrier::sql::Integer *n) {
  terrier::sql::StringFunctions::Left(ctx, result, *str, *n);
}

VM_OP_WARM void OpLength(terrier::exec::ExecutionContext *ctx, terrier::sql::Integer *result, const terrier::sql::StringVal *str) {
  terrier::sql::StringFunctions::Length(ctx, result, *str);
}

VM_OP_WARM void OpLower(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result, const terrier::sql::StringVal *str) {
  terrier::sql::StringFunctions::Lower(ctx, result, *str);
}

VM_OP_WARM void OpLPad(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result, const terrier::sql::StringVal *str,
                       const terrier::sql::Integer *len, const terrier::sql::StringVal *pad) {
  terrier::sql::StringFunctions::Lpad(ctx, result, *str, *len, *pad);
}

VM_OP_WARM void OpLTrim(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result, const terrier::sql::StringVal *str,
                        const terrier::sql::StringVal *chars) {
  terrier::sql::StringFunctions::Ltrim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpRepeat(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result, const terrier::sql::StringVal *str,
                         const terrier::sql::Integer *n) {
  terrier::sql::StringFunctions::Repeat(ctx, result, *str, *n);
}

VM_OP_WARM void OpReverse(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result,
                          const terrier::sql::StringVal *str) {
  terrier::sql::StringFunctions::Reverse(ctx, result, *str);
}

VM_OP_WARM void OpRight(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result, const terrier::sql::StringVal *str,
                        const terrier::sql::Integer *n) {
  terrier::sql::StringFunctions::Right(ctx, result, *str, *n);
}

VM_OP_WARM void OpRPad(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result, const terrier::sql::StringVal *str,
                       const terrier::sql::Integer *n, const terrier::sql::StringVal *pad) {
  terrier::sql::StringFunctions::Rpad(ctx, result, *str, *n, *pad);
}

VM_OP_WARM void OpRTrim(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result, const terrier::sql::StringVal *str,
                        const terrier::sql::StringVal *chars) {
  terrier::sql::StringFunctions::Rtrim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpSplitPart(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result,
                            const terrier::sql::StringVal *str, const terrier::sql::StringVal *delim,
                            const terrier::sql::Integer *field) {
  terrier::sql::StringFunctions::SplitPart(ctx, result, *str, *delim, *field);
}

VM_OP_WARM void OpSubstring(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result,
                            const terrier::sql::StringVal *str, const terrier::sql::Integer *pos,
                            const terrier::sql::Integer *len) {
  terrier::sql::StringFunctions::Substring(ctx, result, *str, *pos, *len);
}

VM_OP_WARM void OpTrim(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result, const terrier::sql::StringVal *str,
                       const terrier::sql::StringVal *chars) {
  terrier::sql::StringFunctions::Trim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpUpper(terrier::exec::ExecutionContext *ctx, terrier::sql::StringVal *result, const terrier::sql::StringVal *str) {
  terrier::sql::StringFunctions::Upper(ctx, result, *str);
}
// ---------------------------------------------------------------
// Index Iterator
// ---------------------------------------------------------------
void OpIndexIteratorConstruct(terrier::sql::IndexIterator *iter, uint32_t table_oid, uint32_t index_oid,
                              terrier::exec::ExecutionContext *exec_ctx);
void OpIndexIteratorFree(terrier::sql::IndexIterator *iter);

void OpIndexIteratorPerformInit(terrier::sql::IndexIterator *iter);

VM_OP_HOT void OpIndexIteratorAddCol(terrier::sql::IndexIterator *iter, uint32_t col_oid) { iter->AddCol(col_oid); }

VM_OP_HOT void OpIndexIteratorScanKey(terrier::sql::IndexIterator *iter) { iter->ScanKey(); }

VM_OP_HOT void OpIndexIteratorAdvance(bool *has_more, terrier::sql::IndexIterator *iter) { *has_more = iter->Advance(); }

VM_OP_HOT void OpIndexIteratorGetTinyInt(terrier::sql::Integer *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetSmallInt(terrier::sql::Integer *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetInteger(terrier::sql::Integer *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetBigInt(terrier::sql::Integer *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetReal(terrier::sql::Real *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<f32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDouble(terrier::sql::Real *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<f64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDecimal(terrier::sql::Decimal *out, UNUSED terrier::sql::IndexIterator *iter,
                                         UNUSED u16 col_idx) {
  // Set
  out->is_null = false;
  out->val = 0;
}

VM_OP_HOT void OpIndexIteratorGetTinyIntNull(terrier::sql::Integer *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i8, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetSmallIntNull(terrier::sql::Integer *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i16, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetIntegerNull(terrier::sql::Integer *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i32, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetBigIntNull(terrier::sql::Integer *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i64, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetRealNull(terrier::sql::Real *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f32, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDoubleNull(terrier::sql::Real *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f64, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDecimalNull(terrier::sql::Decimal *out, terrier::sql::IndexIterator *iter, u16 col_idx) {
  out->val = 0;
  out->is_null = false;
}

VM_OP_HOT void OpIndexIteratorSetKeyTinyInt(terrier::sql::IndexIterator *iter, u16 col_idx, terrier::sql::Integer *val) {
  iter->SetKey(col_idx, static_cast<i8>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeySmallInt(terrier::sql::IndexIterator *iter, u16 col_idx, terrier::sql::Integer *val) {
  iter->SetKey(col_idx, static_cast<i16>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeyInt(terrier::sql::IndexIterator *iter, u16 col_idx, terrier::sql::Integer *val) {
  iter->SetKey(col_idx, static_cast<i32>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeyBigInt(terrier::sql::IndexIterator *iter, u16 col_idx, terrier::sql::Integer *val) {
  iter->SetKey(col_idx, static_cast<i64>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeyReal(terrier::sql::IndexIterator *iter, u16 col_idx, terrier::sql::Real *val) {
  iter->SetKey(col_idx, static_cast<f32>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeyDouble(terrier::sql::IndexIterator *iter, u16 col_idx, terrier::sql::Real *val) {
  iter->SetKey(col_idx, static_cast<f64>(val->val), val->is_null);
}

// ---------------------------------------------------------------
// Insert Calls
// ---------------------------------------------------------------

void OpInsert(terrier::exec::ExecutionContext *exec_ctx, u32 table_oid, byte *values_ptr);

// ---------------------------------------------------------------
// Output Calls
// ---------------------------------------------------------------

void OpOutputAlloc(terrier::exec::ExecutionContext *exec_ctx, byte **result);

void OpOutputAdvance(terrier::exec::ExecutionContext *exec_ctx);

void OpOutputSetNull(terrier::exec::ExecutionContext *exec_ctx, u32 idx);

void OpOutputFinalize(terrier::exec::ExecutionContext *exec_ctx);

}  // extern "C"
