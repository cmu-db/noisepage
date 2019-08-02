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

VM_OP_HOT void OpThreadStateContainerIterate(tpl::sql::ThreadStateContainer *thread_state_container, void *const state,
                                             tpl::sql::ThreadStateContainer::IterateFn iterate_fn) {
  thread_state_container->IterateStates(state, iterate_fn);
}

void OpThreadStateContainerFree(tpl::sql::ThreadStateContainer *thread_state_container);

// ---------------------------------------------------------
// Table Vector Iterator
// ---------------------------------------------------------

void OpTableVectorIteratorConstruct(tpl::sql::TableVectorIterator *iter, u32 table_oid,
                               tpl::exec::ExecutionContext *exec_ctx);


VM_OP_HOT void OpTableVectorIteratorAddCol(tpl::sql::TableVectorIterator *iter, u32 col_oid) {
  iter->AddCol(col_oid);
}

void OpTableVectorIteratorPerformInit(tpl::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorNext(bool *has_more, tpl::sql::TableVectorIterator *iter) {
  *has_more = iter->Advance();
}

void OpTableVectorIteratorFree(tpl::sql::TableVectorIterator *iter);

VM_OP_HOT void OpTableVectorIteratorGetPCI(tpl::sql::ProjectedColumnsIterator **pci,
                                           tpl::sql::TableVectorIterator *iter) {
  *pci = iter->projected_columns_iterator();
}

VM_OP_HOT void OpParallelScanTable(const u32 db_oid, const u32 table_oid, void *const query_state,
                                   tpl::sql::ThreadStateContainer *const thread_states,
                                   const tpl::sql::TableVectorIterator::ScanFn scanner) {
  tpl::sql::TableVectorIterator::ParallelScan(db_oid, table_oid, query_state, thread_states, scanner);
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

VM_OP_HOT void OpPCIGetTinyInt(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i8, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetSmallInt(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetInteger(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetBigInt(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetReal(tpl::sql::Real *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<f32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read real value");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDouble(tpl::sql::Real *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<f64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read double value");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDecimal(tpl::sql::Decimal *out, UNUSED tpl::sql::ProjectedColumnsIterator *iter,
                               UNUSED u16 col_idx) {
  // TODO(Amadou): Implement once the representation of Decimal is settled upon.
  // The sql::Decimal class does not seem to match the storage layer's DECIMAL type as it needs a precision and
  // a scale.
  out->is_null = false;
  out->val = 0;
}

VM_OP_HOT void OpPCIGetDate(tpl::sql::Date *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<u32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read date");

  // Set
  out->is_null = false;
  out->int_val = *ptr;
}

VM_OP_HOT void OpPCIGetVarlen(tpl::sql::StringVal *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  auto *varlen = iter->Get<terrier::storage::VarlenEntry, false>(col_idx, nullptr);
  TPL_ASSERT(varlen != nullptr, "Null pointer when trying to read varlen");

  // Set
  *out = tpl::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
}

VM_OP_HOT void OpPCIGetTinyIntNull(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i8, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetSmallIntNull(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i16, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetIntegerNull(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetBigIntNull(tpl::sql::Integer *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i64, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetRealNull(tpl::sql::Real *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read real value");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDoubleNull(tpl::sql::Real *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f64, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read double value");

  // Set
  out->is_null = null;
  out->val = *ptr;
}

VM_OP_HOT void OpPCIGetDecimalNull(tpl::sql::Decimal *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  out->val = 0;
  out->is_null = false;
}

VM_OP_HOT void OpPCIGetDateNull(tpl::sql::Date *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<u32, true>(col_idx, &null);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read date");

  // Set
  out->is_null = null;
  out->int_val = *ptr;
}

VM_OP_HOT void OpPCIGetVarlenNull(tpl::sql::StringVal *out, tpl::sql::ProjectedColumnsIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *varlen = iter->Get<terrier::storage::VarlenEntry, true>(col_idx, &null);
  TPL_ASSERT(varlen != nullptr, "Null pointer when trying to read varlen");

  // Set
  if (null) {
    out->is_null = null;
  } else {
    *out = tpl::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
  }
}

void OpPCIFilterEqual(u64 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val);

void OpPCIFilterGreaterThan(u64 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val);

void OpPCIFilterGreaterThanEqual(u64 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val);

void OpPCIFilterLessThan(u64 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val);

void OpPCIFilterLessThanEqual(u64 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val);

void OpPCIFilterNotEqual(u64 *size, tpl::sql::ProjectedColumnsIterator *iter, u32 col_idx, i8 type, i64 val);

// ---------------------------------------------------------
// Hashing
// ---------------------------------------------------------

VM_OP_HOT void OpHashInt(hash_t *hash_val, tpl::sql::Integer *input) {
  *hash_val = tpl::util::Hasher::Hash<tpl::util::HashMethod::Crc>(input->val);
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashReal(hash_t *hash_val, tpl::sql::Real *input) {
  *hash_val = tpl::util::Hasher::Hash<tpl::util::HashMethod::Crc>(input->val);
  *hash_val = input->is_null ? 0 : *hash_val;
}

VM_OP_HOT void OpHashString(hash_t *hash_val, tpl::sql::StringVal *input) {
  *hash_val =
      tpl::util::Hasher::Hash<tpl::util::HashMethod::xxHash3>(reinterpret_cast<const u8 *>(input->Content()), input->len);
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

VM_OP_HOT void OpInitDate(tpl::sql::Date *result, i16 year, u8 month, u8 day) {
  result->is_null = false;
  result->ymd = date::year(year) / month / day;
}

VM_OP_HOT void OpInitString(tpl::sql::StringVal *result, u64 length, uintptr_t data) {
  *result = tpl::sql::StringVal(reinterpret_cast<char *>(data), static_cast<u32>(length));
}

VM_OP_HOT void OpInitVarlen(tpl::sql::StringVal *result, uintptr_t data) {
  auto *varlen = reinterpret_cast<terrier::storage::VarlenEntry *>(data);
  *result = tpl::sql::StringVal(reinterpret_cast<const char *>(varlen->Content()), varlen->Size());
}

#define GEN_SQL_COMPARISONS(TYPE)                                                                            \
  VM_OP_HOT void OpGreaterThan##TYPE(tpl::sql::BoolVal *const result, const tpl::sql::TYPE *const left,      \
                                     const tpl::sql::TYPE *const right) {                                    \
    tpl::sql::ComparisonFunctions::Gt##TYPE(result, *left, *right);                                          \
  }                                                                                                          \
  VM_OP_HOT void OpGreaterThanEqual##TYPE(tpl::sql::BoolVal *const result, const tpl::sql::TYPE *const left, \
                                          const tpl::sql::TYPE *const right) {                               \
    tpl::sql::ComparisonFunctions::Ge##TYPE(result, *left, *right);                                          \
  }                                                                                                          \
  VM_OP_HOT void OpEqual##TYPE(tpl::sql::BoolVal *const result, const tpl::sql::TYPE *const left,            \
                               const tpl::sql::TYPE *const right) {                                          \
    tpl::sql::ComparisonFunctions::Eq##TYPE(result, *left, *right);                                          \
  }                                                                                                          \
  VM_OP_HOT void OpLessThan##TYPE(tpl::sql::BoolVal *const result, const tpl::sql::TYPE *const left,         \
                                  const tpl::sql::TYPE *const right) {                                       \
    tpl::sql::ComparisonFunctions::Lt##TYPE(result, *left, *right);                                          \
  }                                                                                                          \
  VM_OP_HOT void OpLessThanEqual##TYPE(tpl::sql::BoolVal *const result, const tpl::sql::TYPE *const left,    \
                                       const tpl::sql::TYPE *const right) {                                  \
    tpl::sql::ComparisonFunctions::Le##TYPE(result, *left, *right);                                          \
  }                                                                                                          \
  VM_OP_HOT void OpNotEqual##TYPE(tpl::sql::BoolVal *const result, const tpl::sql::TYPE *const left,         \
                                  const tpl::sql::TYPE *const right) {                                       \
    tpl::sql::ComparisonFunctions::Ne##TYPE(result, *left, *right);                                          \
  }

GEN_SQL_COMPARISONS(Integer)
GEN_SQL_COMPARISONS(Real)
GEN_SQL_COMPARISONS(StringVal)
GEN_SQL_COMPARISONS(Date)
#undef GEN_SQL_COMPARISONS

// ----------------------------------
// SQL arithmetic
// ---------------------------------
VM_OP_WARM void OpAbsInteger(tpl::sql::Integer *const result, const tpl::sql::Integer *const left) {
  tpl::sql::ArithmeticFunctions::Abs(result, *left);
}

VM_OP_WARM void OpAbsReal(tpl::sql::Real *const result, const tpl::sql::Real *const left) {
  tpl::sql::ArithmeticFunctions::Abs(result, *left);
}

VM_OP_HOT void OpAddInteger(tpl::sql::Integer *const result, const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool overflow;
  tpl::sql::ArithmeticFunctions::Add(result, *left, *right, &overflow);
}

VM_OP_HOT void OpSubInteger(tpl::sql::Integer *const result, const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool overflow;
  tpl::sql::ArithmeticFunctions::Sub(result, *left, *right, &overflow);
}

VM_OP_HOT void OpMulInteger(tpl::sql::Integer *const result, const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool overflow;
  tpl::sql::ArithmeticFunctions::Mul(result, *left, *right, &overflow);
}

VM_OP_HOT void OpDivInteger(tpl::sql::Integer *const result, const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool div_by_zero = false;
  tpl::sql::ArithmeticFunctions::IntDiv(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpRemInteger(tpl::sql::Integer *const result, const tpl::sql::Integer *const left,
                            const tpl::sql::Integer *const right) {
  UNUSED bool div_by_zero = false;
  tpl::sql::ArithmeticFunctions::IntMod(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpAddReal(tpl::sql::Real *const result, const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  tpl::sql::ArithmeticFunctions::Add(result, *left, *right);
}

VM_OP_HOT void OpSubReal(tpl::sql::Real *const result, const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  tpl::sql::ArithmeticFunctions::Sub(result, *left, *right);
}

VM_OP_HOT void OpMulReal(tpl::sql::Real *const result, const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  tpl::sql::ArithmeticFunctions::Mul(result, *left, *right);
}

VM_OP_HOT void OpDivReal(tpl::sql::Real *const result, const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  UNUSED bool div_by_zero = false;
  tpl::sql::ArithmeticFunctions::Div(result, *left, *right, &div_by_zero);
}

VM_OP_HOT void OpRemReal(tpl::sql::Real *const result, const tpl::sql::Real *const left,
                         const tpl::sql::Real *const right) {
  UNUSED bool div_by_zero = false;
  tpl::sql::ArithmeticFunctions::Mod(result, *left, *right, &div_by_zero);
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
                                            const tpl::sql::AggregationHashTable::KeyEqFn key_eq_fn, void *iters[]) {
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

VM_OP_HOT void OpAggregationHashTableTransferPartitions(
    tpl::sql::AggregationHashTable *const agg_hash_table, tpl::sql::ThreadStateContainer *const thread_state_container,
    const u32 agg_ht_offset, const tpl::sql::AggregationHashTable::MergePartitionFn merge_partition_fn) {
  agg_hash_table->TransferMemoryAndPartitions(thread_state_container, agg_ht_offset, merge_partition_fn);
}

VM_OP_HOT void OpAggregationHashTableParallelPartitionedScan(
    tpl::sql::AggregationHashTable *const agg_hash_table, void *const query_state,
    tpl::sql::ThreadStateContainer *const thread_state_container,
    const tpl::sql::AggregationHashTable::ScanPartitionFn scan_partition_fn) {
  agg_hash_table->ExecuteParallelPartitionedScan(query_state, thread_state_container, scan_partition_fn);
}

void OpAggregationHashTableFree(tpl::sql::AggregationHashTable *agg_hash_table);

void OpAggregationHashTableIteratorInit(tpl::sql::AggregationHashTableIterator *iter,
                                        tpl::sql::AggregationHashTable *agg_hash_table);

VM_OP_HOT void OpAggregationHashTableIteratorHasNext(bool *has_more, tpl::sql::AggregationHashTableIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationHashTableIteratorNext(tpl::sql::AggregationHashTableIterator *iter) { iter->Next(); }

VM_OP_HOT void OpAggregationHashTableIteratorGetRow(const byte **row, tpl::sql::AggregationHashTableIterator *iter) {
  *row = iter->GetCurrentAggregateRow();
}

void OpAggregationHashTableIteratorFree(tpl::sql::AggregationHashTableIterator *iter);

VM_OP_HOT void OpAggregationOverflowPartitionIteratorHasNext(bool *has_more,
                                                             tpl::sql::AggregationOverflowPartitionIterator *iter) {
  *has_more = iter->HasNext();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorNext(tpl::sql::AggregationOverflowPartitionIterator *iter) {
  iter->Next();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetHash(hash_t *hash_val,
                                                             tpl::sql::AggregationOverflowPartitionIterator *iter) {
  *hash_val = iter->GetHash();
}

VM_OP_HOT void OpAggregationOverflowPartitionIteratorGetRow(const byte **row,
                                                            tpl::sql::AggregationOverflowPartitionIterator *iter) {
  *row = iter->GetPayload();
}

//
// COUNT
//

VM_OP_HOT void OpCountAggregateInit(tpl::sql::CountAggregate *agg) { new (agg) tpl::sql::CountAggregate(); }

VM_OP_HOT void OpCountAggregateAdvance(tpl::sql::CountAggregate *agg, tpl::sql::Val *val) { agg->Advance(*val); }

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

VM_OP_HOT void OpCountStarAggregateAdvance(tpl::sql::CountStarAggregate *agg, const tpl::sql::Val *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpCountStarAggregateMerge(tpl::sql::CountStarAggregate *agg_1,
                                         const tpl::sql::CountStarAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpCountStarAggregateReset(tpl::sql::CountStarAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpCountStarAggregateGetResult(tpl::sql::Integer *result, const tpl::sql::CountStarAggregate *agg) {
  *result = agg->GetCountResult();
}

VM_OP_HOT void OpCountStarAggregateFree(tpl::sql::CountStarAggregate *agg) { agg->~CountStarAggregate(); }

//
// SUM(int_type)
//

VM_OP_HOT void OpIntegerSumAggregateInit(tpl::sql::IntegerSumAggregate *agg) {
  new (agg) tpl::sql::IntegerSumAggregate();
}

VM_OP_HOT void OpIntegerSumAggregateAdvance(tpl::sql::IntegerSumAggregate *agg, const tpl::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerSumAggregateMerge(tpl::sql::IntegerSumAggregate *agg_1,
                                          const tpl::sql::IntegerSumAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerSumAggregateReset(tpl::sql::IntegerSumAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerSumAggregateGetResult(tpl::sql::Integer *result, const tpl::sql::IntegerSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpIntegerSumAggregateFree(tpl::sql::IntegerSumAggregate *agg) { agg->~IntegerSumAggregate(); }

//
// MAX(int_type)
//

VM_OP_HOT void OpIntegerMaxAggregateInit(tpl::sql::IntegerMaxAggregate *agg) {
  new (agg) tpl::sql::IntegerMaxAggregate();
}

VM_OP_HOT void OpIntegerMaxAggregateAdvance(tpl::sql::IntegerMaxAggregate *agg, const tpl::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerMaxAggregateMerge(tpl::sql::IntegerMaxAggregate *agg_1,
                                          const tpl::sql::IntegerMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMaxAggregateReset(tpl::sql::IntegerMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerMaxAggregateGetResult(tpl::sql::Integer *result, const tpl::sql::IntegerMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpIntegerMaxAggregateFree(tpl::sql::IntegerMaxAggregate *agg) { agg->~IntegerMaxAggregate(); }

//
// MIN(int_type)
//

VM_OP_HOT void OpIntegerMinAggregateInit(tpl::sql::IntegerMinAggregate *agg) {
  new (agg) tpl::sql::IntegerMinAggregate();
}

VM_OP_HOT void OpIntegerMinAggregateAdvance(tpl::sql::IntegerMinAggregate *agg, const tpl::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpIntegerMinAggregateMerge(tpl::sql::IntegerMinAggregate *agg_1,
                                          const tpl::sql::IntegerMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpIntegerMinAggregateReset(tpl::sql::IntegerMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpIntegerMinAggregateGetResult(tpl::sql::Integer *result, const tpl::sql::IntegerMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpIntegerMinAggregateFree(tpl::sql::IntegerMinAggregate *agg) { agg->~IntegerMinAggregate(); }

//
// SUM(real)
//

VM_OP_HOT void OpRealSumAggregateInit(tpl::sql::RealSumAggregate *agg) { new (agg) tpl::sql::RealSumAggregate(); }

VM_OP_HOT void OpRealSumAggregateAdvance(tpl::sql::RealSumAggregate *agg, const tpl::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealSumAggregateMerge(tpl::sql::RealSumAggregate *agg_1, const tpl::sql::RealSumAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealSumAggregateReset(tpl::sql::RealSumAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealSumAggregateGetResult(tpl::sql::Real *result, const tpl::sql::RealSumAggregate *agg) {
  *result = agg->GetResultSum();
}

VM_OP_HOT void OpRealSumAggregateFree(tpl::sql::RealSumAggregate *agg) { agg->~RealSumAggregate(); }

//
// MAX(real_type)
//

VM_OP_HOT void OpRealMaxAggregateInit(tpl::sql::RealMaxAggregate *agg) { new (agg) tpl::sql::RealMaxAggregate(); }

VM_OP_HOT void OpRealMaxAggregateAdvance(tpl::sql::RealMaxAggregate *agg, const tpl::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealMaxAggregateMerge(tpl::sql::RealMaxAggregate *agg_1, const tpl::sql::RealMaxAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealMaxAggregateReset(tpl::sql::RealMaxAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealMaxAggregateGetResult(tpl::sql::Real *result, const tpl::sql::RealMaxAggregate *agg) {
  *result = agg->GetResultMax();
}

VM_OP_HOT void OpRealMaxAggregateFree(tpl::sql::RealMaxAggregate *agg) { agg->~RealMaxAggregate(); }

//
// MIN(real_type)
//

VM_OP_HOT void OpRealMinAggregateInit(tpl::sql::RealMinAggregate *agg) { new (agg) tpl::sql::RealMinAggregate(); }

VM_OP_HOT void OpRealMinAggregateAdvance(tpl::sql::RealMinAggregate *agg, const tpl::sql::Real *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealMinAggregateMerge(tpl::sql::RealMinAggregate *agg_1, const tpl::sql::RealMinAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpRealMinAggregateReset(tpl::sql::RealMinAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpRealMinAggregateGetResult(tpl::sql::Real *result, const tpl::sql::RealMinAggregate *agg) {
  *result = agg->GetResultMin();
}

VM_OP_HOT void OpRealMinAggregateFree(tpl::sql::RealMinAggregate *agg) { agg->~RealMinAggregate(); }

//
// AVG
//

VM_OP_HOT void OpAvgAggregateInit(tpl::sql::AvgAggregate *agg) { new (agg) tpl::sql::AvgAggregate(); }

VM_OP_HOT void OpIntegerAvgAggregateAdvance(tpl::sql::AvgAggregate *agg, const tpl::sql::Integer *val) {
  agg->Advance(*val);
}

VM_OP_HOT void OpRealAvgAggregateAdvance(tpl::sql::AvgAggregate *agg, const tpl::sql::Real *val) { agg->Advance(*val); }

VM_OP_HOT void OpAvgAggregateMerge(tpl::sql::AvgAggregate *agg_1, const tpl::sql::AvgAggregate *agg_2) {
  agg_1->Merge(*agg_2);
}

VM_OP_HOT void OpAvgAggregateReset(tpl::sql::AvgAggregate *agg) { agg->Reset(); }

VM_OP_HOT void OpAvgAggregateGetResult(tpl::sql::Real *result, const tpl::sql::AvgAggregate *agg) {
  *result = agg->GetResultAvg();
}

VM_OP_HOT void OpAvgAggregateFree(tpl::sql::AvgAggregate *agg) { agg->~AvgAggregate(); }
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

VM_OP_HOT void OpJoinHashTableIterInit(tpl::sql::JoinHashTableIterator *result,
                                       tpl::sql::JoinHashTable *join_hash_table, hash_t hash) {
  *result = join_hash_table->Lookup<false>(hash);
}

VM_OP_HOT void OpJoinHashTableIterHasNext(bool *has_more, tpl::sql::JoinHashTableIterator *iterator,
                                          tpl::sql::JoinHashTableIterator::KeyEq key_eq, void *opaque_ctx,
                                          void *probe_tuple) {
  *has_more = iterator->HasNext(key_eq, opaque_ctx, probe_tuple);
}

VM_OP_HOT void OpJoinHashTableIterGetRow(const byte **result, tpl::sql::JoinHashTableIterator *iterator) {
  *result = iterator->NextMatch()->payload;
}

VM_OP_HOT void OpJoinHashTableIterClose(tpl::sql::JoinHashTableIterator *iterator) {
  iterator->~JoinHashTableIterator();
}

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

VM_OP_WARM void OpPi(tpl::sql::Real *result) { tpl::sql::ArithmeticFunctions::Pi(result); }

VM_OP_WARM void OpE(tpl::sql::Real *result) { tpl::sql::ArithmeticFunctions::E(result); }

VM_OP_WARM void OpAcos(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Acos(result, *input);
}

VM_OP_WARM void OpAsin(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Asin(result, *input);
}

VM_OP_WARM void OpAtan(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Atan(result, *input);
}

VM_OP_WARM void OpAtan2(tpl::sql::Real *result, const tpl::sql::Real *arg_1, const tpl::sql::Real *arg_2) {
  tpl::sql::ArithmeticFunctions::Atan2(result, *arg_1, *arg_2);
}

VM_OP_WARM void OpCos(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Cos(result, *input);
}

VM_OP_WARM void OpCot(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Cot(result, *input);
}

VM_OP_WARM void OpSin(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Sin(result, *input);
}

VM_OP_WARM void OpTan(tpl::sql::Real *result, const tpl::sql::Real *input) {
  tpl::sql::ArithmeticFunctions::Tan(result, *input);
}

VM_OP_WARM void OpCosh(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Cosh(result, *v);
}

VM_OP_WARM void OpTanh(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Tanh(result, *v);
}

VM_OP_WARM void OpSinh(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Sinh(result, *v);
}

VM_OP_WARM void OpSqrt(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Sqrt(result, *v);
}

VM_OP_WARM void OpCbrt(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Cbrt(result, *v);
}

VM_OP_WARM void OpExp(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Exp(result, *v);
}

VM_OP_WARM void OpCeil(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Ceil(result, *v);
}

VM_OP_WARM void OpFloor(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Floor(result, *v);
}

VM_OP_WARM void OpTruncate(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Truncate(result, *v);
}

VM_OP_WARM void OpLn(tpl::sql::Real *result, const tpl::sql::Real *v) { tpl::sql::ArithmeticFunctions::Ln(result, *v); }

VM_OP_WARM void OpLog2(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Log2(result, *v);
}

VM_OP_WARM void OpLog10(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Log10(result, *v);
}

VM_OP_WARM void OpSign(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Sign(result, *v);
}

VM_OP_WARM void OpRadians(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Radians(result, *v);
}

VM_OP_WARM void OpDegrees(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Degrees(result, *v);
}

VM_OP_WARM void OpRound(tpl::sql::Real *result, const tpl::sql::Real *v) {
  tpl::sql::ArithmeticFunctions::Round(result, *v);
}

VM_OP_WARM void OpRoundUpTo(tpl::sql::Real *result, const tpl::sql::Real *v, const tpl::sql::Integer *scale) {
  tpl::sql::ArithmeticFunctions::RoundUpTo(result, *v, *scale);
}

VM_OP_WARM void OpLog(tpl::sql::Real *result, const tpl::sql::Real *base, const tpl::sql::Real *val) {
  tpl::sql::ArithmeticFunctions::Log(result, *base, *val);
}

VM_OP_WARM void OpPow(tpl::sql::Real *result, const tpl::sql::Real *base, const tpl::sql::Real *val) {
  tpl::sql::ArithmeticFunctions::Pow(result, *base, *val);
}

// ---------------------------------------------------------
// Null/Not Null predicates
// ---------------------------------------------------------

VM_OP_WARM void OpValIsNull(tpl::sql::BoolVal *result, const tpl::sql::Val *val) {
  tpl::sql::IsNullPredicate::IsNull(result, *val);
}

VM_OP_WARM void OpValIsNotNull(tpl::sql::BoolVal *result, const tpl::sql::Val *val) {
  tpl::sql::IsNullPredicate::IsNotNull(result, *val);
}

// ---------------------------------------------------------
// String functions
// ---------------------------------------------------------

VM_OP_WARM void OpCharLength(tpl::exec::ExecutionContext *ctx, tpl::sql::Integer *result,
                             const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::CharLength(ctx, result, *str);
}

VM_OP_WARM void OpLeft(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result, const tpl::sql::StringVal *str,
                       const tpl::sql::Integer *n) {
  tpl::sql::StringFunctions::Left(ctx, result, *str, *n);
}

VM_OP_WARM void OpLength(tpl::exec::ExecutionContext *ctx, tpl::sql::Integer *result, const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::Length(ctx, result, *str);
}

VM_OP_WARM void OpLower(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result, const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::Lower(ctx, result, *str);
}

VM_OP_WARM void OpLPad(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result, const tpl::sql::StringVal *str,
                       const tpl::sql::Integer *len, const tpl::sql::StringVal *pad) {
  tpl::sql::StringFunctions::Lpad(ctx, result, *str, *len, *pad);
}

VM_OP_WARM void OpLTrim(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result, const tpl::sql::StringVal *str,
                        const tpl::sql::StringVal *chars) {
  tpl::sql::StringFunctions::Ltrim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpRepeat(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result, const tpl::sql::StringVal *str,
                         const tpl::sql::Integer *n) {
  tpl::sql::StringFunctions::Repeat(ctx, result, *str, *n);
}

VM_OP_WARM void OpReverse(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result,
                          const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::Reverse(ctx, result, *str);
}

VM_OP_WARM void OpRight(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result, const tpl::sql::StringVal *str,
                        const tpl::sql::Integer *n) {
  tpl::sql::StringFunctions::Right(ctx, result, *str, *n);
}

VM_OP_WARM void OpRPad(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result, const tpl::sql::StringVal *str,
                       const tpl::sql::Integer *n, const tpl::sql::StringVal *pad) {
  tpl::sql::StringFunctions::Rpad(ctx, result, *str, *n, *pad);
}

VM_OP_WARM void OpRTrim(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result, const tpl::sql::StringVal *str,
                        const tpl::sql::StringVal *chars) {
  tpl::sql::StringFunctions::Rtrim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpSplitPart(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result,
                            const tpl::sql::StringVal *str, const tpl::sql::StringVal *delim,
                            const tpl::sql::Integer *field) {
  tpl::sql::StringFunctions::SplitPart(ctx, result, *str, *delim, *field);
}

VM_OP_WARM void OpSubstring(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result,
                            const tpl::sql::StringVal *str, const tpl::sql::Integer *pos,
                            const tpl::sql::Integer *len) {
  tpl::sql::StringFunctions::Substring(ctx, result, *str, *pos, *len);
}

VM_OP_WARM void OpTrim(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result, const tpl::sql::StringVal *str,
                       const tpl::sql::StringVal *chars) {
  tpl::sql::StringFunctions::Trim(ctx, result, *str, *chars);
}

VM_OP_WARM void OpUpper(tpl::exec::ExecutionContext *ctx, tpl::sql::StringVal *result, const tpl::sql::StringVal *str) {
  tpl::sql::StringFunctions::Upper(ctx, result, *str);
}
// ---------------------------------------------------------------
// Index Iterator
// ---------------------------------------------------------------
void OpIndexIteratorConstruct(tpl::sql::IndexIterator *iter, uint32_t table_oid, uint32_t index_oid,
                         tpl::exec::ExecutionContext *exec_ctx);
void OpIndexIteratorFree(tpl::sql::IndexIterator *iter);

void OpIndexIteratorPerformInit(tpl::sql::IndexIterator *iter);

VM_OP_HOT void OpIndexIteratorAddCol(tpl::sql::IndexIterator *iter, uint32_t col_oid) {
  iter->AddCol(col_oid);
}

VM_OP_HOT void OpIndexIteratorScanKey(tpl::sql::IndexIterator *iter) { iter->ScanKey(); }

VM_OP_HOT void OpIndexIteratorAdvance(bool *has_more, tpl::sql::IndexIterator *iter) { *has_more = iter->Advance(); }

VM_OP_HOT void OpIndexIteratorGetTinyInt(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetSmallInt(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i16, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetInteger(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetBigInt(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<i64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetReal(tpl::sql::Real *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<f32, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDouble(tpl::sql::Real *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  auto *ptr = iter->Get<f64, false>(col_idx, nullptr);
  TPL_ASSERT(ptr != nullptr, "Null pointer when trying to read integer");

  // Set
  out->is_null = false;
  out->val = *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDecimal(tpl::sql::Decimal *out, UNUSED tpl::sql::IndexIterator *iter,
                                         UNUSED u16 col_idx) {
  // Set
  out->is_null = false;
  out->val = 0;
}

VM_OP_HOT void OpIndexIteratorGetTinyIntNull(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i8, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetSmallIntNull(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i16, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetIntegerNull(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i32, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetBigIntNull(tpl::sql::Integer *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<i64, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetRealNull(tpl::sql::Real *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f32, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDoubleNull(tpl::sql::Real *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  // Read
  bool null = false;
  auto *ptr = iter->Get<f64, true>(col_idx, &null);

  // Set
  out->is_null = null;
  out->val = null ? 0 : *ptr;
}

VM_OP_HOT void OpIndexIteratorGetDecimalNull(tpl::sql::Decimal *out, tpl::sql::IndexIterator *iter, u16 col_idx) {
  out->val = 0;
  out->is_null = false;
}

VM_OP_HOT void OpIndexIteratorSetKeyTinyInt(tpl::sql::IndexIterator *iter, u16 col_idx, tpl::sql::Integer *val) {
  iter->SetKey(col_idx, static_cast<i8>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeySmallInt(tpl::sql::IndexIterator *iter, u16 col_idx, tpl::sql::Integer *val) {
  iter->SetKey(col_idx, static_cast<i16>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeyInt(tpl::sql::IndexIterator *iter, u16 col_idx, tpl::sql::Integer *val) {
  iter->SetKey(col_idx, static_cast<i32>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeyBigInt(tpl::sql::IndexIterator *iter, u16 col_idx, tpl::sql::Integer *val) {
  iter->SetKey(col_idx, static_cast<i64>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeyReal(tpl::sql::IndexIterator *iter, u16 col_idx, tpl::sql::Real *val) {
  iter->SetKey(col_idx, static_cast<f32>(val->val), val->is_null);
}

VM_OP_HOT void OpIndexIteratorSetKeyDouble(tpl::sql::IndexIterator *iter, u16 col_idx, tpl::sql::Real *val) {
  iter->SetKey(col_idx, static_cast<f64>(val->val), val->is_null);
}

// ---------------------------------------------------------------
// Insert Calls
// ---------------------------------------------------------------

void OpInsert(tpl::exec::ExecutionContext *exec_ctx, u32 table_oid, byte *values_ptr);

// ---------------------------------------------------------------
// Output Calls
// ---------------------------------------------------------------

void OpOutputAlloc(tpl::exec::ExecutionContext *exec_ctx, byte **result);

void OpOutputAdvance(tpl::exec::ExecutionContext *exec_ctx);

void OpOutputSetNull(tpl::exec::ExecutionContext *exec_ctx, u32 idx);

void OpOutputFinalize(tpl::exec::ExecutionContext *exec_ctx);

}  // extern "C"
