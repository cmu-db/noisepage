#pragma once

#include <algorithm>
#include <cstdint>

#include "common/macros.h"
#include "execution/util/execution_common.h"
#include "execution/vm/bytecode_operands.h"

namespace terrier::execution::vm {

// Creates instances of a given opcode for all integer primitive types
#define CREATE_FOR_INT_TYPES(F, op, ...) \
  F(op##_##int8_t, __VA_ARGS__)          \
  F(op##_##int16_t, __VA_ARGS__)         \
  F(op##_##int32_t, __VA_ARGS__)         \
  F(op##_##int64_t, __VA_ARGS__)         \
  F(op##_##uint8_t, __VA_ARGS__)         \
  F(op##_##uint16_t, __VA_ARGS__)        \
  F(op##_##uint32_t, __VA_ARGS__)        \
  F(op##_##uint64_t, __VA_ARGS__)

// Creates instances of a given opcode for primitive boolean types
#define CREATE_FOR_BOOL_TYPES(F, op, ...) F(op##_bool, __VA_ARGS__)

// Creates instances of a given opcode for all floating-point primitive types
#define CREATE_FOR_FLOAT_TYPES(F, op, ...) \
  F(op##_##float, __VA_ARGS__)             \
  F(op##_##double, __VA_ARGS__)

// Creates instances of a given opcode for primitive numeric types
#define CREATE_FOR_NUMERIC_TYPES(F, op, ...) \
  CREATE_FOR_INT_TYPES(F, op, __VA_ARGS__)   \
  CREATE_FOR_FLOAT_TYPES(F, op, __VA_ARGS__)

// Creates instances of a given opcode for *ALL* primitive types
#define CREATE_FOR_ALL_TYPES(F, op, ...)     \
  CREATE_FOR_INT_TYPES(F, op, __VA_ARGS__)   \
  CREATE_FOR_FLOAT_TYPES(F, op, __VA_ARGS__) \
  CREATE_FOR_BOOL_TYPES(F, op, __VA_ARGS__)

#define GET_BASE_FOR_INT_TYPES(op) (op##_int8_t)
#define GET_BASE_FOR_FLOAT_TYPES(op) (op##_float)
#define GET_BASE_FOR_BOOL_TYPES(op) (op##_bool)

/**
 * The master list of all bytecodes, flags and operands
 */
#define BYTECODE_LIST(F)                                                                                              \
  /* Primitive operations */                                                                                          \
  CREATE_FOR_NUMERIC_TYPES(F, Neg, OperandType::Local, OperandType::Local)                                            \
  CREATE_FOR_NUMERIC_TYPES(F, Add, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  CREATE_FOR_NUMERIC_TYPES(F, Sub, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  CREATE_FOR_NUMERIC_TYPES(F, Mul, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  CREATE_FOR_NUMERIC_TYPES(F, Div, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  CREATE_FOR_NUMERIC_TYPES(F, Rem, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  CREATE_FOR_INT_TYPES(F, BitAnd, OperandType::Local, OperandType::Local, OperandType::Local)                         \
  CREATE_FOR_INT_TYPES(F, BitOr, OperandType::Local, OperandType::Local, OperandType::Local)                          \
  CREATE_FOR_INT_TYPES(F, BitXor, OperandType::Local, OperandType::Local, OperandType::Local)                         \
  CREATE_FOR_INT_TYPES(F, BitNeg, OperandType::Local, OperandType::Local)                                             \
  CREATE_FOR_ALL_TYPES(F, GreaterThan, OperandType::Local, OperandType::Local, OperandType::Local)                    \
  CREATE_FOR_ALL_TYPES(F, GreaterThanEqual, OperandType::Local, OperandType::Local, OperandType::Local)               \
  CREATE_FOR_ALL_TYPES(F, Equal, OperandType::Local, OperandType::Local, OperandType::Local)                          \
  CREATE_FOR_ALL_TYPES(F, LessThan, OperandType::Local, OperandType::Local, OperandType::Local)                       \
  CREATE_FOR_ALL_TYPES(F, LessThanEqual, OperandType::Local, OperandType::Local, OperandType::Local)                  \
  CREATE_FOR_ALL_TYPES(F, NotEqual, OperandType::Local, OperandType::Local, OperandType::Local)                       \
  /* Boolean complement */                                                                                            \
  F(Not, OperandType::Local, OperandType::Local)                                                                      \
                                                                                                                      \
  /* Branching */                                                                                                     \
  F(Jump, OperandType::JumpOffset)                                                                                    \
  F(JumpIfTrue, OperandType::Local, OperandType::JumpOffset)                                                          \
  F(JumpIfFalse, OperandType::Local, OperandType::JumpOffset)                                                         \
                                                                                                                      \
  /* Memory/pointer operations */                                                                                     \
  F(IsNullPtr, OperandType::Local, OperandType::Local)                                                                \
  F(IsNotNullPtr, OperandType::Local, OperandType::Local)                                                             \
  F(Deref1, OperandType::Local, OperandType::Local)                                                                   \
  F(Deref2, OperandType::Local, OperandType::Local)                                                                   \
  F(Deref4, OperandType::Local, OperandType::Local)                                                                   \
  F(Deref8, OperandType::Local, OperandType::Local)                                                                   \
  F(DerefN, OperandType::Local, OperandType::Local, OperandType::UImm4)                                               \
  F(Assign1, OperandType::Local, OperandType::Local)                                                                  \
  F(Assign2, OperandType::Local, OperandType::Local)                                                                  \
  F(Assign4, OperandType::Local, OperandType::Local)                                                                  \
  F(Assign8, OperandType::Local, OperandType::Local)                                                                  \
  F(AssignImm1, OperandType::Local, OperandType::Imm1)                                                                \
  F(AssignImm2, OperandType::Local, OperandType::Imm2)                                                                \
  F(AssignImm4, OperandType::Local, OperandType::Imm4)                                                                \
  F(AssignImm8, OperandType::Local, OperandType::Imm8)                                                                \
  F(AssignImm4F, OperandType::Local, OperandType::Imm4F)                                                              \
  F(AssignImm8F, OperandType::Local, OperandType::Imm8F)                                                              \
  F(Lea, OperandType::Local, OperandType::Local, OperandType::Imm4)                                                   \
  F(LeaScaled, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Imm4, OperandType::Imm4)      \
                                                                                                                      \
  /* Function calls */                                                                                                \
  F(Call, OperandType::FunctionId, OperandType::LocalCount)                                                           \
  F(Return)                                                                                                           \
                                                                                                                      \
  /* Execution Context */                                                                                             \
  F(ExecutionContextGetMemoryPool, OperandType::Local, OperandType::Local)                                            \
  F(ExecutionContextStartResourceTracker, OperandType::Local, OperandType::Local)                                     \
  F(ExecutionContextEndResourceTracker, OperandType::Local, OperandType::Local)                                       \
  F(ExecutionContextEndPipelineTracker, OperandType::Local, OperandType::Local, OperandType::Local)                   \
                                                                                                                      \
  /* Thread State Container */                                                                                        \
  F(ThreadStateContainerInit, OperandType::Local, OperandType::Local)                                                 \
  F(ThreadStateContainerIterate, OperandType::Local, OperandType::Local, OperandType::FunctionId)                     \
  F(ThreadStateContainerReset, OperandType::Local, OperandType::Local, OperandType::FunctionId,                       \
    OperandType::FunctionId, OperandType::Local)                                                                      \
  F(ThreadStateContainerFree, OperandType::Local)                                                                     \
                                                                                                                      \
  /* Table Vector Iterator */                                                                                         \
  F(TableVectorIteratorInit, OperandType::Local, OperandType::Local, OperandType::UImm4, OperandType::Local,          \
    OperandType::UImm4)                                                                                               \
  F(TableVectorIteratorPerformInit, OperandType::Local)                                                               \
  F(TableVectorIteratorNext, OperandType::Local, OperandType::Local)                                                  \
  F(TableVectorIteratorReset, OperandType::Local)                                                                     \
  F(TableVectorIteratorFree, OperandType::Local)                                                                      \
  F(TableVectorIteratorGetPCI, OperandType::Local, OperandType::Local)                                                \
  F(ParallelScanTable, OperandType::UImm4, OperandType::UImm4, OperandType::Local, OperandType::Local,                \
    OperandType::FunctionId)                                                                                          \
                                                                                                                      \
  /* ProjectedColumns Iterator (PCI) */                                                                               \
  F(PCIIsFiltered, OperandType::Local, OperandType::Local)                                                            \
  F(PCIHasNext, OperandType::Local, OperandType::Local)                                                               \
  F(PCIHasNextFiltered, OperandType::Local, OperandType::Local)                                                       \
  F(PCIAdvance, OperandType::Local)                                                                                   \
  F(PCIAdvanceFiltered, OperandType::Local)                                                                           \
  F(PCIMatch, OperandType::Local, OperandType::Local)                                                                 \
  F(PCIReset, OperandType::Local)                                                                                     \
  F(PCIResetFiltered, OperandType::Local)                                                                             \
  F(PCIGetSlot, OperandType::Local, OperandType::Local)                                                               \
  F(PCIGetBool, OperandType::Local, OperandType::Local, OperandType::UImm2)                                           \
  F(PCIGetTinyInt, OperandType::Local, OperandType::Local, OperandType::UImm2)                                        \
  F(PCIGetSmallInt, OperandType::Local, OperandType::Local, OperandType::UImm2)                                       \
  F(PCIGetInteger, OperandType::Local, OperandType::Local, OperandType::UImm2)                                        \
  F(PCIGetBigInt, OperandType::Local, OperandType::Local, OperandType::UImm2)                                         \
  F(PCIGetReal, OperandType::Local, OperandType::Local, OperandType::UImm2)                                           \
  F(PCIGetDouble, OperandType::Local, OperandType::Local, OperandType::UImm2)                                         \
  F(PCIGetDecimal, OperandType::Local, OperandType::Local, OperandType::UImm2)                                        \
  F(PCIGetDateVal, OperandType::Local, OperandType::Local, OperandType::UImm2)                                        \
  F(PCIGetTimestampVal, OperandType::Local, OperandType::Local, OperandType::UImm2)                                   \
  F(PCIGetVarlen, OperandType::Local, OperandType::Local, OperandType::UImm2)                                         \
  F(PCIGetBoolNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                       \
  F(PCIGetTinyIntNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                    \
  F(PCIGetSmallIntNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                   \
  F(PCIGetIntegerNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                    \
  F(PCIGetBigIntNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                     \
  F(PCIGetRealNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                       \
  F(PCIGetDoubleNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                     \
  F(PCIGetDecimalNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                    \
  F(PCIGetDateValNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                    \
  F(PCIGetTimestampValNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                               \
  F(PCIGetVarlenNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                     \
  F(PCIFilterEqual, OperandType::Local, OperandType::Local, OperandType::UImm4, OperandType::Imm1, OperandType::Imm8) \
  F(PCIFilterGreaterThan, OperandType::Local, OperandType::Local, OperandType::UImm4, OperandType::Imm1,              \
    OperandType::Imm8)                                                                                                \
  F(PCIFilterGreaterThanEqual, OperandType::Local, OperandType::Local, OperandType::UImm4, OperandType::Imm1,         \
    OperandType::Imm8)                                                                                                \
  F(PCIFilterLessThan, OperandType::Local, OperandType::Local, OperandType::UImm4, OperandType::Imm1,                 \
    OperandType::Imm8)                                                                                                \
  F(PCIFilterLessThanEqual, OperandType::Local, OperandType::Local, OperandType::UImm4, OperandType::Imm1,            \
    OperandType::Imm8)                                                                                                \
  F(PCIFilterNotEqual, OperandType::Local, OperandType::Local, OperandType::UImm4, OperandType::Imm1,                 \
    OperandType::Imm8)                                                                                                \
                                                                                                                      \
  /* Filter Manager */                                                                                                \
  F(FilterManagerInit, OperandType::Local)                                                                            \
  F(FilterManagerStartNewClause, OperandType::Local)                                                                  \
  F(FilterManagerInsertFlavor, OperandType::Local, OperandType::FunctionId)                                           \
  F(FilterManagerFinalize, OperandType::Local)                                                                        \
  F(FilterManagerRunFilters, OperandType::Local, OperandType::Local)                                                  \
  F(FilterManagerFree, OperandType::Local)                                                                            \
                                                                                                                      \
  /* Date functions */                                                                                                \
  F(ExtractYear, OperandType::Local, OperandType::Local)                                                              \
                                                                                                                      \
  /* SQL type comparisons */                                                                                          \
  F(ForceBoolTruth, OperandType::Local, OperandType::Local)                                                           \
  F(InitSqlNull, OperandType::Local)                                                                                  \
  F(InitBoolVal, OperandType::Local, OperandType::Local)                                                              \
  F(InitInteger, OperandType::Local, OperandType::Local)                                                              \
  F(InitReal, OperandType::Local, OperandType::Local)                                                                 \
  F(IntegerToReal, OperandType::Local, OperandType::Local)                                                            \
  F(InitDate, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                         \
  F(InitTimestamp, OperandType::Local, OperandType::Local)                                                            \
  F(InitTimestampHMSu, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,                \
    OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                                   \
  F(InitString, OperandType::Local, OperandType::Imm8, OperandType::Imm8)                                             \
  F(InitVarlen, OperandType::Local, OperandType::Local)                                                               \
  F(LessThanBoolVal, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(LessThanEqualBoolVal, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
  F(GreaterThanBoolVal, OperandType::Local, OperandType::Local, OperandType::Local)                                   \
  F(GreaterThanEqualBoolVal, OperandType::Local, OperandType::Local, OperandType::Local)                              \
  F(EqualBoolVal, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(NotEqualBoolVal, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(LessThanInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(LessThanEqualInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
  F(GreaterThanInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                   \
  F(GreaterThanEqualInteger, OperandType::Local, OperandType::Local, OperandType::Local)                              \
  F(EqualInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(NotEqualInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(LessThanReal, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(LessThanEqualReal, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(GreaterThanReal, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(GreaterThanEqualReal, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
  F(EqualReal, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(NotEqualReal, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(LessThanStringVal, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(LessThanEqualStringVal, OperandType::Local, OperandType::Local, OperandType::Local)                               \
  F(GreaterThanStringVal, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
  F(GreaterThanEqualStringVal, OperandType::Local, OperandType::Local, OperandType::Local)                            \
  F(EqualStringVal, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
  F(NotEqualStringVal, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(LessThanDateVal, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(LessThanEqualDateVal, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
  F(GreaterThanDateVal, OperandType::Local, OperandType::Local, OperandType::Local)                                   \
  F(GreaterThanEqualDateVal, OperandType::Local, OperandType::Local, OperandType::Local)                              \
  F(EqualDateVal, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(NotEqualDateVal, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(LessThanTimestampVal, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
  F(LessThanEqualTimestampVal, OperandType::Local, OperandType::Local, OperandType::Local)                            \
  F(GreaterThanTimestampVal, OperandType::Local, OperandType::Local, OperandType::Local)                              \
  F(GreaterThanEqualTimestampVal, OperandType::Local, OperandType::Local, OperandType::Local)                         \
  F(EqualTimestampVal, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(NotEqualTimestampVal, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
                                                                                                                      \
  /* SQL value unary operations */                                                                                    \
  F(AbsInteger, OperandType::Local, OperandType::Local)                                                               \
  F(AbsReal, OperandType::Local, OperandType::Local)                                                                  \
  F(ValIsNull, OperandType::Local, OperandType::Local)                                                                \
  F(ValIsNotNull, OperandType::Local, OperandType::Local)                                                             \
  /* SQL value binary operations */                                                                                   \
  F(AddInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(SubInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(MulInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(DivInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(RemInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(AddReal, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(SubReal, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(MulReal, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(DivReal, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(RemReal, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
                                                                                                                      \
  /* Hashing */                                                                                                       \
  F(HashInt, OperandType::Local, OperandType::Local)                                                                  \
  F(HashReal, OperandType::Local, OperandType::Local)                                                                 \
  F(HashString, OperandType::Local, OperandType::Local)                                                               \
  F(HashCombine, OperandType::Local, OperandType::Local)                                                              \
                                                                                                                      \
  /* Aggregation Hash Table */                                                                                        \
  F(AggregationHashTableInit, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  F(AggregationHashTableInsert, OperandType::Local, OperandType::Local, OperandType::Local)                           \
  F(AggregationHashTableLookup, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::FunctionId,  \
    OperandType::Local)                                                                                               \
  F(AggregationHashTableProcessBatch, OperandType::Local, OperandType::Local, OperandType::FunctionId,                \
    OperandType::FunctionId, OperandType::FunctionId, OperandType::FunctionId)                                        \
  F(AggregationHashTableTransferPartitions, OperandType::Local, OperandType::Local, OperandType::Local,               \
    OperandType::FunctionId)                                                                                          \
  F(AggregationHashTableParallelPartitionedScan, OperandType::Local, OperandType::Local, OperandType::Local,          \
    OperandType::FunctionId)                                                                                          \
  F(AggregationHashTableFree, OperandType::Local)                                                                     \
  F(AggregationHashTableIteratorInit, OperandType::Local, OperandType::Local)                                         \
  F(AggregationHashTableIteratorHasNext, OperandType::Local, OperandType::Local)                                      \
  F(AggregationHashTableIteratorNext, OperandType::Local)                                                             \
  F(AggregationHashTableIteratorGetRow, OperandType::Local, OperandType::Local)                                       \
  F(AggregationHashTableIteratorFree, OperandType::Local)                                                             \
  F(AggregationOverflowPartitionIteratorHasNext, OperandType::Local, OperandType::Local)                              \
  F(AggregationOverflowPartitionIteratorNext, OperandType::Local)                                                     \
  F(AggregationOverflowPartitionIteratorGetHash, OperandType::Local, OperandType::Local)                              \
  F(AggregationOverflowPartitionIteratorGetRow, OperandType::Local, OperandType::Local)                               \
  /* Aggregates */                                                                                                    \
  F(CountAggregateInit, OperandType::Local)                                                                           \
  F(CountAggregateAdvance, OperandType::Local, OperandType::Local)                                                    \
  F(CountAggregateMerge, OperandType::Local, OperandType::Local)                                                      \
  F(CountAggregateReset, OperandType::Local)                                                                          \
  F(CountAggregateGetResult, OperandType::Local, OperandType::Local)                                                  \
  F(CountAggregateFree, OperandType::Local)                                                                           \
  F(CountStarAggregateInit, OperandType::Local)                                                                       \
  F(CountStarAggregateAdvance, OperandType::Local, OperandType::Local)                                                \
  F(CountStarAggregateMerge, OperandType::Local, OperandType::Local)                                                  \
  F(CountStarAggregateReset, OperandType::Local)                                                                      \
  F(CountStarAggregateGetResult, OperandType::Local, OperandType::Local)                                              \
  F(CountStarAggregateFree, OperandType::Local)                                                                       \
  F(IntegerSumAggregateInit, OperandType::Local)                                                                      \
  F(IntegerSumAggregateAdvance, OperandType::Local, OperandType::Local)                                               \
  F(IntegerSumAggregateMerge, OperandType::Local, OperandType::Local)                                                 \
  F(IntegerSumAggregateReset, OperandType::Local)                                                                     \
  F(IntegerSumAggregateGetResult, OperandType::Local, OperandType::Local)                                             \
  F(IntegerSumAggregateFree, OperandType::Local)                                                                      \
  F(IntegerMaxAggregateInit, OperandType::Local)                                                                      \
  F(IntegerMaxAggregateAdvance, OperandType::Local, OperandType::Local)                                               \
  F(IntegerMaxAggregateMerge, OperandType::Local, OperandType::Local)                                                 \
  F(IntegerMaxAggregateReset, OperandType::Local)                                                                     \
  F(IntegerMaxAggregateGetResult, OperandType::Local, OperandType::Local)                                             \
  F(IntegerMaxAggregateFree, OperandType::Local)                                                                      \
  F(IntegerMinAggregateInit, OperandType::Local)                                                                      \
  F(IntegerMinAggregateAdvance, OperandType::Local, OperandType::Local)                                               \
  F(IntegerMinAggregateMerge, OperandType::Local, OperandType::Local)                                                 \
  F(IntegerMinAggregateReset, OperandType::Local)                                                                     \
  F(IntegerMinAggregateGetResult, OperandType::Local, OperandType::Local)                                             \
  F(IntegerMinAggregateFree, OperandType::Local)                                                                      \
  F(AvgAggregateInit, OperandType::Local)                                                                             \
  F(IntegerAvgAggregateAdvance, OperandType::Local, OperandType::Local)                                               \
  F(RealAvgAggregateAdvance, OperandType::Local, OperandType::Local)                                                  \
  F(AvgAggregateMerge, OperandType::Local, OperandType::Local)                                                        \
  F(AvgAggregateReset, OperandType::Local)                                                                            \
  F(AvgAggregateGetResult, OperandType::Local, OperandType::Local)                                                    \
  F(AvgAggregateFree, OperandType::Local)                                                                             \
  F(RealSumAggregateInit, OperandType::Local)                                                                         \
  F(RealSumAggregateAdvance, OperandType::Local, OperandType::Local)                                                  \
  F(RealSumAggregateMerge, OperandType::Local, OperandType::Local)                                                    \
  F(RealSumAggregateReset, OperandType::Local)                                                                        \
  F(RealSumAggregateGetResult, OperandType::Local, OperandType::Local)                                                \
  F(RealSumAggregateFree, OperandType::Local)                                                                         \
  F(RealMaxAggregateInit, OperandType::Local)                                                                         \
  F(RealMaxAggregateAdvance, OperandType::Local, OperandType::Local)                                                  \
  F(RealMaxAggregateMerge, OperandType::Local, OperandType::Local)                                                    \
  F(RealMaxAggregateReset, OperandType::Local)                                                                        \
  F(RealMaxAggregateGetResult, OperandType::Local, OperandType::Local)                                                \
  F(RealMaxAggregateFree, OperandType::Local)                                                                         \
  F(RealMinAggregateInit, OperandType::Local)                                                                         \
  F(RealMinAggregateAdvance, OperandType::Local, OperandType::Local)                                                  \
  F(RealMinAggregateMerge, OperandType::Local, OperandType::Local)                                                    \
  F(RealMinAggregateReset, OperandType::Local)                                                                        \
  F(RealMinAggregateGetResult, OperandType::Local, OperandType::Local)                                                \
  F(RealMinAggregateFree, OperandType::Local)                                                                         \
                                                                                                                      \
  /* Hash Joins */                                                                                                    \
  F(JoinHashTableInit, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(JoinHashTableAllocTuple, OperandType::Local, OperandType::Local, OperandType::Local)                              \
  F(JoinHashTableIterInit, OperandType::Local, OperandType::Local, OperandType::Local)                                \
  F(JoinHashTableIterHasNext, OperandType::Local, OperandType::Local, OperandType::FunctionId, OperandType::Local,    \
    OperandType::Local)                                                                                               \
  F(JoinHashTableIterGetRow, OperandType::Local, OperandType::Local)                                                  \
  F(JoinHashTableIterClose, OperandType::Local)                                                                       \
  F(JoinHashTableBuild, OperandType::Local)                                                                           \
  F(JoinHashTableBuildParallel, OperandType::Local, OperandType::Local, OperandType::Local)                           \
  F(JoinHashTableFree, OperandType::Local)                                                                            \
                                                                                                                      \
  /* Sorting */                                                                                                       \
  F(SorterInit, OperandType::Local, OperandType::Local, OperandType::FunctionId, OperandType::Local)                  \
  F(SorterAllocTuple, OperandType::Local, OperandType::Local)                                                         \
  F(SorterAllocTupleTopK, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
  F(SorterAllocTupleTopKFinish, OperandType::Local, OperandType::Local)                                               \
  F(SorterSort, OperandType::Local)                                                                                   \
  F(SorterSortParallel, OperandType::Local, OperandType::Local, OperandType::Local)                                   \
  F(SorterSortTopKParallel, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)           \
  F(SorterFree, OperandType::Local)                                                                                   \
  F(SorterIteratorInit, OperandType::Local, OperandType::Local)                                                       \
  F(SorterIteratorGetRow, OperandType::Local, OperandType::Local)                                                     \
  F(SorterIteratorHasNext, OperandType::Local, OperandType::Local)                                                    \
  F(SorterIteratorNext, OperandType::Local)                                                                           \
  F(SorterIteratorFree, OperandType::Local)                                                                           \
                                                                                                                      \
  /* Output */                                                                                                        \
  F(OutputAlloc, OperandType::Local, OperandType::Local)                                                              \
  F(OutputFinalize, OperandType::Local)                                                                               \
                                                                                                                      \
  /* Index Iterator */                                                                                                \
  F(IndexIteratorInit, OperandType::Local, OperandType::Local, OperandType::UImm4, OperandType::UImm4,                \
    OperandType::UImm4, OperandType::Local, OperandType::UImm4)                                                       \
  F(IndexIteratorPerformInit, OperandType::Local)                                                                     \
  F(IndexIteratorScanKey, OperandType::Local)                                                                         \
  F(IndexIteratorScanAscending, OperandType::Local, OperandType::Local, OperandType::Local)                           \
  F(IndexIteratorScanDescending, OperandType::Local)                                                                  \
  F(IndexIteratorScanLimitDescending, OperandType::Local, OperandType::Local)                                         \
  F(IndexIteratorFree, OperandType::Local)                                                                            \
  F(IndexIteratorAdvance, OperandType::Local, OperandType::Local)                                                     \
  F(IndexIteratorGetPR, OperandType::Local, OperandType::Local)                                                       \
  F(IndexIteratorGetLoPR, OperandType::Local, OperandType::Local)                                                     \
  F(IndexIteratorGetHiPR, OperandType::Local, OperandType::Local)                                                     \
  F(IndexIteratorGetTablePR, OperandType::Local, OperandType::Local)                                                  \
  F(IndexIteratorGetSlot, OperandType::Local, OperandType::Local)                                                     \
                                                                                                                      \
  /* ProjectedRow */                                                                                                  \
  F(PRGetBool, OperandType::Local, OperandType::Local, OperandType::UImm2)                                            \
  F(PRGetTinyInt, OperandType::Local, OperandType::Local, OperandType::UImm2)                                         \
  F(PRGetSmallInt, OperandType::Local, OperandType::Local, OperandType::UImm2)                                        \
  F(PRGetInt, OperandType::Local, OperandType::Local, OperandType::UImm2)                                             \
  F(PRGetBigInt, OperandType::Local, OperandType::Local, OperandType::UImm2)                                          \
  F(PRGetReal, OperandType::Local, OperandType::Local, OperandType::UImm2)                                            \
  F(PRGetDouble, OperandType::Local, OperandType::Local, OperandType::UImm2)                                          \
  F(PRGetDateVal, OperandType::Local, OperandType::Local, OperandType::UImm2)                                         \
  F(PRGetTimestampVal, OperandType::Local, OperandType::Local, OperandType::UImm2)                                    \
  F(PRGetVarlen, OperandType::Local, OperandType::Local, OperandType::UImm2)                                          \
  F(PRGetBoolNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                        \
  F(PRGetTinyIntNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                     \
  F(PRGetSmallIntNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                    \
  F(PRGetIntNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                         \
  F(PRGetBigIntNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                      \
  F(PRGetRealNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                        \
  F(PRGetDoubleNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                      \
  F(PRGetDateValNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                     \
  F(PRGetTimestampValNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                \
  F(PRGetVarlenNull, OperandType::Local, OperandType::Local, OperandType::UImm2)                                      \
  F(PRSetBool, OperandType::Local, OperandType::UImm2, OperandType::Local)                                            \
  F(PRSetTinyInt, OperandType::Local, OperandType::UImm2, OperandType::Local)                                         \
  F(PRSetSmallInt, OperandType::Local, OperandType::UImm2, OperandType::Local)                                        \
  F(PRSetInt, OperandType::Local, OperandType::UImm2, OperandType::Local)                                             \
  F(PRSetBigInt, OperandType::Local, OperandType::UImm2, OperandType::Local)                                          \
  F(PRSetReal, OperandType::Local, OperandType::UImm2, OperandType::Local)                                            \
  F(PRSetDouble, OperandType::Local, OperandType::UImm2, OperandType::Local)                                          \
  F(PRSetDateVal, OperandType::Local, OperandType::UImm2, OperandType::Local)                                         \
  F(PRSetTimestampVal, OperandType::Local, OperandType::UImm2, OperandType::Local)                                    \
  F(PRSetVarlen, OperandType::Local, OperandType::UImm2, OperandType::Local, OperandType::Local)                      \
  F(PRSetBoolNull, OperandType::Local, OperandType::UImm2, OperandType::Local)                                        \
  F(PRSetTinyIntNull, OperandType::Local, OperandType::UImm2, OperandType::Local)                                     \
  F(PRSetSmallIntNull, OperandType::Local, OperandType::UImm2, OperandType::Local)                                    \
  F(PRSetIntNull, OperandType::Local, OperandType::UImm2, OperandType::Local)                                         \
  F(PRSetBigIntNull, OperandType::Local, OperandType::UImm2, OperandType::Local)                                      \
  F(PRSetRealNull, OperandType::Local, OperandType::UImm2, OperandType::Local)                                        \
  F(PRSetDoubleNull, OperandType::Local, OperandType::UImm2, OperandType::Local)                                      \
  F(PRSetDateValNull, OperandType::Local, OperandType::UImm2, OperandType::Local)                                     \
  F(PRSetTimestampValNull, OperandType::Local, OperandType::UImm2, OperandType::Local)                                \
  F(PRSetVarlenNull, OperandType::Local, OperandType::UImm2, OperandType::Local, OperandType::Local)                  \
                                                                                                                      \
  /* StorageInterface */                                                                                              \
  F(StorageInterfaceInit, OperandType::Local, OperandType::Local, OperandType::UImm4, OperandType::Local,             \
    OperandType::UImm4, OperandType::Local)                                                                           \
  F(StorageInterfaceGetTablePR, OperandType::Local, OperandType::Local)                                               \
  F(StorageInterfaceTableUpdate, OperandType::Local, OperandType::Local, OperandType::Local)                          \
  F(StorageInterfaceTableInsert, OperandType::Local, OperandType::Local)                                              \
  F(StorageInterfaceTableDelete, OperandType::Local, OperandType::Local, OperandType::Local)                          \
  F(StorageInterfaceGetIndexPR, OperandType::Local, OperandType::Local, OperandType::UImm4)                           \
  F(StorageInterfaceIndexInsert, OperandType::Local, OperandType::Local)                                              \
  F(StorageInterfaceIndexInsertUnique, OperandType::Local, OperandType::Local)                                        \
  F(StorageInterfaceIndexDelete, OperandType::Local, OperandType::Local)                                              \
  F(StorageInterfaceFree, OperandType::Local)                                                                         \
                                                                                                                      \
  /* Trig functions */                                                                                                \
  F(Pi, OperandType::Local)                                                                                           \
  F(E, OperandType::Local)                                                                                            \
  F(Acos, OperandType::Local, OperandType::Local)                                                                     \
  F(Asin, OperandType::Local, OperandType::Local)                                                                     \
  F(Atan, OperandType::Local, OperandType::Local)                                                                     \
  F(Atan2, OperandType::Local, OperandType::Local, OperandType::Local)                                                \
  F(Cos, OperandType::Local, OperandType::Local)                                                                      \
  F(Cot, OperandType::Local, OperandType::Local)                                                                      \
  F(Sin, OperandType::Local, OperandType::Local)                                                                      \
  F(Tan, OperandType::Local, OperandType::Local)                                                                      \
  F(Cosh, OperandType::Local, OperandType::Local)                                                                     \
  F(Tanh, OperandType::Local, OperandType::Local)                                                                     \
  F(Sinh, OperandType::Local, OperandType::Local)                                                                     \
  F(Sqrt, OperandType::Local, OperandType::Local)                                                                     \
  F(Cbrt, OperandType::Local, OperandType::Local)                                                                     \
  F(Exp, OperandType::Local, OperandType::Local)                                                                      \
  F(Ceil, OperandType::Local, OperandType::Local)                                                                     \
  F(Floor, OperandType::Local, OperandType::Local)                                                                    \
  F(Truncate, OperandType::Local, OperandType::Local)                                                                 \
  F(Ln, OperandType::Local, OperandType::Local)                                                                       \
  F(Log2, OperandType::Local, OperandType::Local)                                                                     \
  F(Log10, OperandType::Local, OperandType::Local)                                                                    \
  F(Sign, OperandType::Local, OperandType::Local)                                                                     \
  F(Radians, OperandType::Local, OperandType::Local)                                                                  \
  F(Degrees, OperandType::Local, OperandType::Local)                                                                  \
  F(Round, OperandType::Local, OperandType::Local)                                                                    \
  F(RoundUpTo, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(Log, OperandType::Local, OperandType::Local, OperandType::Local)                                                  \
  F(Pow, OperandType::Local, OperandType::Local, OperandType::Local)                                                  \
                                                                                                                      \
  /* String functions */                                                                                              \
  F(Left, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  F(Length, OperandType::Local, OperandType::Local, OperandType::Local)                                               \
  F(Lower, OperandType::Local, OperandType::Local, OperandType::Local)                                                \
  F(LPad, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)         \
  F(LTrim, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                            \
  F(Repeat, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                           \
  F(Reverse, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(Right, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                            \
  F(RPad, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)         \
  F(RTrim, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                            \
  F(SplitPart, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)    \
  F(Substring, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)    \
  F(Trim, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  F(Upper, OperandType::Local, OperandType::Local, OperandType::Local)                                                \
  F(NpRunnersEmitInt, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,                 \
    OperandType::Local)                                                                                               \
  F(NpRunnersEmitReal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,                \
    OperandType::Local)                                                                                               \
  F(NpRunnersDummyInt, OperandType::Local)                                                                            \
  F(NpRunnersDummyReal, OperandType::Local)                                                                           \
                                                                                                                      \
  /* String functions */                                                                                              \
  F(GetParamBool, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(GetParamTinyInt, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(GetParamSmallInt, OperandType::Local, OperandType::Local, OperandType::Local)                                     \
  F(GetParamInt, OperandType::Local, OperandType::Local, OperandType::Local)                                          \
  F(GetParamBigInt, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
  F(GetParamReal, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(GetParamDouble, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
  F(GetParamDateVal, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(GetParamTimestampVal, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
  F(GetParamString, OperandType::Local, OperandType::Local, OperandType::Local)

/**
 * The single enumeration of all possible bytecode instructions
 */
enum class Bytecode : uint32_t {
#define DECLARE_OP(inst, ...) inst,
  BYTECODE_LIST(DECLARE_OP)
#undef DECLARE_OP
#define COUNT_OP(inst, ...) +1
      Last = -1 BYTECODE_LIST(COUNT_OP)
#undef COUNT_OP
};

/**
 * Helper class for querying/interacting with bytecode instructions
 */
class Bytecodes {
 public:
  /**
   * The total number of bytecode instructions
   */
  static constexpr const uint32_t K_BYTECODE_COUNT = static_cast<uint32_t>(Bytecode::Last) + 1;

  /**
   * @return total number of bytecode instructions
   */
  static constexpr uint32_t NumBytecodes() { return K_BYTECODE_COUNT; }

  /**
   * @return the maximum length of any bytecode instruction in bytes
   */
  static uint32_t MaxBytecodeNameLength();

  /**
   * @param bytecode bytecode to convert
   * @return the string representation of the given bytecode
   */
  static const char *ToString(Bytecode bytecode) { return k_bytecode_names[static_cast<uint32_t>(bytecode)]; }

  /**
   * @param bytecode bytecode for the number of operands is needed
   * @return the number of operands a bytecode accepts
   */
  static uint32_t NumOperands(Bytecode bytecode) { return k_bytecode_operand_counts[static_cast<uint32_t>(bytecode)]; }

  /**
   * @param bytecode for which the operand types are needed
   * @return an array of the operand types to the given bytecode
   */
  static const OperandType *GetOperandTypes(Bytecode bytecode) {
    return k_bytecode_operand_types[static_cast<uint32_t>(bytecode)];
  }

  /**
   * @param bytecode bytecode for which the operand sizes_ are needed.
   * @return an array of the sizes_ of all operands to the given bytecode
   */
  static const OperandSize *GetOperandSizes(Bytecode bytecode) {
    return k_bytecode_operand_sizes[static_cast<uint32_t>(bytecode)];
  }

  /**
   * Type of the Nth operand
   * @param bytecode bytecode for which the Nth operand type is needed
   * @param operand_index index of the operand
   * @return the type of the operand at the given index
   */
  static OperandType GetNthOperandType(Bytecode bytecode, uint32_t operand_index) {
    TERRIER_ASSERT(operand_index < NumOperands(bytecode), "Accessing out-of-bounds operand number for bytecode");
    return GetOperandTypes(bytecode)[operand_index];
  }

  /**
   * Nth operand size
   * @param bytecode bytecode for which the Nth operand size is needed
   * @param operand_index index of the operand
   * @return the size of the operand at the given index
   */
  static OperandSize GetNthOperandSize(Bytecode bytecode, uint32_t operand_index) {
    TERRIER_ASSERT(operand_index < NumOperands(bytecode), "Accessing out-of-bounds operand number for bytecode");
    return GetOperandSizes(bytecode)[operand_index];
  }

  // Return the offset of the Nth operand of the given bytecode
  /**
   * Nth operand offset
   * @param bytecode bytecode for which the Nth operand offset is needed
   * @param operand_index index of the operand
   * @return the offset of the operand at the given index
   */
  static uint32_t GetNthOperandOffset(Bytecode bytecode, uint32_t operand_index);

  /**
   * @param bytecode bytecode for which the name is needed
   * @return the name of the bytecode handler function for this bytecode
   */
  static const char *GetBytecodeHandlerName(Bytecode bytecode) { return k_bytecode_handler_name[ToByte(bytecode)]; }

  /**
   * Converts the given bytecode to a single-byte representation
   * @param bytecode to convert
   * @return byte representation of the given bytecode
   */
  static constexpr std::underlying_type_t<Bytecode> ToByte(Bytecode bytecode) {
    TERRIER_ASSERT(bytecode <= Bytecode::Last, "Invalid bytecode");
    return static_cast<std::underlying_type_t<Bytecode>>(bytecode);
  }

  // Converts the given unsigned byte into the associated bytecode
  /**
   * Converts the given unsigned byte into the associated bytecode
   * @param val value to convert
   * @return Bytecode representating the given value
   */
  static constexpr Bytecode FromByte(std::underlying_type_t<Bytecode> val) {
    auto bytecode = static_cast<Bytecode>(val);
    TERRIER_ASSERT(bytecode <= Bytecode::Last, "Invalid bytecode");
    return bytecode;
  }

  /**
   * Checks whether the given bytecode is a jump bytecode
   * @param bytecode bytecode to check
   * @return whether the given bytecode is a jump bytecode.
   */
  static constexpr bool IsJump(Bytecode bytecode) {
    return (bytecode == Bytecode::Jump || bytecode == Bytecode::JumpIfFalse || bytecode == Bytecode::JumpIfTrue);
  }

  /**
   * Checks whether the given bytecode is a function call bytecode
   * @param bytecode bytecode to check
   * @return whether the given bytecode is a function call bytecode
   */
  static constexpr bool IsCall(Bytecode bytecode) { return bytecode == Bytecode::Call; }

  /**
   * Checks whether the given bytecode is a terminal (return or unconditional jump) bytecode
   * @param bytecode bytecode to check
   * @return whether the given bytecode is a terminal bytecode.
   */
  static constexpr bool IsTerminal(Bytecode bytecode) {
    return bytecode == Bytecode::Jump || bytecode == Bytecode::Return;
  }

 private:
  static const char *k_bytecode_names[];
  static uint32_t k_bytecode_operand_counts[];
  static const OperandType *k_bytecode_operand_types[];
  static const OperandSize *k_bytecode_operand_sizes[];
  static const char *k_bytecode_handler_name[];
};

}  // namespace terrier::execution::vm
