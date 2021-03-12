#pragma once

#include <algorithm>
#include <cstdint>

#include "common/macros.h"
#include "execution/vm/bytecode_operands.h"

namespace noisepage::execution::vm {

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
  F(op##_float, __VA_ARGS__)               \
  F(op##_double, __VA_ARGS__)

// Creates instances of a given opcode for primitive numeric types
#define CREATE_FOR_NUMERIC_TYPES(F, op, ...) \
  CREATE_FOR_INT_TYPES(F, op, __VA_ARGS__)   \
  CREATE_FOR_FLOAT_TYPES(F, op, __VA_ARGS__)

// Creates instances of a given opcode for *ALL* primitive types
#define CREATE_FOR_ALL_TYPES(F, op, ...)    \
  CREATE_FOR_BOOL_TYPES(F, op, __VA_ARGS__) \
  CREATE_FOR_INT_TYPES(F, op, __VA_ARGS__)  \
  CREATE_FOR_FLOAT_TYPES(F, op, __VA_ARGS__)

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
  CREATE_FOR_NUMERIC_TYPES(F, Mod, OperandType::Local, OperandType::Local, OperandType::Local)                        \
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
  /* Boolean compliment */                                                                                            \
  F(Not, OperandType::Local, OperandType::Local)                                                                      \
  F(NotSql, OperandType::Local, OperandType::Local)                                                                   \
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
  F(ExecutionContextAddRowsAffected, OperandType::Local, OperandType::Local)                                          \
  F(ExecutionContextGetMemoryPool, OperandType::Local, OperandType::Local)                                            \
  F(ExecutionContextGetTLS, OperandType::Local, OperandType::Local)                                                   \
  F(ExecutionContextStartResourceTracker, OperandType::Local, OperandType::Local)                                     \
  F(ExecutionContextEndResourceTracker, OperandType::Local, OperandType::Local)                                       \
  F(ExecutionContextStartPipelineTracker, OperandType::Local, OperandType::Local)                                     \
  F(ExecutionContextEndPipelineTracker, OperandType::Local, OperandType::Local, OperandType::Local,                   \
    OperandType::Local)                                                                                               \
  F(ExecutionContextInitHooks, OperandType::Local, OperandType::Local)                                                \
  F(ExecutionContextRegisterHook, OperandType::Local, OperandType::Local, OperandType::FunctionId)                    \
  F(ExecutionContextClearHooks, OperandType::Local)                                                                   \
  F(ExecOUFeatureVectorRecordFeature, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, \
    OperandType::Local, OperandType::Local)                                                                           \
  F(ExecOUFeatureVectorInitialize, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)    \
  F(ExecOUFeatureVectorFilter, OperandType::Local, OperandType::Local)                                                \
  F(ExecOUFeatureVectorReset, OperandType::Local)                                                                     \
                                                                                                                      \
  F(RegisterThreadWithMetricsManager, OperandType::Local)                                                             \
  F(EnsureTrackersStopped, OperandType::Local)                                                                        \
  F(AggregateMetricsThread, OperandType::Local)                                                                       \
  F(ExecutionContextSetMemoryUseOverride, OperandType::Local, OperandType::Local)                                     \
                                                                                                                      \
  /* Thread State Container */                                                                                        \
  F(ThreadStateContainerIterate, OperandType::Local, OperandType::Local, OperandType::FunctionId)                     \
  F(ThreadStateContainerAccessCurrentThreadState, OperandType::Local, OperandType::Local)                             \
  F(ThreadStateContainerReset, OperandType::Local, OperandType::Local, OperandType::FunctionId,                       \
    OperandType::FunctionId, OperandType::Local)                                                                      \
  F(ThreadStateContainerClear, OperandType::Local)                                                                    \
                                                                                                                      \
  /* Table Vector Iterator */                                                                                         \
  F(TableVectorIteratorInit, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,          \
    OperandType::UImm4)                                                                                               \
  F(TableVectorIteratorPerformInit, OperandType::Local)                                                               \
  F(TableVectorIteratorNext, OperandType::Local, OperandType::Local)                                                  \
  F(TableVectorIteratorFree, OperandType::Local)                                                                      \
  F(TableVectorIteratorGetVPINumTuples, OperandType::Local, OperandType::Local)                                       \
  F(TableVectorIteratorGetVPI, OperandType::Local, OperandType::Local)                                                \
  F(ParallelScanTable, OperandType::Local, OperandType::Local, OperandType::UImm4, OperandType::Local,                \
    OperandType::Local, OperandType::FunctionId)                                                                      \
                                                                                                                      \
  /* Vector Projection Iterator (VPI) */                                                                              \
  F(VPIInit, OperandType::Local, OperandType::Local)                                                                  \
  F(VPIInitWithList, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(VPIFree, OperandType::Local)                                                                                      \
  F(VPIIsFiltered, OperandType::Local, OperandType::Local)                                                            \
  F(VPIGetSelectedRowCount, OperandType::Local, OperandType::Local)                                                   \
  F(VPIGetVectorProjection, OperandType::Local, OperandType::Local)                                                   \
  F(VPIHasNext, OperandType::Local, OperandType::Local)                                                               \
  F(VPIHasNextFiltered, OperandType::Local, OperandType::Local)                                                       \
  F(VPIAdvance, OperandType::Local)                                                                                   \
  F(VPIAdvanceFiltered, OperandType::Local)                                                                           \
  F(VPISetPosition, OperandType::Local, OperandType::Local)                                                           \
  F(VPISetPositionFiltered, OperandType::Local, OperandType::Local)                                                   \
  F(VPIMatch, OperandType::Local, OperandType::Local)                                                                 \
  F(VPIReset, OperandType::Local)                                                                                     \
  F(VPIResetFiltered, OperandType::Local)                                                                             \
  F(VPIGetSlot, OperandType::Local, OperandType::Local)                                                               \
  F(VPIGetBool, OperandType::Local, OperandType::Local, OperandType::UImm4)                                           \
  F(VPIGetTinyInt, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPIGetSmallInt, OperandType::Local, OperandType::Local, OperandType::UImm4)                                       \
  F(VPIGetInteger, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPIGetBigInt, OperandType::Local, OperandType::Local, OperandType::UImm4)                                         \
  F(VPIGetReal, OperandType::Local, OperandType::Local, OperandType::UImm4)                                           \
  F(VPIGetDouble, OperandType::Local, OperandType::Local, OperandType::UImm4)                                         \
  F(VPIGetDecimal, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPIGetDate, OperandType::Local, OperandType::Local, OperandType::UImm4)                                           \
  F(VPIGetTimestamp, OperandType::Local, OperandType::Local, OperandType::UImm4)                                      \
  F(VPIGetString, OperandType::Local, OperandType::Local, OperandType::UImm4)                                         \
  F(VPIGetPointer, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPIGetBoolNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                       \
  F(VPIGetTinyIntNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                    \
  F(VPIGetSmallIntNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                   \
  F(VPIGetIntegerNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                    \
  F(VPIGetBigIntNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                     \
  F(VPIGetRealNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                       \
  F(VPIGetDoubleNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                     \
  F(VPIGetDecimalNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                    \
  F(VPIGetDateNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                       \
  F(VPIGetTimestampNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                  \
  F(VPIGetStringNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                     \
  F(VPISetBool, OperandType::Local, OperandType::Local, OperandType::UImm4)                                           \
  F(VPISetTinyInt, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPISetSmallInt, OperandType::Local, OperandType::Local, OperandType::UImm4)                                       \
  F(VPISetInteger, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPISetBigInt, OperandType::Local, OperandType::Local, OperandType::UImm4)                                         \
  F(VPISetReal, OperandType::Local, OperandType::Local, OperandType::UImm4)                                           \
  F(VPISetDouble, OperandType::Local, OperandType::Local, OperandType::UImm4)                                         \
  F(VPISetDecimal, OperandType::Local, OperandType::Local, OperandType::UImm4)                                        \
  F(VPISetDate, OperandType::Local, OperandType::Local, OperandType::UImm4)                                           \
  F(VPISetTimestamp, OperandType::Local, OperandType::Local, OperandType::UImm4)                                      \
  F(VPISetString, OperandType::Local, OperandType::Local, OperandType::UImm4)                                         \
  F(VPISetBoolNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                       \
  F(VPISetTinyIntNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                    \
  F(VPISetSmallIntNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                   \
  F(VPISetIntegerNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                    \
  F(VPISetBigIntNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                     \
  F(VPISetRealNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                       \
  F(VPISetDoubleNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                     \
  F(VPISetDecimalNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                    \
  F(VPISetDateNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                       \
  F(VPISetTimestampNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                  \
  F(VPISetStringNull, OperandType::Local, OperandType::Local, OperandType::UImm4)                                     \
                                                                                                                      \
  /* Filter Manager */                                                                                                \
  F(FilterManagerInit, OperandType::Local, OperandType::Local)                                                        \
  F(FilterManagerStartNewClause, OperandType::Local)                                                                  \
  F(FilterManagerInsertFilter, OperandType::Local, OperandType::FunctionId)                                           \
  F(FilterManagerRunFilters, OperandType::Local, OperandType::Local, OperandType::Local)                              \
  F(FilterManagerFree, OperandType::Local)                                                                            \
                                                                                                                      \
  /* Vector Filter Executor */                                                                                        \
  F(VectorFilterEqual, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,                \
    OperandType::Local)                                                                                               \
  F(VectorFilterEqualVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,             \
    OperandType::Local)                                                                                               \
  F(VectorFilterGreaterThan, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,          \
    OperandType::Local)                                                                                               \
  F(VectorFilterGreaterThanVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,       \
    OperandType::Local)                                                                                               \
  F(VectorFilterGreaterThanEqual, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,     \
    OperandType::Local)                                                                                               \
  F(VectorFilterGreaterThanEqualVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,  \
    OperandType::Local)                                                                                               \
  F(VectorFilterLessThan, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,             \
    OperandType::Local)                                                                                               \
  F(VectorFilterLessThanVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,          \
    OperandType::Local)                                                                                               \
  F(VectorFilterLessThanEqual, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,        \
    OperandType::Local)                                                                                               \
  F(VectorFilterLessThanEqualVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,     \
    OperandType::Local)                                                                                               \
  F(VectorFilterNotEqual, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,             \
    OperandType::Local)                                                                                               \
  F(VectorFilterNotEqualVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,          \
    OperandType::Local)                                                                                               \
  F(VectorFilterLike, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,                 \
    OperandType::Local)                                                                                               \
  F(VectorFilterLikeVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,              \
    OperandType::Local)                                                                                               \
  F(VectorFilterNotLike, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,              \
    OperandType::Local)                                                                                               \
  F(VectorFilterNotLikeVal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,           \
    OperandType::Local)                                                                                               \
                                                                                                                      \
  /* SQL value creation */                                                                                            \
  F(ForceBoolTruth, OperandType::Local, OperandType::Local)                                                           \
  F(InitSqlNull, OperandType::Local)                                                                                  \
  F(InitBool, OperandType::Local, OperandType::Local)                                                                 \
  F(InitInteger, OperandType::Local, OperandType::Local)                                                              \
  F(InitInteger64, OperandType::Local, OperandType::Local)                                                            \
  F(InitReal, OperandType::Local, OperandType::Local)                                                                 \
  F(InitDate, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                         \
  F(InitTimestamp, OperandType::Local, OperandType::Local)                                                            \
  F(InitTimestampYMDHMSMU, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,            \
    OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)               \
  F(InitString, OperandType::Local, OperandType::StaticLocal, OperandType::UImm4)                                     \
  /* SQL value conversion */                                                                                          \
  F(BoolToInteger, OperandType::Local, OperandType::Local)                                                            \
  F(IntegerToBool, OperandType::Local, OperandType::Local)                                                            \
  F(IntegerToReal, OperandType::Local, OperandType::Local)                                                            \
  F(IntegerToString, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(RealToBool, OperandType::Local, OperandType::Local)                                                               \
  F(RealToInteger, OperandType::Local, OperandType::Local)                                                            \
  F(RealToString, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(DateToTimestamp, OperandType::Local, OperandType::Local)                                                          \
  F(DateToString, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(TimestampToDate, OperandType::Local, OperandType::Local)                                                          \
  F(TimestampToString, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(StringToBool, OperandType::Local, OperandType::Local)                                                             \
  F(StringToInteger, OperandType::Local, OperandType::Local)                                                          \
  F(StringToReal, OperandType::Local, OperandType::Local)                                                             \
  F(StringToDate, OperandType::Local, OperandType::Local)                                                             \
  F(StringToTimestamp, OperandType::Local, OperandType::Local)                                                        \
  /* SQL value comparisons */                                                                                         \
  F(LessThanBool, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(LessThanEqualBool, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(GreaterThanBool, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(GreaterThanEqualBool, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
  F(EqualBool, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(NotEqualBool, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
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
  F(LessThanString, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
  F(LessThanEqualString, OperandType::Local, OperandType::Local, OperandType::Local)                                  \
  F(GreaterThanString, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(GreaterThanEqualString, OperandType::Local, OperandType::Local, OperandType::Local)                               \
  F(EqualString, OperandType::Local, OperandType::Local, OperandType::Local)                                          \
  F(NotEqualString, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
  F(LessThanDate, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(LessThanEqualDate, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(GreaterThanDate, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(GreaterThanEqualDate, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
  F(EqualDate, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(NotEqualDate, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(LessThanTimestamp, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(LessThanEqualTimestamp, OperandType::Local, OperandType::Local, OperandType::Local)                               \
  F(GreaterThanTimestamp, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
  F(GreaterThanEqualTimestamp, OperandType::Local, OperandType::Local, OperandType::Local)                            \
  F(EqualTimestamp, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
  F(NotEqualTimestamp, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
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
  F(ModInteger, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(AddReal, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(SubReal, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(MulReal, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(DivReal, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(ModReal, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
                                                                                                                      \
  /* Hashing */                                                                                                       \
  F(HashInt, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(HashBool, OperandType::Local, OperandType::Local, OperandType::Local)                                             \
  F(HashReal, OperandType::Local, OperandType::Local, OperandType::Local)                                             \
  F(HashDate, OperandType::Local, OperandType::Local, OperandType::Local)                                             \
  F(HashTimestamp, OperandType::Local, OperandType::Local, OperandType::Local)                                        \
  F(HashString, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(HashCombine, OperandType::Local, OperandType::Local)                                                              \
                                                                                                                      \
  /* Aggregation Hash Table */                                                                                        \
  F(AggregationHashTableInit, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  F(AggregationHashTableGetTupleCount, OperandType::Local, OperandType::Local)                                        \
  F(AggregationHashTableGetInsertCount, OperandType::Local, OperandType::Local)                                       \
  F(AggregationHashTableAllocTuple, OperandType::Local, OperandType::Local, OperandType::Local)                       \
  F(AggregationHashTableAllocTuplePartitioned, OperandType::Local, OperandType::Local, OperandType::Local)            \
  F(AggregationHashTableLinkHashTableEntry, OperandType::Local, OperandType::Local)                                   \
  F(AggregationHashTableLookup, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::FunctionId,  \
    OperandType::Local)                                                                                               \
  F(AggregationHashTableProcessBatch, OperandType::Local, OperandType::Local, OperandType::UImm4, OperandType::Local, \
    OperandType::FunctionId, OperandType::FunctionId, OperandType::Local)                                             \
  F(AggregationHashTableTransferPartitions, OperandType::Local, OperandType::Local, OperandType::Local,               \
    OperandType::FunctionId)                                                                                          \
  F(AggregationHashTableBuildAllHashTablePartitions, OperandType::Local, OperandType::Local)                          \
  F(AggregationHashTableRepartition, OperandType::Local)                                                              \
  F(AggregationHashTableMergePartitions, OperandType::Local, OperandType::Local, OperandType::Local,                  \
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
  F(AggregationOverflowPartitionIteratorGetRowEntry, OperandType::Local, OperandType::Local)                          \
  /* COUNT Aggregates */                                                                                              \
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
  /* SUM Aggregates */                                                                                                \
  F(IntegerSumAggregateInit, OperandType::Local)                                                                      \
  F(IntegerSumAggregateAdvance, OperandType::Local, OperandType::Local)                                               \
  F(IntegerSumAggregateMerge, OperandType::Local, OperandType::Local)                                                 \
  F(IntegerSumAggregateReset, OperandType::Local)                                                                     \
  F(IntegerSumAggregateGetResult, OperandType::Local, OperandType::Local)                                             \
  F(IntegerSumAggregateFree, OperandType::Local)                                                                      \
  F(RealSumAggregateInit, OperandType::Local)                                                                         \
  F(RealSumAggregateAdvance, OperandType::Local, OperandType::Local)                                                  \
  F(RealSumAggregateMerge, OperandType::Local, OperandType::Local)                                                    \
  F(RealSumAggregateReset, OperandType::Local)                                                                        \
  F(RealSumAggregateGetResult, OperandType::Local, OperandType::Local)                                                \
  F(RealSumAggregateFree, OperandType::Local)                                                                         \
  /* MAX Aggregates */                                                                                                \
  F(IntegerMaxAggregateInit, OperandType::Local)                                                                      \
  F(IntegerMaxAggregateAdvance, OperandType::Local, OperandType::Local)                                               \
  F(IntegerMaxAggregateMerge, OperandType::Local, OperandType::Local)                                                 \
  F(IntegerMaxAggregateReset, OperandType::Local)                                                                     \
  F(IntegerMaxAggregateGetResult, OperandType::Local, OperandType::Local)                                             \
  F(IntegerMaxAggregateFree, OperandType::Local)                                                                      \
  F(RealMaxAggregateInit, OperandType::Local)                                                                         \
  F(RealMaxAggregateAdvance, OperandType::Local, OperandType::Local)                                                  \
  F(RealMaxAggregateMerge, OperandType::Local, OperandType::Local)                                                    \
  F(RealMaxAggregateReset, OperandType::Local)                                                                        \
  F(RealMaxAggregateGetResult, OperandType::Local, OperandType::Local)                                                \
  F(RealMaxAggregateFree, OperandType::Local)                                                                         \
  F(DateMaxAggregateInit, OperandType::Local)                                                                         \
  F(DateMaxAggregateAdvance, OperandType::Local, OperandType::Local)                                                  \
  F(DateMaxAggregateMerge, OperandType::Local, OperandType::Local)                                                    \
  F(DateMaxAggregateReset, OperandType::Local)                                                                        \
  F(DateMaxAggregateGetResult, OperandType::Local, OperandType::Local)                                                \
  F(DateMaxAggregateFree, OperandType::Local)                                                                         \
  F(StringMaxAggregateInit, OperandType::Local)                                                                       \
  F(StringMaxAggregateAdvance, OperandType::Local, OperandType::Local)                                                \
  F(StringMaxAggregateMerge, OperandType::Local, OperandType::Local)                                                  \
  F(StringMaxAggregateReset, OperandType::Local)                                                                      \
  F(StringMaxAggregateGetResult, OperandType::Local, OperandType::Local)                                              \
  F(StringMaxAggregateFree, OperandType::Local)                                                                       \
  /* MIN Aggregates */                                                                                                \
  F(IntegerMinAggregateInit, OperandType::Local)                                                                      \
  F(IntegerMinAggregateAdvance, OperandType::Local, OperandType::Local)                                               \
  F(IntegerMinAggregateMerge, OperandType::Local, OperandType::Local)                                                 \
  F(IntegerMinAggregateReset, OperandType::Local)                                                                     \
  F(IntegerMinAggregateGetResult, OperandType::Local, OperandType::Local)                                             \
  F(IntegerMinAggregateFree, OperandType::Local)                                                                      \
  F(RealMinAggregateInit, OperandType::Local)                                                                         \
  F(RealMinAggregateAdvance, OperandType::Local, OperandType::Local)                                                  \
  F(RealMinAggregateMerge, OperandType::Local, OperandType::Local)                                                    \
  F(RealMinAggregateReset, OperandType::Local)                                                                        \
  F(RealMinAggregateGetResult, OperandType::Local, OperandType::Local)                                                \
  F(RealMinAggregateFree, OperandType::Local)                                                                         \
  F(DateMinAggregateInit, OperandType::Local)                                                                         \
  F(DateMinAggregateAdvance, OperandType::Local, OperandType::Local)                                                  \
  F(DateMinAggregateMerge, OperandType::Local, OperandType::Local)                                                    \
  F(DateMinAggregateReset, OperandType::Local)                                                                        \
  F(DateMinAggregateGetResult, OperandType::Local, OperandType::Local)                                                \
  F(DateMinAggregateFree, OperandType::Local)                                                                         \
  F(StringMinAggregateInit, OperandType::Local)                                                                       \
  F(StringMinAggregateAdvance, OperandType::Local, OperandType::Local)                                                \
  F(StringMinAggregateMerge, OperandType::Local, OperandType::Local)                                                  \
  F(StringMinAggregateReset, OperandType::Local)                                                                      \
  F(StringMinAggregateGetResult, OperandType::Local, OperandType::Local)                                              \
  F(StringMinAggregateFree, OperandType::Local)                                                                       \
  /* AVG Aggregates */                                                                                                \
  F(AvgAggregateInit, OperandType::Local)                                                                             \
  F(AvgAggregateAdvanceInteger, OperandType::Local, OperandType::Local)                                               \
  F(AvgAggregateAdvanceReal, OperandType::Local, OperandType::Local)                                                  \
  F(AvgAggregateMerge, OperandType::Local, OperandType::Local)                                                        \
  F(AvgAggregateReset, OperandType::Local)                                                                            \
  F(AvgAggregateGetResult, OperandType::Local, OperandType::Local)                                                    \
  F(AvgAggregateFree, OperandType::Local)                                                                             \
  /* Top K Aggregates */                                                                                              \
  F(BooleanTopKAggregateInit, OperandType::Local)                                                                     \
  F(BooleanTopKAggregateAdvance, OperandType::Local, OperandType::Local)                                              \
  F(BooleanTopKAggregateMerge, OperandType::Local, OperandType::Local)                                                \
  F(BooleanTopKAggregateReset, OperandType::Local)                                                                    \
  F(BooleanTopKAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  F(BooleanTopKAggregateFree, OperandType::Local)                                                                     \
  F(IntegerTopKAggregateInit, OperandType::Local)                                                                     \
  F(IntegerTopKAggregateAdvance, OperandType::Local, OperandType::Local)                                              \
  F(IntegerTopKAggregateMerge, OperandType::Local, OperandType::Local)                                                \
  F(IntegerTopKAggregateReset, OperandType::Local)                                                                    \
  F(IntegerTopKAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  F(IntegerTopKAggregateFree, OperandType::Local)                                                                     \
  F(RealTopKAggregateInit, OperandType::Local)                                                                        \
  F(RealTopKAggregateAdvance, OperandType::Local, OperandType::Local)                                                 \
  F(RealTopKAggregateMerge, OperandType::Local, OperandType::Local)                                                   \
  F(RealTopKAggregateReset, OperandType::Local)                                                                       \
  F(RealTopKAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                           \
  F(RealTopKAggregateFree, OperandType::Local)                                                                        \
  F(DecimalTopKAggregateInit, OperandType::Local)                                                                     \
  F(DecimalTopKAggregateAdvance, OperandType::Local, OperandType::Local)                                              \
  F(DecimalTopKAggregateMerge, OperandType::Local, OperandType::Local)                                                \
  F(DecimalTopKAggregateReset, OperandType::Local)                                                                    \
  F(DecimalTopKAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  F(DecimalTopKAggregateFree, OperandType::Local)                                                                     \
  F(StringTopKAggregateInit, OperandType::Local)                                                                      \
  F(StringTopKAggregateAdvance, OperandType::Local, OperandType::Local)                                               \
  F(StringTopKAggregateMerge, OperandType::Local, OperandType::Local)                                                 \
  F(StringTopKAggregateReset, OperandType::Local)                                                                     \
  F(StringTopKAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                         \
  F(StringTopKAggregateFree, OperandType::Local)                                                                      \
  F(DateTopKAggregateInit, OperandType::Local)                                                                        \
  F(DateTopKAggregateAdvance, OperandType::Local, OperandType::Local)                                                 \
  F(DateTopKAggregateMerge, OperandType::Local, OperandType::Local)                                                   \
  F(DateTopKAggregateReset, OperandType::Local)                                                                       \
  F(DateTopKAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                           \
  F(DateTopKAggregateFree, OperandType::Local)                                                                        \
  F(TimestampTopKAggregateInit, OperandType::Local)                                                                   \
  F(TimestampTopKAggregateAdvance, OperandType::Local, OperandType::Local)                                            \
  F(TimestampTopKAggregateMerge, OperandType::Local, OperandType::Local)                                              \
  F(TimestampTopKAggregateReset, OperandType::Local)                                                                  \
  F(TimestampTopKAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                      \
  F(TimestampTopKAggregateFree, OperandType::Local)                                                                   \
  /* Histogram Aggregates */                                                                                          \
  F(BooleanHistogramAggregateInit, OperandType::Local)                                                                \
  F(BooleanHistogramAggregateAdvance, OperandType::Local, OperandType::Local)                                         \
  F(BooleanHistogramAggregateMerge, OperandType::Local, OperandType::Local)                                           \
  F(BooleanHistogramAggregateReset, OperandType::Local)                                                               \
  F(BooleanHistogramAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                   \
  F(BooleanHistogramAggregateFree, OperandType::Local)                                                                \
  F(IntegerHistogramAggregateInit, OperandType::Local)                                                                \
  F(IntegerHistogramAggregateAdvance, OperandType::Local, OperandType::Local)                                         \
  F(IntegerHistogramAggregateMerge, OperandType::Local, OperandType::Local)                                           \
  F(IntegerHistogramAggregateReset, OperandType::Local)                                                               \
  F(IntegerHistogramAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                   \
  F(IntegerHistogramAggregateFree, OperandType::Local)                                                                \
  F(RealHistogramAggregateInit, OperandType::Local)                                                                   \
  F(RealHistogramAggregateAdvance, OperandType::Local, OperandType::Local)                                            \
  F(RealHistogramAggregateMerge, OperandType::Local, OperandType::Local)                                              \
  F(RealHistogramAggregateReset, OperandType::Local)                                                                  \
  F(RealHistogramAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                      \
  F(RealHistogramAggregateFree, OperandType::Local)                                                                   \
  F(DecimalHistogramAggregateInit, OperandType::Local)                                                                \
  F(DecimalHistogramAggregateAdvance, OperandType::Local, OperandType::Local)                                         \
  F(DecimalHistogramAggregateMerge, OperandType::Local, OperandType::Local)                                           \
  F(DecimalHistogramAggregateReset, OperandType::Local)                                                               \
  F(DecimalHistogramAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                   \
  F(DecimalHistogramAggregateFree, OperandType::Local)                                                                \
  F(StringHistogramAggregateInit, OperandType::Local)                                                                 \
  F(StringHistogramAggregateAdvance, OperandType::Local, OperandType::Local)                                          \
  F(StringHistogramAggregateMerge, OperandType::Local, OperandType::Local)                                            \
  F(StringHistogramAggregateReset, OperandType::Local)                                                                \
  F(StringHistogramAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                    \
  F(StringHistogramAggregateFree, OperandType::Local)                                                                 \
  F(DateHistogramAggregateInit, OperandType::Local)                                                                   \
  F(DateHistogramAggregateAdvance, OperandType::Local, OperandType::Local)                                            \
  F(DateHistogramAggregateMerge, OperandType::Local, OperandType::Local)                                              \
  F(DateHistogramAggregateReset, OperandType::Local)                                                                  \
  F(DateHistogramAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                      \
  F(DateHistogramAggregateFree, OperandType::Local)                                                                   \
  F(TimestampHistogramAggregateInit, OperandType::Local)                                                              \
  F(TimestampHistogramAggregateAdvance, OperandType::Local, OperandType::Local)                                       \
  F(TimestampHistogramAggregateMerge, OperandType::Local, OperandType::Local)                                         \
  F(TimestampHistogramAggregateReset, OperandType::Local)                                                             \
  F(TimestampHistogramAggregateGetResult, OperandType::Local, OperandType::Local, OperandType::Local)                 \
  F(TimestampHistogramAggregateFree, OperandType::Local)                                                              \
                                                                                                                      \
  /* Hash Joins */                                                                                                    \
  F(JoinHashTableInit, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(JoinHashTableAllocTuple, OperandType::Local, OperandType::Local, OperandType::Local)                              \
  F(JoinHashTableGetTupleCount, OperandType::Local, OperandType::Local)                                               \
  F(JoinHashTableBuild, OperandType::Local)                                                                           \
  F(JoinHashTableBuildParallel, OperandType::Local, OperandType::Local, OperandType::Local)                           \
  F(JoinHashTableLookup, OperandType::Local, OperandType::Local, OperandType::Local)                                  \
  F(JoinHashTableFree, OperandType::Local)                                                                            \
  F(HashTableEntryIteratorHasNext, OperandType::Local, OperandType::Local)                                            \
  F(HashTableEntryIteratorGetRow, OperandType::Local, OperandType::Local)                                             \
  F(JoinHashTableIteratorInit, OperandType::Local, OperandType::Local)                                                \
  F(JoinHashTableIteratorHasNext, OperandType::Local, OperandType::Local)                                             \
  F(JoinHashTableIteratorNext, OperandType::Local)                                                                    \
  F(JoinHashTableIteratorGetRow, OperandType::Local, OperandType::Local)                                              \
  F(JoinHashTableIteratorFree, OperandType::Local)                                                                    \
                                                                                                                      \
  /* Sorting */                                                                                                       \
  F(SorterInit, OperandType::Local, OperandType::Local, OperandType::FunctionId, OperandType::Local)                  \
  F(SorterGetTupleCount, OperandType::Local, OperandType::Local)                                                      \
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
  F(SorterIteratorSkipRows, OperandType::Local, OperandType::Local)                                                   \
  F(SorterIteratorFree, OperandType::Local)                                                                           \
                                                                                                                      \
  /* Output */                                                                                                        \
  F(ResultBufferNew, OperandType::Local, OperandType::Local)                                                          \
  F(ResultBufferAllocOutputRow, OperandType::Local, OperandType::Local)                                               \
  F(ResultBufferFinalize, OperandType::Local)                                                                         \
  F(ResultBufferFree, OperandType::Local)                                                                             \
                                                                                                                      \
  /* Index Iterator */                                                                                                \
  F(IndexIteratorInit, OperandType::Local, OperandType::Local, OperandType::UImm4, OperandType::Local,                \
    OperandType::Local, OperandType::Local, OperandType::UImm4)                                                       \
  F(IndexIteratorGetSize, OperandType::Local, OperandType::Local)                                                     \
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
  /* CSV Reader */                                                                                                    \
  /*                                                                                                                  \
  F(CSVReaderInit, OperandType::Local, OperandType::StaticLocal, OperandType::UImm4)                                  \
  F(CSVReaderPerformInit, OperandType::Local, OperandType::Local)                                                     \
  F(CSVReaderAdvance, OperandType::Local, OperandType::Local)                                                         \
  F(CSVReaderGetField, OperandType::Local, OperandType::Local, OperandType::Local)                                    \
  F(CSVReaderGetRecordNumber, OperandType::Local, OperandType::Local)                                                 \
  F(CSVReaderClose, OperandType::Local)                                                                               \
  */                                                                                                                  \
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
  F(StorageInterfaceInit, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,             \
    OperandType::UImm4, OperandType::Local)                                                                           \
  F(StorageInterfaceGetTablePR, OperandType::Local, OperandType::Local)                                               \
  F(StorageInterfaceTableUpdate, OperandType::Local, OperandType::Local, OperandType::Local)                          \
  F(StorageInterfaceTableInsert, OperandType::Local, OperandType::Local)                                              \
  F(StorageInterfaceTableDelete, OperandType::Local, OperandType::Local, OperandType::Local)                          \
  F(StorageInterfaceGetIndexHeapSize, OperandType::Local, OperandType::Local)                                         \
  F(StorageInterfaceGetIndexPR, OperandType::Local, OperandType::Local, OperandType::Local)                           \
  F(StorageInterfaceIndexGetSize, OperandType::Local, OperandType::Local)                                             \
  F(StorageInterfaceIndexInsert, OperandType::Local, OperandType::Local)                                              \
  F(StorageInterfaceIndexInsertUnique, OperandType::Local, OperandType::Local)                                        \
  F(StorageInterfaceIndexInsertWithSlot, OperandType::Local, OperandType::Local, OperandType::Local,                  \
    OperandType::Local)                                                                                               \
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
  F(Round2, OperandType::Local, OperandType::Local, OperandType::Local)                                               \
  F(Log, OperandType::Local, OperandType::Local, OperandType::Local)                                                  \
  F(Pow, OperandType::Local, OperandType::Local, OperandType::Local)                                                  \
                                                                                                                      \
  /* Atomic functions */                                                                                              \
  F(AtomicAnd1, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(AtomicAnd2, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(AtomicAnd4, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(AtomicAnd8, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(AtomicOr1, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(AtomicOr2, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(AtomicOr4, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(AtomicOr8, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(AtomicCompareExchange1, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)           \
  F(AtomicCompareExchange2, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)           \
  F(AtomicCompareExchange4, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)           \
  F(AtomicCompareExchange8, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)           \
                                                                                                                      \
  /* String functions */                                                                                              \
  F(Chr, OperandType::Local, OperandType::Local, OperandType::Local)                                                  \
  F(CharLength, OperandType::Local, OperandType::Local, OperandType::Local)                                           \
  F(ASCII, OperandType::Local, OperandType::Local, OperandType::Local)                                                \
  F(Concat, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::UImm4)                           \
  F(Left, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                             \
  F(Length, OperandType::Local, OperandType::Local, OperandType::Local)                                               \
  F(Like, OperandType::Local, OperandType::Local, OperandType::Local)                                                 \
  F(Lower, OperandType::Local, OperandType::Local, OperandType::Local)                                                \
  F(LPad3Arg, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)     \
  F(LPad2Arg, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                         \
  F(LTrim2Arg, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  F(LTrim1Arg, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(Repeat, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                           \
  F(Reverse, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
  F(Right, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                            \
  F(RPad3Arg, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)     \
  F(RPad2Arg, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                         \
  F(RTrim2Arg, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                        \
  F(RTrim1Arg, OperandType::Local, OperandType::Local, OperandType::Local)                                            \
  F(SplitPart, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)    \
  F(Substring, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)    \
  F(Trim, OperandType::Local, OperandType::Local, OperandType::Local)                                                 \
  F(Trim2, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                            \
  F(Upper, OperandType::Local, OperandType::Local, OperandType::Local)                                                \
  F(StartsWith, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                       \
  F(Position, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local)                         \
  F(InitCap, OperandType::Local, OperandType::Local, OperandType::Local)                                              \
                                                                                                                      \
  /* Date Functions */                                                                                                \
  F(ExtractYearFromDate, OperandType::Local, OperandType::Local)                                                      \
                                                                                                                      \
  F(AbortTxn, OperandType::Local)                                                                                     \
                                                                                                                      \
  /* Mini-runners. */                                                                                                 \
  F(NpRunnersEmitInt, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,                 \
    OperandType::Local)                                                                                               \
  F(NpRunnersEmitReal, OperandType::Local, OperandType::Local, OperandType::Local, OperandType::Local,                \
    OperandType::Local)                                                                                               \
  F(NpRunnersDummyInt, OperandType::Local)                                                                            \
  F(NpRunnersDummyReal, OperandType::Local)                                                                           \
                                                                                                                      \
  /* Replication. */                                                                                                  \
  F(ReplicationGetLastRecordId, OperandType::Local, OperandType::Local)                                               \
                                                                                                                      \
  /* Miscellaneous functions. */                                                                                      \
  F(Version, OperandType::Local, OperandType::Local)                                                                  \
                                                                                                                      \
  /* Parameter support. */                                                                                            \
  F(GetParamBool, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(GetParamTinyInt, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(GetParamSmallInt, OperandType::Local, OperandType::Local, OperandType::Local)                                     \
  F(GetParamInt, OperandType::Local, OperandType::Local, OperandType::Local)                                          \
  F(GetParamBigInt, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
  F(GetParamReal, OperandType::Local, OperandType::Local, OperandType::Local)                                         \
  F(GetParamDouble, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
  F(GetParamDateVal, OperandType::Local, OperandType::Local, OperandType::Local)                                      \
  F(GetParamTimestampVal, OperandType::Local, OperandType::Local, OperandType::Local)                                 \
  F(GetParamString, OperandType::Local, OperandType::Local, OperandType::Local)                                       \
                                                                                                                      \
  /* FOR TESTING ONLY */                                                                                              \
  F(TestCatalogLookup, OperandType::Local, OperandType::Local, OperandType::StaticLocal, OperandType::UImm4,          \
    OperandType::StaticLocal, OperandType::UImm4)                                                                     \
  F(TestCatalogIndexLookup, OperandType::Local, OperandType::Local, OperandType::StaticLocal, OperandType::UImm4)

/**
 * The enumeration listing all possible bytecode instructions.
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
 * Helper class for querying/interacting with bytecode instructions.
 */
class Bytecodes {
 public:
  /** The total number of bytecode instructions. */
  static constexpr const uint32_t BYTECODE_COUNT = static_cast<uint32_t>(Bytecode::Last) + 1;

  /**
   * @return The total number of bytecodes.
   */
  static constexpr uint32_t NumBytecodes() { return BYTECODE_COUNT; }

  /**
   * @return The maximum length of any bytecode instruction in bytes.
   */
  static uint32_t MaxBytecodeNameLength();

  /**
   * @return The string representation of the given bytecode.
   */
  static const char *ToString(Bytecode bytecode) { return bytecode_names[static_cast<uint32_t>(bytecode)]; }

  /**
   * @return The number of operands a bytecode accepts.
   */
  static uint32_t NumOperands(Bytecode bytecode) { return bytecode_operand_counts[static_cast<uint32_t>(bytecode)]; }

  /**
   * @return An array of the operand types to the given bytecode.
   */
  static const OperandType *GetOperandTypes(Bytecode bytecode) {
    return bytecode_operand_types[static_cast<uint32_t>(bytecode)];
  }

  /**
   * @return An array containing the sizes of all operands to the given bytecode.
   */
  static const OperandSize *GetOperandSizes(Bytecode bytecode) {
    return bytecode_operand_sizes[static_cast<uint32_t>(bytecode)];
  }

  /**
   * @return The type of the Nth operand to the given bytecode.
   */
  static OperandType GetNthOperandType(Bytecode bytecode, uint32_t operand_index) {
    NOISEPAGE_ASSERT(operand_index < NumOperands(bytecode), "Accessing out-of-bounds operand number for bytecode");
    return GetOperandTypes(bytecode)[operand_index];
  }

  /**
   * @return The size of the Nth operand to the given bytecode.
   */
  static OperandSize GetNthOperandSize(Bytecode bytecode, uint32_t operand_index) {
    NOISEPAGE_ASSERT(operand_index < NumOperands(bytecode), "Accessing out-of-bounds operand number for bytecode");
    return GetOperandSizes(bytecode)[operand_index];
  }

  /**
   * @return The offset of the Nth operand of the given bytecode.
   */
  static uint32_t GetNthOperandOffset(Bytecode bytecode, uint32_t operand_index);

  /**
   * @return The name of the bytecode handler function for the given bytecode.
   */
  static const char *GetBytecodeHandlerName(Bytecode bytecode) { return bytecode_handler_name[ToByte(bytecode)]; }

  /**
   * Converts the bytecode instruction @em bytecode into a raw encoded value.
   * @param bytecode The bytecode to convert.
   * @return The raw encoded value for the input bytecode instruction.
   */
  static constexpr std::underlying_type_t<Bytecode> ToByte(Bytecode bytecode) {
    NOISEPAGE_ASSERT(bytecode <= Bytecode::Last, "Invalid bytecode");
    return static_cast<std::underlying_type_t<Bytecode>>(bytecode);
  }

  /**
   * Decode and convert the raw value @em val into a bytecode instruction.
   * @param val The value to convert.
   * @return The bytecode associated with the given value.
   */
  static constexpr Bytecode FromByte(std::underlying_type_t<Bytecode> val) {
    auto bytecode = static_cast<Bytecode>(val);
    NOISEPAGE_ASSERT(bytecode <= Bytecode::Last, "Invalid bytecode");
    return bytecode;
  }

  /**
   * @return True if the bytecode @em bytecode is an unconditional jump; false otherwise.
   */
  static constexpr bool IsUnconditionalJump(Bytecode bytecode) { return bytecode == Bytecode::Jump; }

  /**
   * @return True if the bytecode @em bytecode is a conditional jump; false otherwise.
   */
  static constexpr bool IsConditionalJump(Bytecode bytecode) {
    return bytecode == Bytecode::JumpIfFalse || bytecode == Bytecode::JumpIfTrue;
  }

  /**
   * @return True if the bytecode @em bytecode is a jump instruction, either conditional or not;
   *         false otherwise.
   */
  static constexpr bool IsJump(Bytecode bytecode) {
    return IsConditionalJump(bytecode) || IsUnconditionalJump(bytecode);
  }

  /**
   * @return True if the bytecode @em bytecode is a return instruction.
   */
  static constexpr bool IsReturn(Bytecode bytecode) { return bytecode == Bytecode::Return; }

  /**
   * @return True if the bytecode @em bytecode is a terminal instruction. A terminal instruction is
   *         one that appears at the end of a basic block.
   */
  static constexpr bool IsTerminal(Bytecode bytecode) { return IsJump(bytecode) || IsReturn(bytecode); }

 private:
  static const char *bytecode_names[];
  static uint32_t bytecode_operand_counts[];
  static const OperandType *bytecode_operand_types[];
  static const OperandSize *bytecode_operand_sizes[];
  static const char *bytecode_handler_name[];
};

}  // namespace noisepage::execution::vm
