#pragma once

#include <cstdint>

#include "common/macros.h"

namespace terrier::execution::ast {

// The list of all builtin functions
// Args: internal name, function name
#define BUILTINS_LIST(F)                                                \
  /* Primitive <-> SQL */                                               \
  F(IntToSql, intToSql)                                                 \
  F(BoolToSql, boolToSql)                                               \
  F(FloatToSql, floatToSql)                                             \
  F(DateToSql, dateToSql)                                               \
  F(TimestampToSql, timestampToSql)                                     \
  F(TimestampToSqlYMDHMSMU, timestampToSqlYMDHMSMU)                     \
  F(StringToSql, stringToSql)                                           \
  F(SqlToBool, sqlToBool)                                               \
  F(IsValNull, isValNull)                                               \
  F(InitSqlNull, initSqlNull)                                           \
                                                                        \
  /* SQL Conversions */                                                 \
  F(ConvertBoolToInteger, convertBoolToInt)                             \
  F(ConvertIntegerToReal, convertIntToReal)                             \
  F(ConvertDateToTimestamp, convertDateToTime)                          \
  F(ConvertStringToBool, convertStringToBool)                           \
  F(ConvertStringToInt, convertStringToInt)                             \
  F(ConvertStringToReal, convertStringToReal)                           \
  F(ConvertStringToDate, convertStringToDate)                           \
  F(ConvertStringToTime, convertStringToTime)                           \
                                                                        \
  /* SQL Functions */                                                   \
  F(Like, like)                                                         \
  F(DatePart, datePart)                                                 \
                                                                        \
  /* Thread State Container */                                          \
  F(ExecutionContextAddRowsAffected, execCtxAddRowsAffected)            \
  F(ExecutionContextGetMemoryPool, execCtxGetMem)                       \
  F(ExecutionContextGetTLS, execCtxGetTLS)                              \
  F(ExecutionContextRegisterHook, execCtxRegisterHook)                  \
  F(ExecutionContextClearHooks, execCtxClearHooks)                      \
  F(ExecutionContextInitHooks, execCtxInitHooks)                        \
  F(ThreadStateContainerReset, tlsReset)                                \
  F(ThreadStateContainerGetState, tlsGetCurrentThreadState)             \
  F(ThreadStateContainerIterate, tlsIterate)                            \
  F(ThreadStateContainerClear, tlsClear)                                \
  F(ExecOUFeatureVectorRecordFeature, execOUFeatureVectorRecordFeature) \
  F(ExecOUFeatureVectorInitialize, execOUFeatureVectorInit)             \
  F(ExecOUFeatureVectorFilter, execOUFeatureVectorFilter)               \
  F(ExecOUFeatureVectorReset, execOUFeatureVectorReset)                 \
                                                                        \
  /* Table scans */                                                     \
  F(TableIterInit, tableIterInit)                                       \
  F(TableIterAdvance, tableIterAdvance)                                 \
  F(TableIterGetVPINumTuples, tableIterGetVPINumTuples)                 \
  F(TableIterGetVPI, tableIterGetVPI)                                   \
  F(TableIterClose, tableIterClose)                                     \
  F(TableIterParallel, iterateTableParallel)                            \
  F(TableIterCreateIndexParallel, iterateTableCreateIndexParallel)      \
                                                                        \
  /* VPI */                                                             \
  F(VPIInit, vpiInit)                                                   \
  F(VPIIsFiltered, vpiIsFiltered)                                       \
  F(VPIGetSelectedRowCount, vpiSelectedRowCount)                        \
  F(VPIGetVectorProjection, vpiGetVectorProjection)                     \
  F(VPIHasNext, vpiHasNext)                                             \
  F(VPIHasNextFiltered, vpiHasNextFiltered)                             \
  F(VPIAdvance, vpiAdvance)                                             \
  F(VPIAdvanceFiltered, vpiAdvanceFiltered)                             \
  F(VPISetPosition, vpiSetPosition)                                     \
  F(VPISetPositionFiltered, vpiSetPositionFiltered)                     \
  F(VPIMatch, vpiMatch)                                                 \
  F(VPIReset, vpiReset)                                                 \
  F(VPIResetFiltered, vpiResetFiltered)                                 \
  F(VPIGetSlot, vpiGetSlot)                                             \
  F(VPIGetBool, vpiGetBool)                                             \
  F(VPIGetBoolNull, vpiGetBoolNull)                                     \
  F(VPIGetTinyInt, vpiGetTinyInt)                                       \
  F(VPIGetTinyIntNull, vpiGetTinyIntNull)                               \
  F(VPIGetSmallInt, vpiGetSmallInt)                                     \
  F(VPIGetSmallIntNull, vpiGetSmallIntNull)                             \
  F(VPIGetInt, vpiGetInt)                                               \
  F(VPIGetIntNull, vpiGetIntNull)                                       \
  F(VPIGetBigInt, vpiGetBigInt)                                         \
  F(VPIGetBigIntNull, vpiGetBigIntNull)                                 \
  F(VPIGetReal, vpiGetReal)                                             \
  F(VPIGetRealNull, vpiGetRealNull)                                     \
  F(VPIGetDouble, vpiGetDouble)                                         \
  F(VPIGetDoubleNull, vpiGetDoubleNull)                                 \
  F(VPIGetDate, vpiGetDate)                                             \
  F(VPIGetDateNull, vpiGetDateNull)                                     \
  F(VPIGetTimestamp, vpiGetTimestamp)                                   \
  F(VPIGetTimestampNull, vpiGetTimestampNull)                           \
  F(VPIGetString, vpiGetString)                                         \
  F(VPIGetStringNull, vpiGetStringNull)                                 \
  F(VPIGetPointer, vpiGetPointer)                                       \
  F(VPISetBool, vpiSetBool)                                             \
  F(VPISetBoolNull, vpiSetBoolNull)                                     \
  F(VPISetTinyInt, vpiSetTinyInt)                                       \
  F(VPISetTinyIntNull, vpiSetTinyIntNull)                               \
  F(VPISetSmallInt, vpiSetSmallInt)                                     \
  F(VPISetSmallIntNull, vpiSetSmallIntNull)                             \
  F(VPISetInt, vpiSetInt)                                               \
  F(VPISetIntNull, vpiSetIntNull)                                       \
  F(VPISetBigInt, vpiSetBigInt)                                         \
  F(VPISetBigIntNull, vpiSetBigIntNull)                                 \
  F(VPISetReal, vpiSetReal)                                             \
  F(VPISetRealNull, vpiSetRealNull)                                     \
  F(VPISetDouble, vpiSetDouble)                                         \
  F(VPISetDoubleNull, vpiSetDoubleNull)                                 \
  F(VPISetDate, vpiSetDate)                                             \
  F(VPISetDateNull, vpiSetDateNull)                                     \
  F(VPISetTimestamp, vpiSetTimestamp)                                   \
  F(VPISetTimestampNull, vpiSetTimestampNull)                           \
  F(VPISetString, vpiSetString)                                         \
  F(VPISetStringNull, vpiSetStringNull)                                 \
  F(VPIFree, vpiFree)                                                   \
                                                                        \
  /* Hashing */                                                         \
  F(Hash, hash)                                                         \
                                                                        \
  /* Filter Manager */                                                  \
  F(FilterManagerInit, filterManagerInit)                               \
  F(FilterManagerInsertFilter, filterManagerInsertFilter)               \
  F(FilterManagerRunFilters, filterManagerRunFilters)                   \
  F(FilterManagerFree, filterManagerFree)                               \
  /* Filter Execution */                                                \
  F(VectorFilterEqual, filterEq)                                        \
  F(VectorFilterGreaterThan, filterGt)                                  \
  F(VectorFilterGreaterThanEqual, filterGe)                             \
  F(VectorFilterLessThan, filterLt)                                     \
  F(VectorFilterLessThanEqual, filterLe)                                \
  F(VectorFilterNotEqual, filterNe)                                     \
  F(VectorFilterLike, filterLike)                                       \
  F(VectorFilterNotLike, filterNotLike)                                 \
                                                                        \
  /* Aggregations */                                                    \
  F(AggHashTableInit, aggHTInit)                                        \
  F(AggHashTableGetTupleCount, aggHTGetTupleCount)                      \
  F(AggHashTableGetInsertCount, aggHTGetInsertCount)                    \
  F(AggHashTableInsert, aggHTInsert)                                    \
  F(AggHashTableLinkEntry, aggHTLink)                                   \
  F(AggHashTableLookup, aggHTLookup)                                    \
  F(AggHashTableProcessBatch, aggHTProcessBatch)                        \
  F(AggHashTableMovePartitions, aggHTMoveParts)                         \
  F(AggHashTableParallelPartitionedScan, aggHTParallelPartScan)         \
  F(AggHashTableFree, aggHTFree)                                        \
  F(AggHashTableIterInit, aggHTIterInit)                                \
  F(AggHashTableIterHasNext, aggHTIterHasNext)                          \
  F(AggHashTableIterNext, aggHTIterNext)                                \
  F(AggHashTableIterGetRow, aggHTIterGetRow)                            \
  F(AggHashTableIterClose, aggHTIterClose)                              \
  F(AggPartIterHasNext, aggPartIterHasNext)                             \
  F(AggPartIterNext, aggPartIterNext)                                   \
  F(AggPartIterGetHash, aggPartIterGetHash)                             \
  F(AggPartIterGetRow, aggPartIterGetRow)                               \
  F(AggPartIterGetRowEntry, aggPartIterGetRowEntry)                     \
  F(AggInit, aggInit)                                                   \
  F(AggAdvance, aggAdvance)                                             \
  F(AggMerge, aggMerge)                                                 \
  F(AggReset, aggReset)                                                 \
  F(AggResult, aggResult)                                               \
                                                                        \
  /* Joins */                                                           \
  F(JoinHashTableInit, joinHTInit)                                      \
  F(JoinHashTableInsert, joinHTInsert)                                  \
  F(JoinHashTableBuild, joinHTBuild)                                    \
  F(JoinHashTableBuildParallel, joinHTBuildParallel)                    \
  F(JoinHashTableGetTupleCount, joinHTGetTupleCount)                    \
  F(JoinHashTableLookup, joinHTLookup)                                  \
  F(JoinHashTableFree, joinHTFree)                                      \
                                                                        \
  /* Hash Table Entry Iterator (for hash joins) */                      \
  F(HashTableEntryIterHasNext, htEntryIterHasNext)                      \
  F(HashTableEntryIterGetRow, htEntryIterGetRow)                        \
                                                                        \
  F(JoinHashTableIterInit, joinHTIterInit)                              \
  F(JoinHashTableIterHasNext, joinHTIterHasNext)                        \
  F(JoinHashTableIterNext, joinHTIterNext)                              \
  F(JoinHashTableIterGetRow, joinHTIterGetRow)                          \
  F(JoinHashTableIterFree, joinHTIterFree)                              \
                                                                        \
  /* Sorting */                                                         \
  F(SorterInit, sorterInit)                                             \
  F(SorterGetTupleCount, sorterGetTupleCount)                           \
  F(SorterInsert, sorterInsert)                                         \
  F(SorterInsertTopK, sorterInsertTopK)                                 \
  F(SorterInsertTopKFinish, sorterInsertTopKFinish)                     \
  F(SorterSort, sorterSort)                                             \
  F(SorterSortParallel, sorterSortParallel)                             \
  F(SorterSortTopKParallel, sorterSortTopKParallel)                     \
  F(SorterFree, sorterFree)                                             \
  F(SorterIterInit, sorterIterInit)                                     \
  F(SorterIterHasNext, sorterIterHasNext)                               \
  F(SorterIterNext, sorterIterNext)                                     \
  F(SorterIterSkipRows, sorterIterSkipRows)                             \
  F(SorterIterGetRow, sorterIterGetRow)                                 \
  F(SorterIterClose, sorterIterClose)                                   \
                                                                        \
  /* Output */                                                          \
  F(ResultBufferNew, resultBufferNew)                                   \
  F(ResultBufferAllocOutRow, resultBufferAllocRow)                      \
  F(ResultBufferFinalize, resultBufferFinalize)                         \
  F(ResultBufferFree, resultBufferFree)                                 \
                                                                        \
  /* Index */                                                           \
  F(IndexIteratorInit, indexIteratorInit)                               \
  F(IndexIteratorGetSize, indexIteratorGetSize)                         \
  F(IndexIteratorScanKey, indexIteratorScanKey)                         \
  F(IndexIteratorScanAscending, indexIteratorScanAscending)             \
  F(IndexIteratorScanDescending, indexIteratorScanDescending)           \
  F(IndexIteratorScanLimitDescending, indexIteratorScanLimitDescending) \
  F(IndexIteratorAdvance, indexIteratorAdvance)                         \
  F(IndexIteratorGetPR, indexIteratorGetPR)                             \
  F(IndexIteratorGetLoPR, indexIteratorGetLoPR)                         \
  F(IndexIteratorGetHiPR, indexIteratorGetHiPR)                         \
  F(IndexIteratorGetSlot, indexIteratorGetSlot)                         \
  F(IndexIteratorGetTablePR, indexIteratorGetTablePR)                   \
  F(IndexIteratorFree, indexIteratorFree)                               \
                                                                        \
  /* Projected Row Operations */                                        \
  F(PRSetBool, prSetBool)                                               \
  F(PRSetTinyInt, prSetTinyInt)                                         \
  F(PRSetSmallInt, prSetSmallInt)                                       \
  F(PRSetInt, prSetInt)                                                 \
  F(PRSetBigInt, prSetBigInt)                                           \
  F(PRSetReal, prSetReal)                                               \
  F(PRSetDouble, prSetDouble)                                           \
  F(PRSetDate, prSetDate)                                               \
  F(PRSetTimestamp, prSetTimestamp)                                     \
  F(PRSetVarlen, prSetVarlen)                                           \
  F(PRSetBoolNull, prSetBoolNull)                                       \
  F(PRSetTinyIntNull, prSetTinyIntNull)                                 \
  F(PRSetSmallIntNull, prSetSmallIntNull)                               \
  F(PRSetIntNull, prSetIntNull)                                         \
  F(PRSetBigIntNull, prSetBigIntNull)                                   \
  F(PRSetRealNull, prSetRealNull)                                       \
  F(PRSetDoubleNull, prSetDoubleNull)                                   \
  F(PRSetDateNull, prSetDateNull)                                       \
  F(PRSetTimestampNull, prSetTimestampNull)                             \
  F(PRSetVarlenNull, prSetVarlenNull)                                   \
  F(PRGetBool, prGetBool)                                               \
  F(PRGetTinyInt, prGetTinyInt)                                         \
  F(PRGetSmallInt, prGetSmallInt)                                       \
  F(PRGetInt, prGetInt)                                                 \
  F(PRGetBigInt, prGetBigInt)                                           \
  F(PRGetReal, prGetReal)                                               \
  F(PRGetDouble, prGetDouble)                                           \
  F(PRGetDate, prGetDate)                                               \
  F(PRGetTimestamp, prGetTimestamp)                                     \
  F(PRGetVarlen, prGetVarlen)                                           \
  F(PRGetBoolNull, prGetBoolNull)                                       \
  F(PRGetTinyIntNull, prGetTinyIntNull)                                 \
  F(PRGetSmallIntNull, prGetSmallIntNull)                               \
  F(PRGetIntNull, prGetIntNull)                                         \
  F(PRGetBigIntNull, prGetBigIntNull)                                   \
  F(PRGetRealNull, prGetRealNull)                                       \
  F(PRGetDoubleNull, prGetDoubleNull)                                   \
  F(PRGetDateNull, prGetDateNull)                                       \
  F(PRGetTimestampNull, prGetTimestampNull)                             \
  F(PRGetVarlenNull, prGetVarlenNull)                                   \
  /* CSV */                                                             \
  F(CSVReaderInit, csvReaderInit)                                       \
  F(CSVReaderAdvance, csvReaderAdvance)                                 \
  F(CSVReaderGetField, csvReaderGetField)                               \
  F(CSVReaderGetRecordNumber, csvReaderGetRecordNumber)                 \
  F(CSVReaderClose, csvReaderClose)                                     \
                                                                        \
  /* SQL Table Calls */                                                 \
  F(StorageInterfaceInit, storageInterfaceInit)                         \
  F(StorageInterfaceGetIndexHeapSize, storageInterfaceGetIndexHeapSize) \
  F(GetTablePR, getTablePR)                                             \
  F(TableInsert, tableInsert)                                           \
  F(TableDelete, tableDelete)                                           \
  F(TableUpdate, tableUpdate)                                           \
  F(GetIndexPR, getIndexPR)                                             \
  F(IndexGetSize, indexGetSize)                                         \
  F(IndexInsert, indexInsert)                                           \
  F(IndexInsertUnique, indexInsertUnique)                               \
  F(IndexInsertWithSlot, indexInsertWithSlot)                           \
  F(IndexDelete, indexDelete)                                           \
  F(StorageInterfaceFree, storageInterfaceFree)                         \
  /* Trig */                                                            \
  F(ACos, acos)                                                         \
  F(ASin, asin)                                                         \
  F(ATan, atan)                                                         \
  F(ATan2, atan2)                                                       \
  F(Cosh, cosh)                                                         \
  F(Sinh, sinh)                                                         \
  F(Tanh, tanh)                                                         \
  F(Cos, cos)                                                           \
  F(Cot, cot)                                                           \
  F(Sin, sin)                                                           \
  F(Tan, tan)                                                           \
  F(Ceil, ceil)                                                         \
  F(Floor, floor)                                                       \
  F(Truncate, truncate)                                                 \
  F(Log10, log10)                                                       \
  F(Log2, log2)                                                         \
  F(Sqrt, sqrt)                                                         \
  F(Cbrt, cbrt)                                                         \
  F(Round, round)                                                       \
  F(Round2, round2)                                                     \
                                                                        \
  /* EXP */                                                             \
  F(Exp, exp)                                                           \
                                                                        \
  /* Generic */                                                         \
  F(SizeOf, sizeOf)                                                     \
  F(OffsetOf, offsetOf)                                                 \
  F(PtrCast, ptrCast)                                                   \
                                                                        \
  /* Parameter calls */                                                 \
  F(GetParamBool, getParamBool)                                         \
  F(GetParamTinyInt, getParamTinyInt)                                   \
  F(GetParamSmallInt, getParamSmallInt)                                 \
  F(GetParamInt, getParamInt)                                           \
  F(GetParamBigInt, getParamBigInt)                                     \
  F(GetParamReal, getParamReal)                                         \
  F(GetParamDouble, getParamDouble)                                     \
  F(GetParamDate, getParamDate)                                         \
  F(GetParamTimestamp, getParamTimestamp)                               \
  F(GetParamString, getParamString)                                     \
                                                                        \
  /* String functions */                                                \
  F(Lower, lower)                                                       \
  F(Upper, upper)                                                       \
  F(Version, version)                                                   \
  F(StartsWith, startsWith)                                             \
  F(Substring, substring)                                               \
  F(Left, left)                                                         \
  F(Right, right)                                                       \
  F(Reverse, reverse)                                                   \
  F(Repeat, repeat)                                                     \
  F(Trim, trim)                                                         \
  F(Trim2, trim2)                                                       \
  F(Position, position)                                                 \
  F(ASCII, ascii)                                                       \
  F(Length, length)                                                     \
  F(InitCap, initCap)                                                   \
  F(SplitPart, splitPart)                                               \
  F(Lpad, lpad)                                                         \
  F(Ltrim, ltrim)                                                       \
  F(Rpad, rpad)                                                         \
  F(Rtrim, rtrim)                                                       \
  F(Concat, concat)                                                     \
                                                                        \
  /* Char function */                                                   \
  F(Chr, chr)                                                           \
  F(CharLength, charLength)                                             \
                                                                        \
  /* Arithmetic functions */                                            \
  F(Mod, mod)                                                           \
  F(Pow, pow)                                                           \
  F(Abs, abs)                                                           \
                                                                        \
  /* Mini runners functions */                                          \
  F(NpRunnersEmitInt, NpRunnersEmitInt)                                 \
  F(NpRunnersEmitReal, NpRunnersEmitReal)                               \
  F(NpRunnersDummyInt, NpRunnersDummyInt)                               \
  F(NpRunnersDummyReal, NpRunnersDummyReal)                             \
                                                                        \
  F(ExecutionContextStartResourceTracker, execCtxStartResourceTracker)  \
  F(ExecutionContextSetMemoryUseOverride, execCtxSetMemoryUseOverride)  \
  F(ExecutionContextEndResourceTracker, execCtxEndResourceTracker)      \
  F(ExecutionContextStartPipelineTracker, execCtxStartPipelineTracker)  \
  F(ExecutionContextEndPipelineTracker, execCtxEndPipelineTracker)      \
                                                                        \
  F(RegisterThreadWithMetricsManager, registerThreadWithMetricsManager) \
  F(CheckTrackersStopped, checkTrackersStopped)                         \
  F(AggregateMetricsThread, aggregateMetricsThread)                     \
                                                                        \
  F(AbortTxn, abortTxn)                                                 \
                                                                        \
  /* FOR TESTING USE ONLY!!!!! */                                       \
  F(TestCatalogLookup, testCatalogLookup)                               \
  F(TestCatalogIndexLookup, testCatalogIndexLookup)

/**
 * An enumeration of all TPL builtin functions.
 */
enum class Builtin : uint16_t {
#define ENTRY(Name, ...) Name,
  BUILTINS_LIST(ENTRY)
#undef ENTRY
#define COUNT_OP(inst, ...) +1
      Last = -1 BUILTINS_LIST(COUNT_OP)
#undef COUNT_OP
};

/**
 * Manages builtin functions.
 */
class Builtins {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(Builtins);

  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(Builtins);

  /** The total number of builtin functions. */
  static const uint32_t BUILTINS_COUNT = static_cast<uint32_t>(Builtin::Last) + 1;

  /**
   * @return The total number of builtin functions.
   */
  static constexpr uint32_t NumBuiltins() { return BUILTINS_COUNT; }

  /**
   * @return The name of the function associated with the given builtin enumeration.
   */
  static const char *GetFunctionName(Builtin builtin) {
    return builtin_function_names[static_cast<std::underlying_type<Builtin>::type>(builtin)];
  }

 private:
  static const char *builtin_function_names[];
};

}  // namespace terrier::execution::ast
