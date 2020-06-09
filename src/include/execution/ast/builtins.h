#pragma once

#include <cstdint>

#include "common/all_static.h"

namespace terrier::execution::ast {

// The list of all builtin functions
// Args: internal name, function name
#define BUILTINS_LIST(F)                                                \
  /* SQL NULL. */                                                       \
  F(IsSqlNull, isSqlNull)                                               \
  F(IsSqlNotNull, isSqlNotNull)                                         \
  F(NullToSql, nullToSql)                                               \
                                                                        \
  /* Primitive <-> SQL */                                               \
  F(BoolToSql, boolToSql)                                               \
  F(DateToSql, dateToSql)                                               \
  F(FloatToSql, floatToSql)                                             \
  F(IntToSql, intToSql)                                                 \
  F(IsValNull, isValNull)                                               \
  F(SqlToBool, sqlToBool)                                               \
  F(StringToSql, stringToSql)                                           \
  F(TimestampToSql, timestampToSql)                                     \
  F(TimestampToSqlHMSu, timestampToSqlHMSu)                             \
  F(VarlenToSql, varlenToSql)                                           \
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
  F(ExtractYear, extractYear)                                           \
                                                                        \
  /* Vectorized Filters */                                              \
  F(FilterEq, filterEq)                                                 \
  F(FilterGe, filterGe)                                                 \
  F(FilterGt, filterGt)                                                 \
  F(FilterLe, filterLe)                                                 \
  F(FilterLt, filterLt)                                                 \
  F(FilterNe, filterNe)                                                 \
                                                                        \
  /* Thread State Container */                                          \
  F(ExecutionContextGetMemoryPool, execCtxGetMem)                       \
  F(ExecutionContextGetTLS, execCtxGetTLS)                              \
  F(ExecutionContextStartResourceTracker, execCtxStartResourceTracker)  \
  F(ExecutionContextEndResourceTracker, execCtxEndResourceTracker)      \
  F(ExecutionContextEndPipelineTracker, execCtxEndPipelineTracker)      \
  F(ThreadStateContainerInit, tlsInit)                                  \
  F(ThreadStateContainerReset, tlsReset)                                \
  F(ThreadStateContainerGetState, tlsGetCurrentThreadState)             \
  F(ThreadStateContainerIterate, tlsIterate)                            \
  F(ThreadStateContainerClear, tlsClear)                                \
  F(ThreadStateContainerFree, tlsFree)                                  \
                                                                        \
  /* Table scans */                                                     \
  F(TableIterInit, tableIterInit)                                       \
  F(TableIterInitBind, tableIterInitBind)                               \
  F(TableIterAdvance, tableIterAdvance)                                 \
  F(TableIterGetVPI, tableIterGetVPI)                                   \
  F(TableIterClose, tableIterClose)                                     \
  F(TableIterReset, tableIterReset)                                     \
  F(TableIterParallel, iterateTableParallel)                            \
                                                                        \
  /* VPI */                                                             \
  F(VPIIsFiltered, vpiIsFiltered)                                       \
  F(VPIHasNext, vpiHasNext)                                             \
  F(VPIHasNextFiltered, vpiHasNextFiltered)                             \
  F(VPIAdvance, vpiAdvance)                                             \
  F(VPIAdvanceFiltered, vpiAdvanceFiltered)                             \
  F(VPIGetSlot, vpiGetSlot)                                             \
  F(VPIMatch, vpiMatch)                                                 \
  F(VPIReset, vpiReset)                                                 \
  F(VPIResetFiltered, vpiResetFiltered)                                 \
  F(VPIGetBool, vpiGetBool)                                             \
  F(VPIGetTinyInt, vpiGetTinyInt)                                       \
  F(VPIGetSmallInt, vpiGetSmallInt)                                     \
  F(VPIGetInt, vpiGetInt)                                               \
  F(VPIGetBigInt, vpiGetBigInt)                                         \
  F(VPIGetReal, vpiGetReal)                                             \
  F(VPIGetDouble, vpiGetDouble)                                         \
  F(VPIGetDate, vpiGetDate)                                             \
  F(VPIGetTimestamp, vpiGetTimestamp)                                   \
  F(VPIGetVarlen, vpiGetVarlen)                                         \
  F(VPIGetBoolNull, vpiGetBoolNull)                                     \
  F(VPIGetTinyIntNull, vpiGetTinyIntNull)                               \
  F(VPIGetSmallIntNull, vpiGetSmallIntNull)                             \
  F(VPIGetIntNull, vpiGetIntNull)                                       \
  F(VPIGetBigIntNull, vpiGetBigIntNull)                                 \
  F(VPIGetRealNull, vpiGetRealNull)                                     \
  F(VPIGetDoubleNull, vpiGetDoubleNull)                                 \
  F(VPIGetDateNull, vpiGetDateNull)                                     \
  F(VPIGetTimestampNull, vpiGetTimestampNull)                           \
  F(VPIGetVarlenNull, vpiGetVarlenNull)                                 \
                                                                        \
  /* Hashing */                                                         \
  F(Hash, hash)                                                         \
                                                                        \
  /* Filter Manager */                                                  \
  F(FilterManagerInit, filterManagerInit)                               \
  F(FilterManagerInsertFilter, filterManagerInsertFilter)               \
  F(FilterManagerFinalize, filterManagerFinalize)                       \
  F(FilterManagerRunFilters, filterManagerRunFilters)                   \
  F(FilterManagerFree, filterManagerFree)                               \
                                                                        \
  /* Aggregations */                                                    \
  F(AggHashTableInit, aggHTInit)                                        \
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
  F(JoinHashTableIterInit, joinHTIterInit)                              \
  F(JoinHashTableIterHasNext, joinHTIterHasNext)                        \
  F(JoinHashTableIterGetRow, joinHTIterGetRow)                          \
  F(JoinHashTableIterClose, joinHTIterClose)                            \
  F(JoinHashTableBuild, joinHTBuild)                                    \
  F(JoinHashTableBuildParallel, joinHTBuildParallel)                    \
  F(JoinHashTableLookup, joinHTLookup)                                  \
  F(JoinHashTableFree, joinHTFree)                                      \
                                                                        \
  /* Hash Table Entry Iterator (for hash joins) */                      \
  F(HashTableEntryIterHasNext, htEntryIterHasNext)                      \
  F(HashTableEntryIterGetRow, htEntryIterGetRow)                        \
                                                                        \
  /* Sorting */                                                         \
  F(SorterInit, sorterInit)                                             \
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
  F(ResultBufferAllocOutRow, resultBufferAllocRow)                      \
  F(ResultBufferFinalize, resultBufferFinalize)                         \
                                                                        \
  /* CSV */                                                             \
  F(CSVReaderInit, csvReaderInit)                                       \
  F(CSVReaderAdvance, csvReaderAdvance)                                 \
  F(CSVReaderGetField, csvReaderGetField)                               \
  F(CSVReaderGetRecordNumber, csvReaderGetRecordNumber)                 \
  F(CSVReaderClose, csvReaderClose)                                     \
                                                                        \
  /* Trig */                                                            \
  F(ACos, acos)                                                         \
  F(ASin, asin)                                                         \
  F(ATan, atan)                                                         \
  F(ATan2, atan2)                                                       \
  F(Cos, cos)                                                           \
  F(Cot, cot)                                                           \
  F(Sin, sin)                                                           \
  F(Tan, tan)                                                           \
                                                                        \
  /* Generic */                                                         \
  F(OffsetOf, offsetOf)                                                 \
  F(PtrCast, ptrCast)                                                   \
  F(SizeOf, sizeOf)                                                     \
                                                                        \
  /* Output Buffer */                                                   \
  F(OutputAlloc, outputAlloc)                                           \
  F(OutputFinalize, outputFinalize)                                     \
                                                                        \
  /* Index */                                                           \
  F(IndexIteratorInit, indexIteratorInit)                               \
  F(IndexIteratorInitBind, indexIteratorInitBind)                       \
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
                                                                        \
  /* SQL Table Calls */                                                 \
  F(StorageInterfaceInit, storageInterfaceInit)                         \
  F(StorageInterfaceInitBind, storageInterfaceInitBind)                 \
  F(GetTablePR, getTablePR)                                             \
  F(TableInsert, tableInsert)                                           \
  F(TableDelete, tableDelete)                                           \
  F(TableUpdate, tableUpdate)                                           \
  F(GetIndexPR, getIndexPR)                                             \
  F(GetIndexPRBind, getIndexPRBind)                                     \
  F(IndexInsert, indexInsert)                                           \
  F(IndexInsertUnique, indexInsertUnique)                               \
  F(IndexDelete, indexDelete)                                           \
  F(StorageInterfaceFree, storageInterfaceFree)                         \
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
  F(Lower, lower)

/**
 * An enumeration of all TPL builtin functions.
 */
enum class Builtin : uint8_t {
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
class Builtins : public common::AllStatic {
 public:
  /**
   * The total number of builtin functions
   */
  static const uint32_t BUILTINS_COUNT = static_cast<uint32_t>(Builtin::Last) + 1;

  /**
   * @return The total number of builtin functions.
   */
  static constexpr uint32_t NumBuiltins() { return BUILTINS_COUNT; }

  /**
   * @return The name of the function associated with the given builtin enumeration.
   */
  static const char *GetFunctionName(Builtin builtin) { return builtin_functions_name[static_cast<uint8_t>(builtin)]; }

 private:
  static const char *builtin_functions_name[];
};

}  // namespace terrier::execution::ast
