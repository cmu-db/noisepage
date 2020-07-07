#pragma once

#include <cstdint>

#include "common/all_static.h"

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
  F(ExtractYear, extractYear)                                           \
                                                                        \
  /* Thread State Container */                                          \
  F(ExecutionContextGetMemoryPool, execCtxGetMem)                       \
  F(ExecutionContextGetTLS, execCtxGetTLS)                              \
  F(ThreadStateContainerReset, tlsReset)                                \
  F(ThreadStateContainerGetState, tlsGetCurrentThreadState)             \
  F(ThreadStateContainerIterate, tlsIterate)                            \
  F(ThreadStateContainerClear, tlsClear)                                \
                                                                        \
  /* Table scans */                                                     \
  F(TableIterInit, tableIterInit)                                       \
  F(TableIterAdvance, tableIterAdvance)                                 \
  F(TableIterGetVPI, tableIterGetVPI)                                   \
  F(TableIterClose, tableIterClose)                                     \
  F(TableIterParallel, iterateTableParallel)                            \
  F(TableIterGetSlot, tableIterGetSlot)                                 \
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
  F(VPIGetBool, vpiGetBool)                                             \
  F(VPIGetTinyInt, vpiGetTinyInt)                                       \
  F(VPIGetSmallInt, vpiGetSmallInt)                                     \
  F(VPIGetInt, vpiGetInt)                                               \
  F(VPIGetBigInt, vpiGetBigInt)                                         \
  F(VPIGetReal, vpiGetReal)                                             \
  F(VPIGetDouble, vpiGetDouble)                                         \
  F(VPIGetDate, vpiGetDate)                                             \
  F(VPIGetTimestamp, vpiGetTimestamp)                                   \
  F(VPIGetString, vpiGetString)                                         \
  F(VPIGetPointer, vpiGetPointer)                                       \
  F(VPISetBool, vpiSetBool)                                             \
  F(VPISetTinyInt, vpiSetTinyInt)                                       \
  F(VPISetSmallInt, vpiSetSmallInt)                                     \
  F(VPISetInt, vpiSetInt)                                               \
  F(VPISetBigInt, vpiSetBigInt)                                         \
  F(VPISetReal, vpiSetReal)                                             \
  F(VPISetDouble, vpiSetDouble)                                         \
  F(VPISetDate, vpiSetDate)                                             \
  F(VPISetTimestamp, vpiSetTimestamp)                                   \
  F(VPISetString, vpiSetString)                                         \
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
  /* CSV */                                                             \
  F(CSVReaderInit, csvReaderInit)                                       \
  F(CSVReaderAdvance, csvReaderAdvance)                                 \
  F(CSVReaderGetField, csvReaderGetField)                               \
  F(CSVReaderGetRecordNumber, csvReaderGetRecordNumber)                 \
  F(CSVReaderClose, csvReaderClose)                                     \
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
  F(Version, version)                                                   \
                                                                        \
  /* Mini runners functions */                                          \
  F(NpRunnersEmitInt, NpRunnersEmitInt)                                 \
  F(NpRunnersEmitReal, NpRunnersEmitReal)                               \
  F(NpRunnersDummyInt, NpRunnersDummyInt)                               \
  F(NpRunnersDummyReal, NpRunnersDummyReal)                             \
                                                                        \
  F(AbortTxn, abortTxn)                                                 \
                                                                        \
  /* FOR TESTING USE ONLY!!!!! */                                       \
  F(TestCatalogLookup, testCatalogLookup)                               \
  F(TestCatalogIndexLookup, testCatalogIndexLookup)

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
  /** The total number of builtin functions. */
  static const uint32_t BUILTINS_COUNT = static_cast<uint32_t>(Builtin::Last) + 1;

  /**
   * @return The total number of builtin functions.
   */
  static constexpr uint32_t NumBuiltins() { return BUILTINS_COUNT; }

  /**
   * @return The name of the function associated with the given builtin enumeration.
   */
  static const char *GetFunctionName(Builtin builtin) { return builtin_function_names[static_cast<uint8_t>(builtin)]; }

 private:
  static const char *builtin_function_names[];
};

}  // namespace terrier::execution::ast
