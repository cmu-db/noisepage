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
  F(StringToSql, stringToSql)                                           \
  F(SqlToBool, sqlToBool)                                               \
  F(IsValNull, isValNull)                                               \
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
  F(SizeOf, sizeOf)                                                     \
  F(OffsetOf, offsetOf)                                                 \
  F(PtrCast, ptrCast)                                                   \
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
  /** The total number of builtin functions. */
  static const uint32_t BUILTINS_COUNT = static_cast<uint32_t>(Builtin::Last) + 1;

  /**
   * @return The total number of builtin functions.
   */
  static constexpr uint32_t NumBuiltins() { return BUILTINS_COUNT; }

  /**
   * @return The name of the function associated with the given builtin enumeration.
   */
  static const char *GetFunctionName(Builtin builtin) { return BUILTIN_FUNCTION_NAMES[static_cast<uint8_t>(builtin)]; }

 private:
  static const char *BUILTIN_FUNCTION_NAMES[];
};

}  // namespace terrier::execution::ast
