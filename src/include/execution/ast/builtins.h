#pragma once

#include "execution/util/common.h"

namespace tpl::ast {

// The list of all builtin functions
// Args: internal name, function name
#define BUILTINS_LIST(F)                                        \
  /* Primitive <-> SQL */                                       \
  F(IntToSql, intToSql)                                         \
  F(BoolToSql, boolToSql)                                       \
  F(FloatToSql, floatToSql)                                     \
  F(SqlToBool, sqlToBool)                                       \
  F(StringToSql, stringToSql)                                   \
  F(VarlenToSql, varlenToSql)                                   \
  F(DateToSql, dateToSql)                                       \
                                                                \
  /* Vectorized Filters */                                      \
  F(FilterEq, filterEq)                                         \
  F(FilterGe, filterGe)                                         \
  F(FilterGt, filterGt)                                         \
  F(FilterLe, filterLe)                                         \
  F(FilterLt, filterLt)                                         \
  F(FilterNe, filterNe)                                         \
                                                                \
  /* Thread State Container */                                  \
  F(ExecutionContextGetMemoryPool, execCtxGetMem)               \
  F(ThreadStateContainerInit, tlsInit)                          \
  F(ThreadStateContainerReset, tlsReset)                        \
  F(ThreadStateContainerIterate, tlsIterate)                    \
  F(ThreadStateContainerFree, tlsFree)                          \
                                                                \
  /* Table scans */                                             \
  F(TableIterConstruct, tableIterConstruct)                               \
  F(TableIterConstructBind, tableIterConstructBind)                               \
  F(TableIterPerformInit, tableIterPerformInit)                               \
  F(TableIterAddCol, tableIterAddCol)                               \
  F(TableIterAddColBind, tableIterAddColBind)                               \
  F(TableIterAdvance, tableIterAdvance)                         \
  F(TableIterGetPCI, tableIterGetPCI)                           \
  F(TableIterClose, tableIterClose)                             \
  F(TableIterParallel, iterateTableParallel)                    \
                                                                \
  /* PCI */                                                     \
  F(PCIIsFiltered, pciIsFiltered)                               \
  F(PCIHasNext, pciHasNext)                                     \
  F(PCIHasNextFiltered, pciHasNextFiltered)                     \
  F(PCIAdvance, pciAdvance)                                     \
  F(PCIAdvanceFiltered, pciAdvanceFiltered)                     \
  F(PCIMatch, pciMatch)                                         \
  F(PCIReset, pciReset)                                         \
  F(PCIResetFiltered, pciResetFiltered)                         \
  F(PCIGetTinyInt, pciGetTinyInt)                             \
  F(PCIGetSmallInt, pciGetSmallInt)                             \
  F(PCIGetInt, pciGetInt)                                       \
  F(PCIGetBigInt, pciGetBigInt)                                 \
  F(PCIGetReal, pciGetReal)                                     \
  F(PCIGetDouble, pciGetDouble)                                 \
  F(PCIGetDate, pciGetDate)                                     \
  F(PCIGetVarlen, pciGetVarlen)                                 \
  F(PCIGetTinyIntNull, pciGetTinyIntNull)                     \
  F(PCIGetSmallIntNull, pciGetSmallIntNull)                     \
  F(PCIGetIntNull, pciGetIntNull)                               \
  F(PCIGetBigIntNull, pciGetBigIntNull)                         \
  F(PCIGetRealNull, pciGetRealNull)                             \
  F(PCIGetDoubleNull, pciGetDoubleNull)                         \
  F(PCIGetDateNull, pciGetDateNull)                             \
  F(PCIGetVarlenNull, pciGetVarlenNull)                         \
                                                                \
  /* Hashing */                                                 \
  F(Hash, hash)                                                 \
                                                                \
  /* Filter Manager */                                          \
  F(FilterManagerInit, filterManagerInit)                       \
  F(FilterManagerInsertFilter, filterManagerInsertFilter)       \
  F(FilterManagerFinalize, filterManagerFinalize)               \
  F(FilterManagerRunFilters, filtersRun)                        \
  F(FilterManagerFree, filterManagerFree)                       \
                                                                \
  /* Aggregations */                                            \
  F(AggHashTableInit, aggHTInit)                                \
  F(AggHashTableInsert, aggHTInsert)                            \
  F(AggHashTableLookup, aggHTLookup)                            \
  F(AggHashTableProcessBatch, aggHTProcessBatch)                \
  F(AggHashTableMovePartitions, aggHTMoveParts)                 \
  F(AggHashTableParallelPartitionedScan, aggHTParallelPartScan) \
  F(AggHashTableFree, aggHTFree)                                \
  F(AggHashTableIterInit, aggHTIterInit)                        \
  F(AggHashTableIterHasNext, aggHTIterHasNext)                  \
  F(AggHashTableIterNext, aggHTIterNext)                        \
  F(AggHashTableIterGetRow, aggHTIterGetRow)                    \
  F(AggHashTableIterClose, aggHTIterClose)                      \
  F(AggPartIterHasNext, aggPartIterHasNext)                     \
  F(AggPartIterNext, aggPartIterNext)                           \
  F(AggPartIterGetHash, aggPartIterGetHash)                     \
  F(AggPartIterGetRow, aggPartIterGetRow)                       \
  F(AggInit, aggInit)                                           \
  F(AggAdvance, aggAdvance)                                     \
  F(AggMerge, aggMerge)                                         \
  F(AggReset, aggReset)                                         \
  F(AggResult, aggResult)                                       \
                                                                \
  /* Joins */                                                   \
  F(JoinHashTableInit, joinHTInit)                              \
  F(JoinHashTableInsert, joinHTInsert)                          \
  F(JoinHashTableIterInit, joinHTIterInit)                      \
  F(JoinHashTableIterHasNext, joinHTIterHasNext)                \
  F(JoinHashTableIterGetRow, joinHTIterGetRow)                  \
  F(JoinHashTableIterClose, joinHTIterClose)                    \
  F(JoinHashTableBuild, joinHTBuild)                            \
  F(JoinHashTableBuildParallel, joinHTBuildParallel)            \
  F(JoinHashTableFree, joinHTFree)                              \
                                                                \
  /* Sorting */                                                 \
  F(SorterInit, sorterInit)                                     \
  F(SorterInsert, sorterInsert)                                 \
  F(SorterSort, sorterSort)                                     \
  F(SorterSortParallel, sorterSortParallel)                     \
  F(SorterSortTopKParallel, sorterSortTopKParallel)             \
  F(SorterFree, sorterFree)                                     \
  F(SorterIterInit, sorterIterInit)                             \
  F(SorterIterHasNext, sorterIterHasNext)                       \
  F(SorterIterNext, sorterIterNext)                             \
  F(SorterIterGetRow, sorterIterGetRow)                         \
  F(SorterIterClose, sorterIterClose)                           \
                                                                \
  /* Trig */                                                    \
  F(ACos, acos)                                                 \
  F(ASin, asin)                                                 \
  F(ATan, atan)                                                 \
  F(ATan2, atan2)                                               \
  F(Cos, cos)                                                   \
  F(Cot, cot)                                                   \
  F(Sin, sin)                                                   \
  F(Tan, tan)                                                   \
                                                                \
  /* Generic */                                                 \
  F(SizeOf, sizeOf)                                             \
  F(PtrCast, ptrCast)                                           \
                                                                \
  /* Output Buffer */                                           \
  F(OutputAlloc, outputAlloc)                                   \
  F(OutputAdvance, outputAdvance)                               \
  F(OutputSetNull, outputSetNull)                               \
  F(OutputFinalize, outputFinalize)                             \
                                                                \
  /* Index */                                                   \
  F(IndexIteratorConstruct, indexIteratorConstruct)                       \
  F(IndexIteratorConstructBind, indexIteratorConstructBind)                       \
  F(IndexIteratorPerformInit, indexIteratorPerformInit)                       \
  F(IndexIteratorAddCol, indexIteratorAddCol)                       \
  F(IndexIteratorAddColBind, indexIteratorAddColBind)                       \
  F(IndexIteratorScanKey, indexIteratorScanKey)                 \
  F(IndexIteratorAdvance, indexIteratorAdvance)                 \
  F(IndexIteratorGetTinyInt, indexIteratorGetTinyInt)         \
  F(IndexIteratorGetSmallInt, indexIteratorGetSmallInt)         \
  F(IndexIteratorGetInt, indexIteratorGetInt)                   \
  F(IndexIteratorGetBigInt, indexIteratorGetBigInt)             \
  F(IndexIteratorGetReal, indexIteratorGetReal)                 \
  F(IndexIteratorGetDouble, indexIteratorGetDouble)             \
  F(IndexIteratorGetTinyIntNull, indexIteratorGetTinyIntNull)         \
  F(IndexIteratorGetSmallIntNull, indexIteratorGetSmallIntNull)         \
  F(IndexIteratorGetIntNull, indexIteratorGetIntNull)                   \
  F(IndexIteratorGetBigIntNull, indexIteratorGetBigIntNull)             \
  F(IndexIteratorGetRealNull, indexIteratorGetRealNull)                 \
  F(IndexIteratorGetDoubleNull, indexIteratorGetDoubleNull)             \
  F(IndexIteratorSetKeyTinyInt, indexIteratorSetKeyTinyInt)         \
  F(IndexIteratorSetKeySmallInt, indexIteratorSetKeySmallInt)         \
  F(IndexIteratorSetKeyInt, indexIteratorSetKeyInt)                   \
  F(IndexIteratorSetKeyBigInt, indexIteratorSetKeyBigInt)             \
  F(IndexIteratorSetKeyReal, indexIteratorSetKeyReal)                 \
  F(IndexIteratorSetKeyDouble, indexIteratorSetKeyDouble)             \
  F(IndexIteratorFree, indexIteratorFree)                       \
                                                                \
  /* Insert */                                                  \
  F(Insert, insert)

/**
 * Enum of builtins
 */
enum class Builtin : u8 {
#define ENTRY(Name, ...) Name,
  BUILTINS_LIST(ENTRY)
#undef ENTRY
#define COUNT_OP(inst, ...) +1
      Last = -1 BUILTINS_LIST(COUNT_OP)
#undef COUNT_OP
};

/**
 * Manages builtin functions
 */
class Builtins {
 public:
  /**
   * The total number of builtin functions
   */
  static const u32 kBuiltinsCount = static_cast<u32>(Builtin ::Last) + 1;

  /**
   * @return the total number of bytecodes
   */
  static constexpr u32 NumBuiltins() { return kBuiltinsCount; }

  /**
   * Return the name of the builtin
   * @param builtin builtin to retrieve
   * @return name of the builtin function
   */
  static const char *GetFunctionName(Builtin builtin) { return kBuiltinFunctionNames[static_cast<u8>(builtin)]; }

 private:
  static const char *kBuiltinFunctionNames[];
};

}  // namespace tpl::ast
