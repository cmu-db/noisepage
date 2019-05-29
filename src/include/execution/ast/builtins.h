#pragma once

#include "execution/util/common.h"

namespace tpl::ast {

#define BUILTINS_LIST(F)                        \
  /* Primitive <-> SQL */                       \
  F(IntToSql, intToSql)                         \
  F(BoolToSql, boolToSql)                       \
  F(FloatToSql, floatToSql)                     \
  F(SqlToBool, sqlToBool)                       \
                                                \
  /* Vectorized Filters */                      \
  F(FilterEq, filterEq)                         \
  F(FilterGe, filterGe)                         \
  F(FilterGt, filterGt)                         \
  F(FilterLe, filterLe)                         \
  F(FilterLt, filterLt)                         \
  F(FilterNe, filterNe)                         \
                                                \
  /* Region Allocator */                        \
  F(RegionInit, regionInit)                     \
  F(RegionFree, regionFree)                     \
                                                \
  /* Joins */                                   \
  F(JoinHashTableInit, joinHTInit)              \
  F(JoinHashTableInsert, joinHTInsert)          \
  F(JoinHashTableBuild, joinHTBuild)            \
  F(JoinHashTableFree, joinHTFree)              \
                                                \
  /* Sorting */                                 \
  F(SorterInit, sorterInit)                     \
  F(SorterInsert, sorterInsert)                 \
  F(SorterSort, sorterSort)                     \
  F(SorterFree, sorterFree)                     \
                                                \
  /* Trig */                                    \
  F(ACos, acos)                                 \
  F(ASin, asin)                                 \
  F(ATan, atan)                                 \
  F(ATan2, atan2)                               \
  F(Cos, cos)                                   \
  F(Cot, cot)                                   \
  F(Sin, sin)                                   \
  F(Tan, tan)                                   \
                                                \
  /* Generic */                                 \
  F(Map, map)                                   \
  F(Fold, fold)                                 \
  F(Gather, gather)                             \
  F(Scatter, scatter)                           \
  F(Compress, compress)                         \
  F(SizeOf, sizeOf)                             \
  F(PtrCast, ptrCast)                           \
                                                \
  /* Output Buffer */                           \
  F(OutputAlloc, outputAlloc)                   \
  F(OutputAdvance, outputAdvance)               \
  F(OutputSetNull, outputSetNull)               \
  F(OutputFinalize, outputFinalize)             \
                                                \
  /* Index */                                   \
  F(IndexIteratorInit, indexIteratorInit)       \
  F(IndexIteratorScanKey, indexIteratorScanKey) \
  F(IndexIteratorFree, indexIteratorFree)       \
                                                \
  /* Insert */                                  \
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
