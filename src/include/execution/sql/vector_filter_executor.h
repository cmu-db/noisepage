#pragma once

#include <functional>
#include <vector>

#include "execution/sql/constant_vector.h"
#include "execution/sql/generic_value.h"
#include "execution/sql/sql.h"
#include "execution/sql/tuple_id_list.h"
#include "execution/sql/vector_operations/vector_operations.h"
#include "execution/sql/vector_projection.h"
#include "execution/sql/vector_projection_iterator.h"

namespace noisepage::execution::sql {

/**
 * This is a helper class to execute filters over vector projections.
 */
class VectorFilterExecutor {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(VectorFilterExecutor);
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(VectorFilterExecutor);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are equal to the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectEqualVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                             uint32_t col_idx, const GenericValue &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are equal to the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectEqualVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                             uint32_t col_idx, const Val &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are greater than or equal to the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectGreaterThanEqualVal(const exec::ExecutionSettings &exec_settings,
                                        VectorProjection *vector_projection, uint32_t col_idx, const GenericValue &val,
                                        TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are greater than or equal to the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectGreaterThanEqualVal(const exec::ExecutionSettings &exec_settings,
                                        VectorProjection *vector_projection, uint32_t col_idx, const Val &val,
                                        TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are strictly greater than the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectGreaterThanVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                                   uint32_t col_idx, const GenericValue &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are strictly greater than the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectGreaterThanVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                                   uint32_t col_idx, const Val &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are less than or equal to the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectLessThanEqualVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                                     uint32_t col_idx, const GenericValue &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are less than or equal to the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectLessThanEqualVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                                     uint32_t col_idx, const Val &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are strictly less than the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectLessThanVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                                uint32_t col_idx, const GenericValue &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are strictly less than the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectLessThanVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                                uint32_t col_idx, const Val &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are not equal to the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectNotEqualVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                                uint32_t col_idx, const GenericValue &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are not equal to the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectNotEqualVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                                uint32_t col_idx, const Val &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are like the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectLikeVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                            uint32_t col_idx, const Val &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are like the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectLikeVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                            uint32_t col_idx, const GenericValue &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are not like the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectNotLikeVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                               uint32_t col_idx, const Val &val, TupleIdList *tid_list);

  /**
   * Select tuples in the column stored at the given index (@em col_idx) in the vector projection
   * that are not like the provided constant value (@em val).
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param col_idx The index of the column to compare with.
   * @param val The value to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectNotLikeVal(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                               uint32_t col_idx, const GenericValue &val, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are equal to the values in the right
   * (second) column.
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectEqual(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                          uint32_t left_col_idx, uint32_t right_col_idx, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are greater than or equal to the values
   * in the right (second) column.
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectGreaterThanEqual(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                                     uint32_t left_col_idx, uint32_t right_col_idx, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are greater than the values in the right
   * (second) column.
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectGreaterThan(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                                uint32_t left_col_idx, uint32_t right_col_idx, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are less than or equal to the values in
   * the right (second) column.
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectLessThanEqual(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                                  uint32_t left_col_idx, uint32_t right_col_idx, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are less than the values in the right
   * (second) column.
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectLessThan(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                             uint32_t left_col_idx, uint32_t right_col_idx, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are not equal to the values in the right
   * (second) column.
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectNotEqual(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                             uint32_t left_col_idx, uint32_t right_col_idx, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are like the values in the right
   * (second) column.
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectLike(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                         uint32_t left_col_idx, uint32_t right_col_idx, TupleIdList *tid_list);

  /**
   * Select tuples whose values in the left (first) column are not like the values in the right
   * (second) column.
   * @param exec_settings The execution settings to use.
   * @param vector_projection The vector projection to run on.
   * @param left_col_idx The index of the left column to compare with.
   * @param right_col_idx The index of the right column to compare with.
   * @param tid_list The tuple ID list to use.
   */
  static void SelectNotLike(const exec::ExecutionSettings &exec_settings, VectorProjection *vector_projection,
                            uint32_t left_col_idx, uint32_t right_col_idx, TupleIdList *tid_list);
};

// ---------------------------------------------------------
//
// Implementation
//
// ---------------------------------------------------------

#define GEN_FILTER_VECTOR_GENERIC_VAL(OpName)                                                                \
  inline void VectorFilterExecutor::OpName##Val(const exec::ExecutionSettings &exec_settings,                \
                                                VectorProjection *vector_projection, const uint32_t col_idx, \
                                                const GenericValue &val, TupleIdList *tid_list) {            \
    const auto *left_vector = vector_projection->GetColumn(col_idx);                                         \
    VectorOps::OpName(exec_settings, *left_vector, ConstantVector(val), tid_list);                           \
  }

#define GEN_FILTER_VECTOR_VAL(OpName)                                                                        \
  inline void VectorFilterExecutor::OpName##Val(const exec::ExecutionSettings &exec_settings,                \
                                                VectorProjection *vector_projection, const uint32_t col_idx, \
                                                const Val &val, TupleIdList *tid_list) {                     \
    const auto *left_vector = vector_projection->GetColumn(col_idx);                                         \
    const auto constant = GenericValue::CreateFromRuntimeValue(left_vector->GetTypeId(), val);               \
    VectorOps::OpName(exec_settings, *left_vector, ConstantVector(constant), tid_list);                      \
  }

#define GEN_FILTER_VECTOR_VECTOR(OpName)                                                                     \
  inline void VectorFilterExecutor::OpName(const exec::ExecutionSettings &exec_settings,                     \
                                           VectorProjection *vector_projection, const uint32_t left_col_idx, \
                                           const uint32_t right_col_idx, TupleIdList *tid_list) {            \
    const auto *left_vector = vector_projection->GetColumn(left_col_idx);                                    \
    const auto *right_vector = vector_projection->GetColumn(right_col_idx);                                  \
    VectorOps::OpName(exec_settings, *left_vector, *right_vector, tid_list);                                 \
  }

#define GEN_FILTER(OpName)              \
  GEN_FILTER_VECTOR_GENERIC_VAL(OpName) \
  GEN_FILTER_VECTOR_VAL(OpName)         \
  GEN_FILTER_VECTOR_VECTOR(OpName)

GEN_FILTER(SelectEqual);
GEN_FILTER(SelectGreaterThan);
GEN_FILTER(SelectGreaterThanEqual);
GEN_FILTER(SelectLessThan);
GEN_FILTER(SelectLessThanEqual);
GEN_FILTER(SelectNotEqual);
GEN_FILTER(SelectLike);
GEN_FILTER(SelectNotLike);

#undef GEN_FILTER
#undef GEN_FILTER_VECTOR_VECTOR
#undef GEN_FILTER_VECTOR_VAL
#undef GEN_FILTER_VECTOR_GENERIC_VAL

}  // namespace noisepage::execution::sql
