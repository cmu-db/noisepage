#pragma once

#include <functional>

#include "execution/util/common.h"
#include "execution/util/simd.h"

namespace tpl::util {

/**
 * Utility class containing vectorized operations.
 */
class VectorUtil {
 public:
  /**
   * Force only static functions.
   */
  VectorUtil() = delete;

  /**
   * Filter an input vector by a constant value and store the indexes of valid
   * elements in the output vector. If a selection vector is provided, only
   * vector elements from the selection vector will be read.
   * @tparam T The data type of the elements stored in the input vector.
   * @tparam Op The filter comparison operation.
   * @param in The input vector.
   * @param in_count The number of elements in the input (or selection) vector.
   * @param val The constant value to compare with.
   * @param[out] out The vector storing indexes of valid input elements.
   * @param sel The selection vector used to read input values.
   * @return The number of elements that pass the filter.
   */
  template <typename T, template <typename> typename Op>
  static u32 FilterVectorByVal(const T *RESTRICT in, const u32 in_count, const T val, u32 *RESTRICT out,
                               const u32 *RESTRICT sel) {
    // Simple check to make sure the provided filter operation returns bool
    static_assert(std::is_same_v<bool, std::invoke_result_t<Op<T>, T, T>>);

    u32 in_pos = 0;
    u32 out_pos = simd::FilterVectorByVal<T, Op>(in, in_count, val, out, sel, &in_pos);

    if (sel == nullptr) {
      for (; in_pos < in_count; in_pos++) {
        bool cmp = Op<T>()(in[in_pos], val);
        out[out_pos] = in_pos;
        out_pos += static_cast<u32>(cmp);
      }
    } else {
      for (; in_pos < in_count; in_pos++) {
        bool cmp = Op<T>()(in[sel[in_pos]], val);
        out[out_pos] = sel[in_pos];
        out_pos += static_cast<u32>(cmp);
      }
    }

    return out_pos;
  }

  /**
   * Filter an input vector by the values in a second input vector, and store
   * indexes of the valid elements (zero-based) into an output vector. If a
   * selection vector is provided, only the vector elements whose indexes are in
   * the selection vector will be read.
   * @tparam T The data type of the elements stored in the input vector.
   * @tparam Op The filter operation.
   * @param in_1 The first input vector.
   * @param in_2 The second input vector.
   * @param in_count The number of elements in the input (or selection) vector.
   * @param[out] out The vector storing the indexes of the valid input elements.
   * @param sel The selection vector storing indexes of elements to process.
   * @return The number of elements that pass the filter.
   */
  template <typename T, template <typename> typename Op>
  static u32 FilterVectorByVector(const T *RESTRICT in_1, const T *RESTRICT in_2, const u32 in_count, u32 *RESTRICT out,
                                  const u32 *RESTRICT sel) {
    // Simple check to make sure the provided filter operation returns bool
    static_assert(std::is_same_v<bool, std::invoke_result_t<Op<T>, T, T>>);

    u32 in_pos = 0;
    u32 out_pos = simd::FilterVectorByVector<T, Op>(in_1, in_2, in_count, out, sel, &in_pos);

    if (sel == nullptr) {
      for (; in_pos < in_count; in_pos++) {
        bool cmp = Op<T>()(in_1[in_pos], in_2[in_pos]);
        out[out_pos] = in_pos;
        out_pos += static_cast<u32>(cmp);
      }
    } else {
      for (; in_pos < in_count; in_pos++) {
        bool cmp = Op<T>()(in_1[sel[in_pos]], in_2[sel[in_pos]]);
        out[out_pos] = sel[in_pos];
        out_pos += static_cast<u32>(cmp);
      }
    }

    return out_pos;
  }

  // -------------------------------------------------------
  // Generate specialized vectorized filters
  // -------------------------------------------------------

#define GEN_FILTER(Op, Comparison)                                                                         \
  template <typename T>                                                                                    \
  static u32 Filter##Op(const T *RESTRICT in, u32 in_count, T val, u32 *RESTRICT out, u32 *RESTRICT sel) { \
    return FilterVectorByVal<T, Comparison>(in, in_count, val, out, sel);                                  \
  }                                                                                                        \
  template <typename T>                                                                                    \
  static u32 Filter##Op(const T *RESTRICT in_1, const T *RESTRICT in_2, u32 in_count, u32 *RESTRICT out,   \
                        u32 *RESTRICT sel) {                                                               \
    return FilterVectorByVector<T, Comparison>(in_1, in_2, in_count, out, sel);                            \
  }
  GEN_FILTER(Eq, std::equal_to)
  GEN_FILTER(Gt, std::greater)
  GEN_FILTER(Ge, std::greater_equal)
  GEN_FILTER(Lt, std::less)
  GEN_FILTER(Le, std::less_equal)
  GEN_FILTER(Ne, std::not_equal_to)
#undef GEN_FILTER
};

}  // namespace tpl::util
