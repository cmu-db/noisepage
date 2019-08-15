#pragma once

#include <functional>

#include "execution/util/execution_common.h"
#include "execution/util/simd.h"

namespace terrier::execution::util {

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
  static uint32_t FilterVectorByVal(const T *RESTRICT in, const uint32_t in_count, const T val, uint32_t *RESTRICT out,
                                    const uint32_t *RESTRICT sel) {
    // Simple check to make sure the provided filter operation returns bool
    static_assert(std::is_same_v<bool, std::invoke_result_t<Op<T>, T, T>>);

    uint32_t in_pos = 0;
#if defined(__AVX2__) || defined(__AVX512F__)
    uint32_t out_pos = simd::FilterVectorByVal<T, Op>(in, in_count, val, out, sel, &in_pos);
#else
    uint32_t out_pos = 0;
#endif

    if (sel == nullptr) {
      for (; in_pos < in_count; in_pos++) {
        bool cmp = Op<T>()(in[in_pos], val);
        out[out_pos] = in_pos;
        out_pos += static_cast<uint32_t>(cmp);
      }
    } else {
      for (; in_pos < in_count; in_pos++) {
        bool cmp = Op<T>()(in[sel[in_pos]], val);
        out[out_pos] = sel[in_pos];
        out_pos += static_cast<uint32_t>(cmp);
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
  static uint32_t FilterVectorByVector(const T *RESTRICT in_1, const T *RESTRICT in_2, const uint32_t in_count,
                                       uint32_t *RESTRICT out, const uint32_t *RESTRICT sel) {
    // Simple check to make sure the provided filter operation returns bool
    static_assert(std::is_same_v<bool, std::invoke_result_t<Op<T>, T, T>>);

    uint32_t in_pos = 0;
#if defined(__AVX2__) || defined(__AVX512F__)
    uint32_t out_pos = simd::FilterVectorByVector<T, Op>(in_1, in_2, in_count, out, sel, &in_pos);
#else
    uint32_t out_pos = 0;
#endif

    if (sel == nullptr) {
      for (; in_pos < in_count; in_pos++) {
        bool cmp = Op<T>()(in_1[in_pos], in_2[in_pos]);
        out[out_pos] = in_pos;
        out_pos += static_cast<uint32_t>(cmp);
      }
    } else {
      for (; in_pos < in_count; in_pos++) {
        bool cmp = Op<T>()(in_1[sel[in_pos]], in_2[sel[in_pos]]);
        out[out_pos] = sel[in_pos];
        out_pos += static_cast<uint32_t>(cmp);
      }
    }

    return out_pos;
  }

  /**
   * Gather potentially non-contiguous indexes from an input vector and store
   * them into an output vector. Only elements whose indexes are stored in the
   * index selection vector are gathered into the output vector.
   * @tparam T The data type of the input vector.
   * @param n The number of elements in the index selection vector.
   * @param input The input vector to read from.
   * @param indexes The index selection vector storing input vector indexes.
   * @param out The output vector where results are stored.
   * @return The number of elements that were gathered.
   */
  template <typename T>
  static uint32_t Gather(const uint32_t n, const T *RESTRICT input, const uint32_t *RESTRICT indexes, T *RESTRICT out) {
    TERRIER_ASSERT(input != nullptr, "Input cannot be null");
    TERRIER_ASSERT(indexes != nullptr, "Indexes vector cannot be null");

    for (std::size_t i = 0; i < n; i++) {
      out[i] = input[indexes[i]];
    }

    return n;
  }

  // -------------------------------------------------------
  // Generate specialized vectorized filters
  // -------------------------------------------------------

#define GEN_FILTER(Op, Comparison)                                                                   \
  template <typename T>                                                                              \
  static uint32_t Filter##Op(const T *RESTRICT in, uint32_t in_count, T val, uint32_t *RESTRICT out, \
                             uint32_t *RESTRICT sel) {                                               \
    return FilterVectorByVal<T, Comparison>(in, in_count, val, out, sel);                            \
  }                                                                                                  \
  template <typename T>                                                                              \
  static uint32_t Filter##Op(const T *RESTRICT in_1, const T *RESTRICT in_2, uint32_t in_count,      \
                             uint32_t *RESTRICT out, uint32_t *RESTRICT sel) {                       \
    return FilterVectorByVector<T, Comparison>(in_1, in_2, in_count, out, sel);                      \
  }
  GEN_FILTER(Eq, std::equal_to)
  GEN_FILTER(Gt, std::greater)
  GEN_FILTER(Ge, std::greater_equal)
  GEN_FILTER(Lt, std::less)
  GEN_FILTER(Le, std::less_equal)
  GEN_FILTER(Ne, std::not_equal_to)
#undef GEN_FILTER

  /**
   * Given a vector of pointers, insert the indexes of all null elements into
   * the selection vector @em out.
   * @tparam T The type of the elements in the vector.
   * @param in The input vector of pointers.
   * @param in_count The number of elements in the input vector.
   * @param out The output vector where results are stored.
   * @param sel The selection vector storing indexes of elements to process.
   * @return The number of null elements in the vector.
   */
  template <typename T>
  static auto SelectNull(const T *RESTRICT in, const uint32_t in_count, uint32_t *RESTRICT out, uint32_t *RESTRICT sel)
      -> std::enable_if_t<std::is_pointer_v<T>, uint32_t> {
    return FilterEq(reinterpret_cast<const intptr_t *>(in), in_count, intptr_t(0), out, sel);
  }

  /**
   * Given a vector of pointers, insert the indexes of all non-null elements
   * into the selection vector @em out.
   * @tparam T The type of the elements in the vector.
   * @param in The input vector of pointers.
   * @param in_count The number of elements in the input vector.
   * @param out The output vector where results are stored.
   * @param sel The selection vector storing indexes of elements to process.
   * @return The number of null elements in the vector.
   */
  template <typename T>
  static auto SelectNotNull(const T *RESTRICT in, const uint32_t in_count, uint32_t *RESTRICT out,
                            uint32_t *RESTRICT sel) -> std::enable_if_t<std::is_pointer_v<T>, uint32_t> {
    return FilterNe(reinterpret_cast<const intptr_t *>(in), in_count, intptr_t(0), out, sel);
  }
};

}  // namespace terrier::execution::util
