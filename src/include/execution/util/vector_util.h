#pragma once

#include <cstddef>

#include "common/macros.h"
#include "execution/util/execution_common.h"

namespace noisepage::execution::util {

/**
 * Utility class containing vectorized operations.
 */
class VectorUtil {
 public:
  /** This class cannot be instantiated. */
  DISALLOW_INSTANTIATION(VectorUtil);
  /** This class cannot be copied or moved. */
  DISALLOW_COPY_AND_MOVE(VectorUtil);

  /**
   * Intersect the sorted selection vectors @em sel_vector_1 with @em sel_vector_2 with lengths @em sel_vector_1_len and
   * @em sel_vector_2_len, respectively. Store the result in @em out_sel_vector. The function returns the number of
   * elements in the output selection vector.
   *
   * @param sel_vector_1 The fi
   *
   * rst input selection vector.
   * @param sel_vector_1_len The length of the first input selection vector.
   * @param sel_vector_2 The second input selection vector.
   * @param sel_vector_2_len The length of the second input selection vector.
   * @param out_sel_vector The output selection vector storing the result of the intersection.
   * @return The number of elements in the output selection vector.
   */
  [[nodiscard]] static uint32_t IntersectSelected(const sel_t *sel_vector_1, uint32_t sel_vector_1_len,
                                                  const sel_t *sel_vector_2, uint32_t sel_vector_2_len,
                                                  sel_t *out_sel_vector);

  /**
   * Intersect the sorted selection vector @em sel_vector whose length is @em sel_vector_len with the bit vector
   * @em bit_vector of @em bit_vector_len bits. Store the result in @em out_sel_vector.
   *
   * @param sel_vector The input selection vector.
   * @param sel_vector_len The length of the input selection vector.
   * @param bit_vector The input bit vector.
   * @param bit_vector_len The length of the bit vector in bits.
   * @param[out] out_sel_vector The output selection vector storing the result of the intersection.
   * @return The number of elements in the output selection vector.
   */
  [[nodiscard]] static uint32_t IntersectSelected(const sel_t *sel_vector, uint32_t sel_vector_len,
                                                  const uint64_t *bit_vector, uint32_t bit_vector_len,
                                                  sel_t *out_sel_vector);

  /**
   * Populate the output selection vector @em out_sel_vector with all indexes that do not appear in the input selection
   * vector @em sel_vector.
   *
   * @param n The maximum number of indexes that can appear in the selection vector.
   * @param sel_vector The input selection vector.
   * @param sel_vector_len The number of elements in the input selection vector.
   * @param[out] out_sel_vector The output selection vector.
   * @return The number of elements in the output selection vector.
   */
  [[nodiscard]] static uint32_t DiffSelected(uint32_t n, const sel_t *sel_vector, uint32_t sel_vector_len,
                                             sel_t *out_sel_vector);

  /**
   * Convert a selection vector into a byte vector. For each index in @em sel_vector set the corresponding byte in the
   * @em byte_vector to the saturated 8-bit integer 0xFF.
   *
   * @pre The size of the selection vector and byte output vector must match!
   *
   * @param num_elems The number of elements in the selection vector.
   * @param sel_vector The input selection index vector.
   * @param[out] byte_vector The output byte vector.
   */
  static void SelectionVectorToByteVector(const sel_t *sel_vector, uint32_t num_elems, uint8_t *byte_vector);

  /**
   * Convert a byte vector into a selection vector. Store the indexes positions of all saturated byte values 0xFF into
   * the selection vector @em sel_vector.
   *
   * @pre The capacity of the output selection vector must be at least that of the byte vector.
   *
   * @param num_bytes The number of elements in the byte vector.
   * @param byte_vector The input byte vector.
   * @param[out] sel_vector The output selection vector.
   * @return The number of elements in the selection vector.
   */
  [[nodiscard]] static uint32_t ByteVectorToSelectionVector(const uint8_t *byte_vector, uint32_t num_bytes,
                                                            sel_t *sel_vector);

  /**
   * Convert a byte vector to a bit vector. Set the corresponding bit in the bit vector @em bit_vector of all positions
   * in the byte vector containing a saturated byte value 0xFF.
   *
   * @pre The bit vector capacity must be at least that of the byte vector.
   *
   * @param num_bytes The number of elements in the byte vector.
   * @param byte_vector The input byte vector.
   * @param[out] bit_vector The output bit vector.
   */
  static void ByteVectorToBitVector(const uint8_t *byte_vector, uint32_t num_bytes, uint64_t *bit_vector);

  /**
   * Convert a bit vector into a byte vector. For all set bits in the input bit vector, set the corresponding byte to a
   * saturated 8-bit integer.
   *
   * @pre The capacity of the output byte vector must be at least that of the bit vector.
   *
   * @param num_bits The number of bits in the bit vector.
   * @param bit_vector The input bit vector, passed along as an array of words.
   * @param byte_vector The output byte vector.
   */
  static void BitVectorToByteVector(const uint64_t *bit_vector, uint32_t num_bits, uint8_t *byte_vector);

  /**
   * Convert a bit vector into a densely packed selection vector. Extract the indexes of all set (1) bits and store into
   * the output selection vector. The resulting selection vector is guaranteed to be sorted ascending.
   *
   * NOTE: Use this if you do not know the density of the bit vector. Otherwise, use the sparse or dense implementations
   *       below which are optimized as appropriate.
   *
   * @param bit_vector The input bit vector.
   * @param num_bits The number of bits in the bit vector. This must match the maximum capacity of
   *                 the output selection vector!
   * @param[out] sel_vector The output selection vector.
   * @return The number of elements in the selection vector.
   */
  [[nodiscard]] static uint32_t BitVectorToSelectionVector(const uint64_t *bit_vector, uint32_t num_bits,
                                                           sel_t *sel_vector);

 private:
  FRIEND_TEST(VectorUtilTest, BitToSelectionVector_Sparse_vs_Dense);
  FRIEND_TEST(VectorUtilTest, DiffSelected);
  FRIEND_TEST(VectorUtilTest, DiffSelectedWithScratchPad);
  FRIEND_TEST(VectorUtilTest, IntersectScalar);
  FRIEND_TEST(VectorUtilTest, PerfIntersectSelected);

  [[nodiscard]] static uint32_t BitVectorToSelectionVectorSparse(const uint64_t *bit_vector, uint32_t num_bits,
                                                                 sel_t *sel_vector);

  [[nodiscard]] static uint32_t BitVectorToSelectionVectorDense(const uint64_t *bit_vector, uint32_t num_bits,
                                                                sel_t *sel_vector);

  [[nodiscard]] static uint32_t BitVectorToSelectionVectorDenseAvX2(const uint64_t *bit_vector, uint32_t num_bits,
                                                                    sel_t *sel_vector);

  [[nodiscard]] static uint32_t BitVectorToSelectionVectorDenseAVX512(const uint64_t *bit_vector, uint32_t num_bits,
                                                                      sel_t *sel_vector);

  /** A sorted-set difference implementation using purely scalar operations. */
  [[nodiscard]] static uint32_t DiffSelectedScalar(uint32_t n, const sel_t *sel_vector, uint32_t m,
                                                   sel_t *out_sel_vector);

  /**
   * A sorted-set difference implementation that uses a little extra memory (the scratchpad) and executes more
   * instructions, but has better CPI, and is faster in the common case.
   */
  [[nodiscard]] static uint32_t DiffSelectedWithScratchPad(uint32_t n, const sel_t *sel_vector, uint32_t sel_vector_len,
                                                           sel_t *out_sel_vector, uint8_t *scratch);
};

}  // namespace noisepage::execution::util
