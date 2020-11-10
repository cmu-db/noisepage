#include <sys/mman.h>

#include <algorithm>
#include <random>
#include <utility>
#include <vector>

#include "execution/sql/sql.h"
#include "execution/tpl_test.h"
#include "execution/util/bit_vector.h"
#include "execution/util/fast_rand.h"
#include "execution/util/simd.h"
#include "execution/util/timer.h"
#include "execution/util/vector_util.h"

namespace noisepage::execution::util {

class VectorUtilTest : public TplTest {};

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, Access) {
  {
    simd::Vec8 in(44);
    for (uint32_t i = 0; i < simd::Vec8::Size(); i++) {
      EXPECT_EQ(44, in[i]);
    }
  }

  {
    int32_t base = 44;
    simd::Vec8 in(base, base + 1, base + 2, base + 3, base + 4, base + 5, base + 6, base + 7);
    for (int32_t i = 0; i < static_cast<int32_t>(simd::Vec8::Size()); i++) {
      EXPECT_EQ(base + i, in[i]);
    }
  }
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, Arithmetic) {
  // Addition
  {
    simd::Vec8 v(1, 2, 3, 4, 5, 6, 7, 8);
    simd::Vec8 add = v + simd::Vec8(10);
    for (int32_t i = 0; i < 8; i++) {
      EXPECT_EQ(10 + (i + 1), add[i]);
    }

    // Update the original vector
    v += simd::Vec8(10);
    for (int32_t i = 0; i < 8; i++) {
      EXPECT_EQ(10 + (i + 1), v[i]);
    }
  }

  // Subtraction
  {
    simd::Vec8 v(11, 12, 13, 14, 15, 16, 17, 18);
    simd::Vec8 sub = v - simd::Vec8(10);
    for (int32_t i = 0; i < 8; i++) {
      EXPECT_EQ(i + 1, sub[i]);
    }

    // Update the original vector
    v -= simd::Vec8(10);
    for (int32_t i = 0; i < 8; i++) {
      EXPECT_EQ(i + 1, v[i]);
    }
  }

  // Multiplication
  {
    simd::Vec8 v(1, 2, 3, 4, 5, 6, 7, 8);
    simd::Vec8 add = v * simd::Vec8(10);
    for (int32_t i = 0; i < 8; i++) {
      EXPECT_EQ(10 * (i + 1), add[i]);
    }

    // Update the original vector
    v *= simd::Vec8(10);
    for (int32_t i = 0; i < 8; i++) {
      EXPECT_EQ(10 * (i + 1), v[i]);
    }
  }
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, BitwiseOperations) {
  // Left shift
  {
    simd::Vec8 v(1, 2, 3, 4, 5, 6, 7, 8);
    simd::Vec8 left_shift = v << 2;
    for (int32_t i = 0; i < 8; i++) {
      EXPECT_EQ(v[i] * 4, left_shift[i]);
    }

    // Update the original vector
    v <<= 2;
    for (int32_t i = 0; i < 8; i++) {
      EXPECT_EQ(left_shift[i], v[i]);
    }
  }

  // Right shift
  {
    simd::Vec8 v(4, 8, 16, 32, 64, 128, 256, 512);
    simd::Vec8 right_shift = v >> 1;
    for (int32_t i = 0; i < 8; i++) {
      EXPECT_EQ(v[i] / 2, right_shift[i]);
    }

    // Update the original vector
    v >>= 1;
    for (int32_t i = 0; i < 8; i++) {
      EXPECT_EQ(right_shift[i], v[i]);
    }
  }
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, Comparisons) {
  // Equality
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    auto check = simd::Vec8(40);
    simd::Vec8Mask mask = in == check;
    EXPECT_FALSE(mask[0]);
    EXPECT_FALSE(mask[1]);
    EXPECT_FALSE(mask[2]);
    EXPECT_TRUE(mask[3]);
    EXPECT_FALSE(mask[4]);
    EXPECT_FALSE(mask[5]);
    EXPECT_FALSE(mask[6]);
    EXPECT_FALSE(mask[7]);
  }

  // Greater Than
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    auto check = simd::Vec8(40);
    simd::Vec8Mask mask = in > check;
    EXPECT_FALSE(mask[0]);
    EXPECT_FALSE(mask[1]);
    EXPECT_FALSE(mask[2]);
    EXPECT_FALSE(mask[3]);
    EXPECT_TRUE(mask[4]);
    EXPECT_TRUE(mask[5]);
    EXPECT_TRUE(mask[6]);
    EXPECT_TRUE(mask[7]);
  }

  // Greater Than Equal
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    auto check = simd::Vec8(40);
    simd::Vec8Mask mask = in >= check;
    EXPECT_FALSE(mask[0]);
    EXPECT_FALSE(mask[1]);
    EXPECT_FALSE(mask[2]);
    EXPECT_TRUE(mask[3]);
    EXPECT_TRUE(mask[4]);
    EXPECT_TRUE(mask[5]);
    EXPECT_TRUE(mask[6]);
    EXPECT_TRUE(mask[7]);
  }

  // Less Than
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    auto check = simd::Vec8(40);
    simd::Vec8Mask mask = in < check;
    EXPECT_TRUE(mask[0]);
    EXPECT_TRUE(mask[1]);
    EXPECT_TRUE(mask[2]);
    EXPECT_FALSE(mask[3]);
    EXPECT_FALSE(mask[4]);
    EXPECT_FALSE(mask[5]);
    EXPECT_FALSE(mask[6]);
    EXPECT_FALSE(mask[7]);
  }

  // Less Than Equal
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    auto check = simd::Vec8(40);
    simd::Vec8Mask mask = (in <= check);
    EXPECT_TRUE(mask[0]);
    EXPECT_TRUE(mask[1]);
    EXPECT_TRUE(mask[2]);
    EXPECT_TRUE(mask[3]);
    EXPECT_FALSE(mask[4]);
    EXPECT_FALSE(mask[5]);
    EXPECT_FALSE(mask[6]);
    EXPECT_FALSE(mask[7]);
  }

  // Not Equal
  {
    simd::Vec8 in(10, 20, 30, 40, 50, 60, 70, 80);
    auto check = simd::Vec8(40);
    simd::Vec8Mask mask = (in == check);
    EXPECT_FALSE(mask[0]);
    EXPECT_FALSE(mask[1]);
    EXPECT_FALSE(mask[2]);
    EXPECT_TRUE(mask[3]);
    EXPECT_FALSE(mask[4]);
    EXPECT_FALSE(mask[5]);
    EXPECT_FALSE(mask[6]);
    EXPECT_FALSE(mask[7]);
  }
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, MaskToPosition) {
  simd::Vec8 vec(3, 0, 4, 1, 5, 2, 6, 2);
  simd::Vec8 val(2);

  simd::Vec8Mask mask = vec > val;

  alignas(64) uint32_t positions[8] = {0};

  uint32_t count = mask.ToPositions(positions, 0);
  EXPECT_EQ(4u, count);

  EXPECT_EQ(0u, positions[0]);
  EXPECT_EQ(2u, positions[1]);
  EXPECT_EQ(4u, positions[2]);
  EXPECT_EQ(6u, positions[3]);
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, ByteToSelectionVector) {
  constexpr uint32_t n = 14;
  uint8_t bytes[n] = {0xFF, 0x00, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0xFF, 0x00, 0xFF, 0xFF, 0xFF, 0x00};
  sel_t sel[n];

  uint32_t size = VectorUtil::ByteVectorToSelectionVector(bytes, n, sel);
  EXPECT_EQ(8u, size);
  EXPECT_EQ(0u, sel[0]);
  EXPECT_EQ(3u, sel[1]);
  EXPECT_EQ(5u, sel[2]);
  EXPECT_EQ(7u, sel[3]);
  EXPECT_EQ(8u, sel[4]);
  EXPECT_EQ(10u, sel[5]);
  EXPECT_EQ(11u, sel[6]);
  EXPECT_EQ(12u, sel[7]);
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, BitToByteVector) {
  // Large enough to run through both vector-loop and scalar tail
  constexpr uint32_t num_bits = 97;

  // The input bit vector output byte vector
  BitVector bv(num_bits);
  uint8_t bytes[num_bits];

  // Set some random bits
  bv.Set(1);
  bv.Set(2);
  bv.Set(7);
  bv.Set(31);
  bv.Set(44);
  bv.Set(73);

  util::VectorUtil::BitVectorToByteVector(bv.GetWords(), bv.GetNumBits(), bytes);

  for (uint32_t i = 0; i < bv.GetNumBits(); i++) {
    EXPECT_EQ(bv[i], bytes[i] == 0xFF);
  }
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, BitToSelectionVector) {
  // 126-bit vector and the output selection vector
  constexpr uint32_t num_bits = 126;
  BitVector bv(num_bits);
  sel_t sel[common::Constants::K_DEFAULT_VECTOR_SIZE];

  // Set even bits
  for (uint32_t i = 0; i < num_bits; i++) {
    bv.Set(i, i % 2 == 0);
  }

  // Transform
  uint32_t size = util::VectorUtil::BitVectorToSelectionVector(bv.GetWords(), num_bits, sel);

  // Only 63 bits are set (remember there are only 126-bits)
  EXPECT_EQ(63u, size);

  // Ensure the indexes that are set are even
  for (uint32_t i = 0; i < size; i++) {
    EXPECT_EQ(0u, sel[i] % 2);
  }
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, BitToSelectionVector_Sparse_vs_Dense) {
  for (uint32_t density : {0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100}) {
    // Create a bit vector with specific density
    BitVector bv(common::Constants::K_DEFAULT_VECTOR_SIZE);
    std::random_device r;
    for (uint32_t i = 0; i < common::Constants::K_DEFAULT_VECTOR_SIZE; i++) {
      if (r() % 100 < density) {
        bv[i] = true;
      }
    }

    sel_t sel_1[common::Constants::K_DEFAULT_VECTOR_SIZE], sel_2[common::Constants::K_DEFAULT_VECTOR_SIZE];

    // Ensure both sparse and dense implementations produce the same output
    const uint32_t size_1 = util::VectorUtil::BitVectorToSelectionVectorSparse(bv.GetWords(), bv.GetNumBits(), sel_1);
    const uint32_t size_2 = util::VectorUtil::BitVectorToSelectionVectorDense(bv.GetWords(), bv.GetNumBits(), sel_2);

    ASSERT_EQ(size_1, size_2);
    ASSERT_TRUE(std::equal(sel_1, sel_1 + size_1, sel_2, sel_2 + size_2));
  }
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, DiffSelected) {
  sel_t input[common::Constants::K_DEFAULT_VECTOR_SIZE] = {0, 2, 3, 5, 7, 9};
  sel_t output[common::Constants::K_DEFAULT_VECTOR_SIZE];
  uint32_t out_count = VectorUtil::DiffSelectedScalar(10, input, 6, output);
  EXPECT_EQ(4u, out_count);
  EXPECT_EQ(1u, output[0]);
  EXPECT_EQ(4u, output[1]);
  EXPECT_EQ(6u, output[2]);
  EXPECT_EQ(8u, output[3]);
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, DiffSelectedWithScratchPad) {
  sel_t input[common::Constants::K_DEFAULT_VECTOR_SIZE] = {2, 3, 5, 7, 9};
  sel_t output[common::Constants::K_DEFAULT_VECTOR_SIZE];
  uint8_t scratch[common::Constants::K_DEFAULT_VECTOR_SIZE];

  auto count = VectorUtil::DiffSelectedWithScratchPad(10, input, 5, output, scratch);
  EXPECT_EQ(5u, count);
  EXPECT_EQ(0u, output[0]);
  EXPECT_EQ(1u, output[1]);
  EXPECT_EQ(4u, output[2]);
  EXPECT_EQ(6u, output[3]);
  EXPECT_EQ(8u, output[4]);
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, IntersectSelectionVectors) {
  sel_t a[] = {2, 3, 5, 7, 9};
  sel_t b[] = {1, 2, 4, 7, 8, 9, 10, 11};
  sel_t out[common::Constants::K_DEFAULT_VECTOR_SIZE];

  auto out_count = VectorUtil::IntersectSelected(a, sizeof(a) / sizeof(a[0]), b, sizeof(b) / sizeof(b[0]), out);
  EXPECT_EQ(3u, out_count);
  EXPECT_EQ(2u, out[0]);
  EXPECT_EQ(7u, out[1]);
  EXPECT_EQ(9u, out[2]);

  // Reverse arguments, should still work
  out_count = VectorUtil::IntersectSelected(b, sizeof(b) / sizeof(b[0]), a, sizeof(a) / sizeof(a[0]), out);
  EXPECT_EQ(3u, out_count);
  EXPECT_EQ(2u, out[0]);
  EXPECT_EQ(7u, out[1]);
  EXPECT_EQ(9u, out[2]);

  // Empty arguments should work
  out_count = VectorUtil::IntersectSelected(nullptr, 0, b, sizeof(b) / sizeof(b[0]), out);
  EXPECT_EQ(0u, out_count);

  out_count = VectorUtil::IntersectSelected(b, sizeof(b) / sizeof(b[0]), static_cast<sel_t *>(nullptr), 0, out);
  EXPECT_EQ(0u, out_count);
}
}  // namespace noisepage::execution::util
