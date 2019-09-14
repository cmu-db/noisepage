#include <sys/mman.h>
#include <algorithm>
#include <random>
#include <utility>
#include <vector>

#include "execution/tpl_test.h"

#include "execution/sql/memory_pool.h"
#include "execution/util/timer.h"
#include "execution/util/vector_util.h"

namespace terrier::execution::util::test {

template <typename T>
class PoolArray {
 public:
  PoolArray() = default;
  PoolArray(sql::MemoryPool *memory, uint32_t num_elems)
      : memory_(memory),
        arr_(memory_->AllocateArray<T>(num_elems, common::Constants::CACHELINE_SIZE, true)),
        size_(num_elems) {}
  PoolArray(PoolArray &&other) noexcept : memory_(other.memory_), arr_(other.arr_), size_(other.size) {
    other.memory_ = nullptr;
    other.arr_ = nullptr;
    other.size_ = 0;
  }

  PoolArray &operator=(PoolArray &&other) noexcept {
    std::swap(memory_, other.memory_);
    std::swap(arr_, other.arr_);
    std::swap(size_, other.size_);
  }

  DISALLOW_COPY(PoolArray);

  ~PoolArray() {
    if (arr_ != nullptr) {
      TERRIER_ASSERT(memory_ != nullptr, "No memory pool to return to!");
      memory_->DeallocateArray(arr_, size_);
    }
    arr_ = nullptr;
    size_ = 0;
  }

  T &operator[](std::size_t n) { return arr_[n]; }

  T *Raw() { return arr_; }
  const T *Raw() const { return arr_; }
  uint32_t Size() { return size_; }

  T *Begin() { return Raw(); }
  T *End() { return Raw() + Size(); }

 private:
  sql::MemoryPool *memory_{nullptr};
  T *arr_{nullptr};
  uint32_t size_{0};
};

class VectorUtilTest : public TplTest {
 public:
  VectorUtilTest() : pool_(nullptr) {}

  template <typename T>
  PoolArray<T> AllocateArray(const uint32_t num_elems) {
    return PoolArray<T>(&pool_, num_elems);
  }

 private:
  sql::MemoryPool pool_;
};

#if defined(__AVX2__) || defined(__AVX512__)

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, AccessTest) {
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
TEST_F(VectorUtilTest, ArithmeticTest) {
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
TEST_F(VectorUtilTest, BitwiseOperationsTest) {
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
TEST_F(VectorUtilTest, ComparisonsTest) {
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
TEST_F(VectorUtilTest, MaskToPositionTest) {
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

#endif

template <typename T>
void SmallScaleNeedleTest() {
  static_assert(std::is_integral_v<T>, "This only works for integral types");

  constexpr const uint32_t num_elems = 4400;
  constexpr const uint32_t chunk_size = 1024;
  constexpr const T needle = 16;

  std::vector<T> arr(num_elems);

  uint32_t actual_count = 0;

  // Load
  {
    std::mt19937 gen;
    std::uniform_int_distribution<T> dist(0, 100);
    for (uint32_t i = 0; i < num_elems; i++) {
      arr[i] = dist(gen);
      if (arr[i] == needle) {
        actual_count++;
      }
    }
  }

  uint32_t out[chunk_size] = {0};

  uint32_t count = 0;
  for (uint32_t offset = 0; offset < num_elems; offset += chunk_size) {
    auto size = std::min(chunk_size, num_elems - offset);
    auto found = VectorUtil::FilterEq(&arr[offset], size, needle, out, nullptr);
    count += found;

    // Check each element in this vector
    for (uint32_t i = 0; i < found; i++) {
      EXPECT_EQ(needle, arr[offset + out[i]]);
    }
  }

  // Ensure total found through vector util matches what we generated
  EXPECT_EQ(actual_count, count);
}

template <typename T>
void SmallScaleMultiFilterTest() {
  static_assert(std::is_integral_v<T>, "This only works for integral types");

  constexpr const uint32_t num_elems = 4400;
  constexpr const uint32_t chunk_size = 1024;
  constexpr const T needle_1 = 16;
  constexpr const T needle_2 = 10;

  std::vector<T> arr_1(num_elems);
  std::vector<T> arr_2(num_elems);

  uint32_t actual_count = 0;

  // Load
  {
    std::mt19937 gen;
    std::uniform_int_distribution<T> dist(0, 100);
    for (uint32_t i = 0; i < num_elems; i++) {
      arr_1[i] = dist(gen);
      arr_2[i] = dist(gen);
      if (arr_1[i] < needle_1 && arr_2[i] < needle_2) {
        actual_count++;
      }
    }
  }

  alignas(common::Constants::CACHELINE_SIZE) uint32_t out[chunk_size] = {0};
  alignas(common::Constants::CACHELINE_SIZE) uint32_t sel[chunk_size] = {0};

  uint32_t count = 0;
  for (uint32_t offset = 0; offset < num_elems; offset += chunk_size) {
    auto size = std::min(chunk_size, num_elems - offset);

    // Apply first filter
    auto found = VectorUtil::FilterLt(&arr_1[offset], size, needle_1, sel, nullptr);

    // Apply second filter
    found = VectorUtil::FilterLt(&arr_2[offset], found, needle_2, out, sel);

    // Count
    count += found;

    // Check each element in this vector
    for (uint32_t i = 0; i < found; i++) {
      EXPECT_LT(arr_1[offset + out[i]], needle_1);
      EXPECT_LT(arr_2[offset + out[i]], needle_2);
    }
  }

  // Ensure total found through vector util matches what we generated
  EXPECT_EQ(actual_count, count);
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, SimpleFilterTest) {
  SmallScaleNeedleTest<int8_t>();
  SmallScaleNeedleTest<uint8_t>();
  SmallScaleNeedleTest<int16_t>();
  SmallScaleNeedleTest<uint16_t>();
  SmallScaleNeedleTest<int32_t>();
  SmallScaleNeedleTest<uint32_t>();
  SmallScaleNeedleTest<int64_t>();
  SmallScaleNeedleTest<uint64_t>();
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, MultiFilterTest) {
  SmallScaleMultiFilterTest<int8_t>();
  SmallScaleMultiFilterTest<uint8_t>();
  SmallScaleMultiFilterTest<int16_t>();
  SmallScaleMultiFilterTest<uint16_t>();
  SmallScaleMultiFilterTest<int32_t>();
  SmallScaleMultiFilterTest<uint32_t>();
  SmallScaleMultiFilterTest<int64_t>();
  SmallScaleMultiFilterTest<uint64_t>();
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, VectorVectorFilterTest) {
  //
  // Test: two arrays, a1 and a2; a1 contains sequential numbers in the range
  //       [0, 1000), and s2 contains sequential numbers in range [1,1001).
  //       - a1 == a2, a1 >= a2, and a1 > a2 should return 0 results
  //       - a1 <= a2, a1 < a2, and a1 != a2 should return all results
  //

  const uint32_t num_elems = 10000;

  std::vector<int32_t> arr_1(num_elems);
  std::vector<int32_t> arr_2(num_elems);

  // Load
  std::iota(arr_1.begin(), arr_1.end(), 0);
  std::iota(arr_2.begin(), arr_2.end(), 1);

  alignas(common::Constants::CACHELINE_SIZE) uint32_t out[common::Constants::K_DEFAULT_VECTOR_SIZE] = {0};

#define CHECK(op, expected_count)                                                                       \
  {                                                                                                     \
    uint32_t count = 0;                                                                                 \
    for (uint32_t offset = 0; offset < num_elems; offset += common::Constants::K_DEFAULT_VECTOR_SIZE) { \
      auto size = std::min(common::Constants::K_DEFAULT_VECTOR_SIZE, num_elems - offset);               \
      auto found = VectorUtil::Filter##op(&arr_1[offset], &arr_2[offset], size, out, nullptr);          \
      count += found;                                                                                   \
    }                                                                                                   \
    EXPECT_EQ(expected_count, count);                                                                   \
  }

  CHECK(Eq, 0u)
  CHECK(Ge, 0u)
  CHECK(Gt, 0u)
  CHECK(Le, num_elems)
  CHECK(Lt, num_elems)
  CHECK(Ne, num_elems)

#undef CHECK

  //
  // Test: fill a1 and a2 with random data. Verify filter with scalar versions.
  //

#define CHECK(vec_op, scalar_op)                                                                        \
  {                                                                                                     \
    std::random_device random;                                                                          \
    for (uint32_t idx = 0; idx < num_elems; idx++) {                                                    \
      arr_1[idx] = (random() % 100);                                                                    \
      arr_2[idx] = (random() % 100);                                                                    \
    }                                                                                                   \
    uint32_t vec_count = 0, scalar_count = 0;                                                           \
    for (uint32_t offset = 0; offset < num_elems; offset += common::Constants::K_DEFAULT_VECTOR_SIZE) { \
      auto size = std::min(common::Constants::K_DEFAULT_VECTOR_SIZE, num_elems - offset);               \
      /* Vector filter*/                                                                                \
      auto found = VectorUtil::Filter##vec_op(&arr_1[offset], &arr_2[offset], size, out, nullptr);      \
      vec_count += found;                                                                               \
      /* Scalar filter */                                                                               \
      for (uint32_t iter = offset, end = iter + size; iter != end; iter++) {                            \
        scalar_count += arr_1[iter] scalar_op arr_2[iter];                                              \
      }                                                                                                 \
    }                                                                                                   \
    EXPECT_EQ(scalar_count, vec_count);                                                                 \
  }

  CHECK(Eq, ==)
  CHECK(Ge, >=)
  CHECK(Gt, >)
  CHECK(Le, <=)
  CHECK(Lt, <)
  CHECK(Ne, !=)

#undef CHECK
}

// NOLINTNEXTLINE
TEST_F(VectorUtilTest, GatherTest) {
  auto array = AllocateArray<uint32_t>(800000);
  auto indexes = AllocateArray<uint32_t>(1000);
  auto output = AllocateArray<uint32_t>(1000);

  std::iota(array.Begin(), array.End(), 0);

  std::random_device random;
  std::generate(indexes.Begin(), indexes.End(), [&]() { return random() % array.Size(); });

  // Perform gather
  VectorUtil::Gather(indexes.Size(), array.Raw(), indexes.Raw(), output.Raw());

  // Check
  for (uint32_t i = 0; i < indexes.Size(); i++) {
    EXPECT_EQ(array[indexes[i]], output[i]);
  }
}

}  // namespace terrier::execution::util::test
