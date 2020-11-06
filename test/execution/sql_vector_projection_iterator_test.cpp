#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "execution/sql/vector_projection_iterator.h"
#include "execution/tpl_test.h"

namespace noisepage::execution::sql::test {

///
/// The tests in this file work from one VectorProjection with five columns:
///   col_a SMALLINT NOT NULL (Sequential)
///   col_b INTEGER (Random)
///   col_c INTEGER NOT NULL (Range [0,1000])
///   col_d BIGINT NOT NULL (Random)
///   col_e BIGINT NOT NULL (Range[0,100])
///   col_f BIGINT NOT NULL (Range [50,100])
///

namespace {

template <typename T>
std::unique_ptr<byte[]> CreateMonotonicallyIncreasing(uint32_t num_elems) {
  auto input = std::make_unique<byte[]>(num_elems * sizeof(T));
  auto *typed_input = reinterpret_cast<T *>(input.get());
  std::iota(typed_input, &typed_input[num_elems], static_cast<T>(0));
  return input;
}

template <typename T>
std::unique_ptr<byte[]> CreateRandom(uint32_t num_elems, T min = 0, T max = std::numeric_limits<T>::max()) {
  auto input = std::make_unique<byte[]>(num_elems * sizeof(T));

  std::mt19937 generator;
  std::uniform_int_distribution<T> distribution(min, max);

  auto *typed_input = reinterpret_cast<T *>(input.get());
  for (uint32_t i = 0; i < num_elems; i++) {
    typed_input[i] = distribution(generator);
  }

  return input;
}

std::pair<std::unique_ptr<uint32_t[]>, uint32_t> CreateRandomNullBitmap(uint32_t num_elems) {
  auto input = std::make_unique<uint32_t[]>(util::BitUtil::Num32BitWordsFor(num_elems));
  auto num_nulls = 0;

  std::mt19937 generator;
  std::uniform_int_distribution<uint32_t> distribution(0, 10);

  for (uint32_t i = 0; i < num_elems; i++) {
    if (distribution(generator) < 5) {
      util::BitUtil::Set(input.get(), i);
      num_nulls++;
    }
  }

  return {std::move(input), num_nulls};
}

}  // namespace

class VectorProjectionIteratorTest : public TplTest {
 protected:
  // The columns
  enum ColId : uint8_t { col_a = 0, col_b = 1, col_c = 2, col_d = 3, col_e = 4, col_f = 5 };

  struct ColData {
    std::unique_ptr<byte[]> data_;
    std::unique_ptr<uint32_t[]> nulls_;
    uint32_t num_nulls_;
    uint32_t num_tuples_;

    ColData(std::unique_ptr<byte[]> data, std::unique_ptr<uint32_t[]> nulls, uint32_t num_nulls, uint32_t num_tuples)
        : data_(std::move(data)), nulls_(std::move(nulls)), num_nulls_(num_nulls), num_tuples_(num_tuples) {}
  };

 public:
  VectorProjectionIteratorTest() {
    vector_projection_ = std::make_unique<VectorProjection>();
    vector_projection_->Initialize({
        TypeId::SmallInt,  // col a
        TypeId::Integer,   // col b
        TypeId::Integer,   // col c
        TypeId::BigInt,    // col d
        TypeId::BigInt,    // col e
        TypeId::BigInt,    // col f
    });
    vector_projection_->Reset(NumTuples());

    // Load the data
    LoadData();
  }

  void LoadData() {
    auto cola_data = CreateMonotonicallyIncreasing<int16_t>(NumTuples());
    auto colb_data = CreateRandom<int32_t>(NumTuples());
    auto [colb_null, colb_num_nulls] = CreateRandomNullBitmap(NumTuples());
    auto colc_data = CreateRandom<int32_t>(NumTuples(), 0, 1000);
    auto cold_data = CreateRandom<int64_t>(NumTuples());
    auto cole_data = CreateRandom<int64_t>(NumTuples(), 0, 100);
    auto colf_data = CreateRandom<int64_t>(NumTuples(), 50, 100);

    data_.emplace_back(std::move(cola_data), nullptr, 0, NumTuples());
    data_.emplace_back(std::move(colb_data), std::move(colb_null), colb_num_nulls, NumTuples());
    data_.emplace_back(std::move(colc_data), nullptr, 0, NumTuples());
    data_.emplace_back(std::move(cold_data), nullptr, 0, NumTuples());
    data_.emplace_back(std::move(cole_data), nullptr, 0, NumTuples());
    data_.emplace_back(std::move(colf_data), nullptr, 0, NumTuples());

    for (uint32_t col_idx = 0; col_idx < data_.size(); col_idx++) {
      auto &[data, nulls, num_nulls, num_tuples] = data_[col_idx];
      (void)num_nulls;
      vector_projection_->GetColumn(col_idx)->Reference(data.get(), nulls.get(), num_tuples);
    }
  }

 protected:
  uint32_t NumTuples() const { return num_tuples_; }

  VectorProjection *GetVectorProjection() { return vector_projection_.get(); }

  const ColData &ColumnData(uint32_t col_idx) const { return data_[col_idx]; }

 private:
  uint32_t num_tuples_{10};
  std::unique_ptr<catalog::Schema> schema_;
  std::unique_ptr<VectorProjection> vector_projection_;
  std::vector<ColData> data_;
};

// NOLINTNEXTLINE
TEST_F(VectorProjectionIteratorTest, EmptyIteratorTest) {
  //
  // Test: check to see that iteration doesn't begin without an input block
  //

  VectorProjection empty_vector_proj;
  VectorProjectionIterator iter;
  iter.SetVectorProjection(&empty_vector_proj);

  for (; iter.HasNext(); iter.Advance()) {
    FAIL() << "Should not iterate with empty vector projection!";
  }
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionIteratorTest, SimpleIteratorTest) {
  //
  // Test: check to see that iteration iterates over all tuples in the
  //       projection
  //

  {
    uint32_t tuple_count = 0;

    VectorProjectionIterator iter(GetVectorProjection());

    EXPECT_FALSE(iter.IsFiltered());
    EXPECT_EQ(NumTuples(), iter.GetSelectedTupleCount());
    EXPECT_EQ(NumTuples(), iter.GetTotalTupleCount());

    for (; iter.HasNext(); iter.Advance()) {
      tuple_count++;
    }

    EXPECT_FALSE(iter.IsFiltered());
    EXPECT_EQ(GetVectorProjection()->GetTotalTupleCount(), tuple_count);
    EXPECT_EQ(GetVectorProjection()->GetSelectedTupleCount(), tuple_count);
  }

  //
  // Check to see that column A is monotonically increasing
  //

  {
    VectorProjectionIterator iter(GetVectorProjection());

    EXPECT_FALSE(iter.IsFiltered());
    EXPECT_EQ(NumTuples(), iter.GetSelectedTupleCount());
    EXPECT_EQ(NumTuples(), iter.GetTotalTupleCount());

    // Check that column A is monotonically increasing
    uint32_t num_vals = 0;
    for (int16_t last = -1; iter.HasNext(); iter.Advance()) {
      auto *ptr = iter.GetValue<int16_t, false>(ColId::col_a, nullptr);
      EXPECT_NE(nullptr, ptr);
      if (last != -1) {
        EXPECT_LE(last, *ptr);
      }
      last = *ptr;
      num_vals++;
    }

    EXPECT_EQ(NumTuples(), num_vals);
  }
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionIteratorTest, ReadNullableColumnsTest) {
  //
  // Test: check to see that we can correctly count all NULL values in NULLable
  //       cols
  //

  VectorProjectionIterator iter(GetVectorProjection());

  EXPECT_FALSE(iter.IsFiltered());
  EXPECT_EQ(NumTuples(), iter.GetSelectedTupleCount());
  EXPECT_EQ(NumTuples(), iter.GetTotalTupleCount());

  uint32_t num_nulls = 0;
  for (; iter.HasNext(); iter.Advance()) {
    bool null = false;
    iter.GetValue<int32_t, true>(ColId::col_b, &null);
    num_nulls += null ? 1 : 0;
  }

  EXPECT_FALSE(iter.IsFiltered());
  EXPECT_EQ(ColumnData(ColId::col_b).num_nulls_, num_nulls);
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionIteratorTest, ManualFilterTest) {
  //
  // Test: check to see that we can correctly manually apply a single filter on
  //       a single column. We apply the filter IS_NOT_NULL(col_b)
  //

  {
    VectorProjectionIterator iter(GetVectorProjection());

    for (; iter.HasNext(); iter.Advance()) {
      bool null = false;
      iter.GetValue<int32_t, true>(ColId::col_b, &null);
      iter.Match(!null);
    }

    iter.ResetFiltered();

    EXPECT_TRUE(iter.IsFiltered());
    EXPECT_LT(iter.GetSelectedTupleCount(), iter.GetTotalTupleCount());

    // Now all selected/active elements must not be null
    uint32_t num_non_null = 0;
    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      bool null = false;
      iter.GetValue<int32_t, true>(ColId::col_b, &null);
      EXPECT_FALSE(null);
      num_non_null++;
    }

    iter.ResetFiltered();

    const auto &col_data = ColumnData(ColId::col_b);
    uint32_t actual_non_null = col_data.num_tuples_ - col_data.num_nulls_;
    EXPECT_EQ(actual_non_null, num_non_null);
    EXPECT_EQ(actual_non_null, iter.GetSelectedTupleCount());
  }

  //
  // Test: try to apply filters individually on separate columns.
  //
  // Apply: WHERE col_a < 100 and IS_NULL(col_b)
  //
  // Expectation: The first filter (col_a < 100) should return 100 rows since
  //              it's a monotonically increasing column. The second filter
  //              returns a non-deterministic number of tuples, but it should be
  //              less than or equal to 100. We'll do a manual check to be sure.
  //

  {
    VectorProjectionIterator iter(GetVectorProjection());

    for (; iter.HasNext(); iter.Advance()) {
      auto *val = iter.GetValue<int16_t, false>(ColId::col_a, nullptr);
      iter.Match(*val < 100);
    }

    iter.ResetFiltered();

    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      bool null = false;
      iter.GetValue<int32_t, true>(ColId::col_b, &null);
      iter.Match(null);
    }

    iter.ResetFiltered();

    EXPECT_LE(iter.GetSelectedTupleCount(), 100u);

    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      // col_a must be less than 100
      {
        auto *val = iter.GetValue<int16_t, false>(ColId::col_a, nullptr);
        EXPECT_LT(*val, 100);
      }

      // col_b must be NULL
      {
        bool null = false;
        iter.GetValue<int32_t, true>(ColId::col_b, &null);
        EXPECT_TRUE(null);
      }
    }
  }
}

// NOLINTNEXTLINE
TEST_F(VectorProjectionIteratorTest, ManagedFilterTest) {
  //
  // Test: check to see that we can correctly apply a single filter on a single
  // column using VPI's managed filter. We apply the filter IS_NOT_NULL(col_b)
  //

  VectorProjectionIterator iter(GetVectorProjection());

  iter.RunFilter([&iter]() {
    bool null = false;
    iter.GetValue<int32_t, true>(ColId::col_b, &null);
    return !null;
  });

  EXPECT_TRUE(iter.IsFiltered());

  const auto &col_data = ColumnData(ColId::col_b);
  uint32_t actual_non_null = col_data.num_tuples_ - col_data.num_nulls_;
  EXPECT_EQ(actual_non_null, iter.GetSelectedTupleCount());

  //
  // Ensure subsequent iterations only work on selected items
  //

  {
    uint32_t c = 0;
    iter.ForEach([&iter, &c]() {
      c++;
      bool null = false;
      iter.GetValue<int32_t, true>(ColId::col_b, &null);
      EXPECT_FALSE(null);
    });

    EXPECT_EQ(actual_non_null, iter.GetSelectedTupleCount());
    EXPECT_EQ(iter.GetSelectedTupleCount(), c);
  }
}

}  // namespace noisepage::execution::sql::test
