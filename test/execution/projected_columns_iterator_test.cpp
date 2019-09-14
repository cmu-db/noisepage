#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <utility>
#include <vector>

#include "execution/sql_test.h"

#include "catalog/catalog.h"
#include "execution/sql/projected_columns_iterator.h"

namespace terrier::execution::sql::test {

///
/// This test uses a ProjectedColumns with four columns. The first column,
/// named "col_a" is a non-nullable small integer column whose values are
/// monotonically increasing. The second column, "col_b", is a nullable integer
/// column whose values are random. The third column, "col_c", is a non-nullable
/// integer column whose values are in the range [0, 1000). The last column,
/// "col_d", is a nullable big-integer column whose values are random.
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
  uint32_t num_nulls = 0;
  util::BitUtil::Clear(input.get(), num_elems);
  std::mt19937 generator;
  std::uniform_int_distribution<uint32_t> distribution(0, 10);

  for (uint32_t i = 0; i < num_elems; i++) {
    if (distribution(generator) < 5) {
      util::BitUtil::Flip(input.get(), i);
      num_nulls++;
    }
  }

  return {std::move(input), num_nulls};
}

}  // namespace

class ProjectedColumnsIteratorTest : public SqlBasedTest {
 protected:
  enum ColId : uint8_t { col_a = 0, col_b = 1, col_c = 2, col_d = 3 };

  struct ColData {
    std::unique_ptr<byte[]> data_;
    std::unique_ptr<uint32_t[]> nulls_;
    uint32_t num_nulls_;
    uint32_t num_tuples_;
    uint32_t elem_size_;

    ColData(std::unique_ptr<byte[]> data, std::unique_ptr<uint32_t[]> nulls, uint32_t num_nulls, uint32_t num_tuples,
            uint32_t elem_size)
        : data_(std::move(data)),
          nulls_(std::move(nulls)),
          num_nulls_(num_nulls),
          num_tuples_(num_tuples),
          elem_size_(elem_size) {}
  };

 public:
  ProjectedColumnsIteratorTest() : num_tuples_(common::Constants::K_DEFAULT_VECTOR_SIZE) {}

  void SetUp() override {
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();

    // NOTE: the storage layer reoder's columns by size. So let's filter them now.
    auto cola_data = CreateMonotonicallyIncreasing<int16_t>(NumTuples());
    auto colb_data = CreateRandom<int32_t>(NumTuples());
    auto colb_null = CreateRandomNullBitmap(NumTuples());
    auto colc_data = CreateRandom<int32_t>(NumTuples(), 0, 1000);
    auto cold_data = CreateRandom<int64_t>(NumTuples());
    auto cold_null = CreateRandomNullBitmap(NumTuples());

    data_.emplace_back(std::move(cola_data), nullptr, 0, NumTuples(), sizeof(int16_t));
    data_.emplace_back(std::move(colb_data), std::move(colb_null.first), colb_null.second, NumTuples(),
                       sizeof(int32_t));
    data_.emplace_back(std::move(colc_data), nullptr, 0, NumTuples(), sizeof(int32_t));
    data_.emplace_back(std::move(cold_data), std::move(cold_null.first), cold_null.second, NumTuples(),
                       sizeof(int64_t));

    InitializeColumns();
    //  Fill up data
    for (uint16_t col_idx = 0; col_idx < data_.size(); col_idx++) {
      // NOLINTNEXTLINE
      auto &[data, nulls, num_nulls, num_tuples, elem_size] = data_[col_idx];
      (void)num_nulls;
      uint16_t col_offset = GetColOffset(static_cast<ColId>(col_idx));
      projected_columns_->SetNumTuples(num_tuples);
      if (nulls != nullptr) {
        // Fill up the null bitmap.
        std::memcpy(projected_columns_->ColumnNullBitmap(col_offset), nulls.get(),
                    num_tuples / common::Constants::K_BITS_PER_BYTE);
        // Because the storage layer treats 1 as non-null, we have to flip the
        // bits.
        for (uint32_t i = 0; i < num_tuples; i++) {
          projected_columns_->ColumnNullBitmap(col_offset)->Flip(i);
        }
      } else {
        // Set all rows to non-null.
        std::memset(projected_columns_->ColumnNullBitmap(col_offset), 0,
                    num_tuples / common::Constants::K_BITS_PER_BYTE);
      }
      // Fill up the values.
      std::memcpy(projected_columns_->ColumnStart(col_offset), data.get(), elem_size * num_tuples);
    }
  }

  void InitializeColumns() {
    // Create column metadata for every column.
    catalog::Schema::Column col_a("col_a", type::TypeId::SMALLINT, false, DummyCVE());
    catalog::Schema::Column col_b("col_b", type::TypeId::INTEGER, false, DummyCVE());
    catalog::Schema::Column col_c("col_c", type::TypeId::INTEGER, false, DummyCVE());
    catalog::Schema::Column col_d("col_d", type::TypeId::BIGINT, false, DummyCVE());

    // Create the table in the catalog.
    catalog::Schema tmp_schema({col_a, col_b, col_c, col_d});
    auto table_oid = exec_ctx_->GetAccessor()->CreateTable(NSOid(), "pci_test_table", tmp_schema);
    auto schema = exec_ctx_->GetAccessor()->GetSchema(table_oid);
    auto sql_table = new storage::SqlTable(BlockStore(), schema);
    exec_ctx_->GetAccessor()->SetTablePointer(table_oid, sql_table);

    // Create a ProjectedColumns
    std::vector<catalog::col_oid_t> col_oids;
    for (const auto &col : schema.GetColumns()) {
      col_oids.emplace_back(col.Oid());
    }
    auto pc_init = sql_table->InitializerForProjectedColumns(col_oids, common::Constants::K_DEFAULT_VECTOR_SIZE);
    pm_ = sql_table->ProjectionMapForOids(col_oids);
    buffer_ = common::AllocationUtil::AllocateAligned(pc_init.ProjectedColumnsSize());
    projected_columns_ = pc_init.Initialize(buffer_);
    projected_columns_->SetNumTuples(common::Constants::K_DEFAULT_VECTOR_SIZE);
  }

  // Delete allocated objects and remove the created table.
  ~ProjectedColumnsIteratorTest() override { delete[] buffer_; }

  // Used to test various ProjectedColumn sizes_
  void SetSize(uint32_t size) { projected_columns_->SetNumTuples(size); }

 protected:
  uint32_t NumTuples() const { return num_tuples_; }

  storage::ProjectedColumns *GetProjectedColumn() { return projected_columns_; }

  // Compute the offset of the column
  uint16_t GetColOffset(ColId col) { return pm_.at(catalog::col_oid_t(col + 1)); }

  const ColData &ColumnData(uint32_t col_idx) const { return data_[col_idx]; }

 private:
  uint32_t num_tuples_;
  std::vector<ColData> data_;
  byte *buffer_ = nullptr;
  storage::ProjectedColumns *projected_columns_ = nullptr;
  std::unique_ptr<exec::ExecutionContext> exec_ctx_;
  storage::ProjectionMap pm_;
};

// NOLINTNEXTLINE
TEST_F(ProjectedColumnsIteratorTest, EmptyIteratorTest) {
  //
  // Check to see that iteration doesn't begin without an input block
  //

  ProjectedColumnsIterator iter(GetProjectedColumn());
  SetSize(0);

  for (; iter.HasNext(); iter.Advance()) {
    FAIL() << "Should not iterate with empty ProjectedColumns!";
  }
}

// NOLINTNEXTLINE
TEST_F(ProjectedColumnsIteratorTest, SimpleIteratorTest) {
  //
  // Check to see that iteration iterates over all tuples in the projection
  //

  {
    uint32_t tuple_count = 0;
    ProjectedColumnsIterator iter(GetProjectedColumn());
    SetSize(common::Constants::K_DEFAULT_VECTOR_SIZE);

    for (; iter.HasNext(); iter.Advance()) {
      tuple_count++;
    }

    EXPECT_EQ(common::Constants::K_DEFAULT_VECTOR_SIZE, tuple_count);
    EXPECT_FALSE(iter.IsFiltered());
  }

  //
  // Check to see that column A is monotonically increasing
  //

  {
    ProjectedColumnsIterator iter(GetProjectedColumn());
    SetSize(common::Constants::K_DEFAULT_VECTOR_SIZE);

    bool entered = false;
    for (int16_t last = -1; iter.HasNext(); iter.Advance()) {
      entered = true;
      auto *ptr = iter.Get<int16_t, false>(GetColOffset(ColId::col_a), nullptr);
      EXPECT_NE(nullptr, ptr);
      if (last != -1) {
        EXPECT_LE(last, *ptr);
      }
      last = *ptr;
    }

    EXPECT_TRUE(entered);
    EXPECT_FALSE(iter.IsFiltered());
  }
}

// NOLINTNEXTLINE
TEST_F(ProjectedColumnsIteratorTest, ReadNullableColumnsTest) {
  //
  // Check to see that we can correctly count all NULL values in NULLable cols
  //

  ProjectedColumnsIterator iter(GetProjectedColumn());
  SetSize(common::Constants::K_DEFAULT_VECTOR_SIZE);

  uint32_t num_nulls = 0;
  for (; iter.HasNext(); iter.Advance()) {
    bool null = false;
    auto *ptr = iter.Get<int32_t, true>(GetColOffset(ColId::col_b), &null);
    EXPECT_NE(nullptr, ptr);
    num_nulls += static_cast<uint32_t>(null);
  }

  EXPECT_EQ(ColumnData(ColId::col_b).num_nulls_, num_nulls);
}

// NOLINTNEXTLINE
TEST_F(ProjectedColumnsIteratorTest, ManualFilterTest) {
  //
  // Check to see that we can correctly manually apply a single filter on a
  // single column. We apply the filter IS_NOT_NULL(col_b)
  //

  {
    ProjectedColumnsIterator iter(GetProjectedColumn());
    SetSize(common::Constants::K_DEFAULT_VECTOR_SIZE);

    for (; iter.HasNext(); iter.Advance()) {
      bool null = false;
      iter.Get<int32_t, true>(GetColOffset(ColId::col_b), &null);
      iter.Match(!null);
    }

    iter.ResetFiltered();

    // Now all selected/active elements must not be null
    uint32_t num_non_null = 0;
    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      bool null = false;
      iter.Get<int32_t, true>(GetColOffset(ColId::col_b), &null);
      EXPECT_FALSE(null);
      num_non_null++;
    }

    const auto &col_data = ColumnData(ColId::col_b);
    uint32_t actual_non_null = col_data.num_tuples_ - col_data.num_nulls_;
    EXPECT_EQ(actual_non_null, num_non_null);
    EXPECT_EQ(actual_non_null, iter.NumSelected());
  }

  //
  // Now try to apply filters individually on separate columns. Let's try:
  //
  // WHERE col_a < 100 and IS_NULL(col_b)
  //
  // The first filter (col_a < 100) should return 100 rows since it's a
  // monotonically increasing column. The second filter returns a
  // non-deterministic number of tuples, but it should be less than or equal to
  // 100. We'll do a manual check to be sure.
  //

  {
    ProjectedColumnsIterator iter(GetProjectedColumn());
    SetSize(common::Constants::K_DEFAULT_VECTOR_SIZE);

    for (; iter.HasNext(); iter.Advance()) {
      auto *val = iter.Get<int16_t, false>(GetColOffset(ColId::col_a), nullptr);
      iter.Match(*val < 100);
    }

    iter.ResetFiltered();

    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      bool null = false;
      iter.Get<int32_t, true>(GetColOffset(ColId::col_b), &null);
      iter.Match(null);
    }

    iter.ResetFiltered();

    EXPECT_LE(iter.NumSelected(), 100u);

    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      // col_a must be less than 100
      {
        auto *val = iter.Get<int16_t, false>(GetColOffset(ColId::col_a), nullptr);
        EXPECT_LT(*val, 100);
      }

      // col_b must be NULL
      {
        bool null = false;
        iter.Get<int32_t, true>(GetColOffset(ColId::col_b), &null);
        EXPECT_TRUE(null);
      }
    }
  }
}

// NOLINTNEXTLINE
TEST_F(ProjectedColumnsIteratorTest, ManagedFilterTest) {
  //
  // Check to see that we can correctly apply a single filter on a single
  // column using PCI's managed filter. We apply the filter IS_NOT_NULL(col_b)
  //

  ProjectedColumnsIterator iter(GetProjectedColumn());
  SetSize(common::Constants::K_DEFAULT_VECTOR_SIZE);

  iter.RunFilter([&iter, this]() {
    bool null = false;
    iter.Get<int32_t, true>(this->GetColOffset(ColId::col_b), &null);
    return !null;
  });

  const auto &col_data = ColumnData(ColId::col_b);
  uint32_t actual_non_null = col_data.num_tuples_ - col_data.num_nulls_;
  EXPECT_EQ(actual_non_null, iter.NumSelected());

  //
  // Ensure subsequent iterations only work on selected items
  //
  {
    uint32_t c = 0;
    iter.ForEach([&iter, &c, this]() {
      c++;
      bool null = false;
      iter.Get<int32_t, true>(this->GetColOffset(ColId::col_b), &null);
      EXPECT_FALSE(null);
    });

    EXPECT_EQ(actual_non_null, iter.NumSelected());
    EXPECT_EQ(iter.NumSelected(), c);
  }
}

// NOLINTNEXTLINE
TEST_F(ProjectedColumnsIteratorTest, SimpleVectorizedFilterTest) {
  //
  // Check to see that we can correctly apply a single vectorized filter. Here
  // we just check col_c < 100
  //

  ProjectedColumnsIterator iter(GetProjectedColumn());
  SetSize(common::Constants::K_DEFAULT_VECTOR_SIZE);

  // Compute expected result
  uint32_t expected = 0;
  for (; iter.HasNext(); iter.Advance()) {
    auto val = *iter.Get<int32_t, false>(GetColOffset(ColId::col_c), nullptr);
    if (val < 100) {
      expected++;
    }
  }

  // Filter
  iter.FilterColByVal<std::less>(GetColOffset(ColId::col_c), type::TypeId::INTEGER,
                                 ProjectedColumnsIterator::FilterVal{.i_ = 100});

  // Check
  uint32_t count = 0;
  for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
    auto val = *iter.Get<int32_t, false>(GetColOffset(ColId::col_c), nullptr);
    EXPECT_LT(val, 100);
    count++;
  }

  EXPECT_EQ(expected, count);
}

// NOLINTNEXTLINE
TEST_F(ProjectedColumnsIteratorTest, MultipleVectorizedFilterTest) {
  //
  // Apply two filters in order:
  //  - col_c < 750
  //  - col_a < 10
  //
  // The first filter will return a non-deterministic result because it is a
  // randomly generated column. But, the second filter will return at most 10
  // results because it is a monotonically increasing column beginning at 0.
  //

  ProjectedColumnsIterator iter(GetProjectedColumn());
  SetSize(common::Constants::K_DEFAULT_VECTOR_SIZE);

  iter.FilterColByVal<std::less>(GetColOffset(ColId::col_c), type::TypeId::INTEGER,
                                 ProjectedColumnsIterator::FilterVal{.i_ = 750});
  iter.FilterColByVal<std::less>(GetColOffset(ColId::col_a), type::TypeId::SMALLINT,
                                 ProjectedColumnsIterator::FilterVal{.si_ = 10});

  // Check
  uint32_t count = 0;
  for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
    auto col_a_val = *iter.Get<int16_t, false>(GetColOffset(ColId::col_a), nullptr);
    auto col_c_val = *iter.Get<int32_t, false>(GetColOffset(ColId::col_c), nullptr);
    EXPECT_LT(col_a_val, 10);
    EXPECT_LT(col_c_val, 750);
    count++;
  }

  EXECUTION_LOG_INFO("Selected: {}", count);
  EXPECT_LE(count, 10u);
}

}  // namespace terrier::execution::sql::test
