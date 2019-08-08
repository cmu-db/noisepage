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
std::unique_ptr<byte[]> CreateMonotonicallyIncreasing(u32 num_elems) {
  auto input = std::make_unique<byte[]>(num_elems * sizeof(T));
  auto *typed_input = reinterpret_cast<T *>(input.get());
  std::iota(typed_input, &typed_input[num_elems], static_cast<T>(0));
  return input;
}

template <typename T>
std::unique_ptr<byte[]> CreateRandom(u32 num_elems, T min = 0, T max = std::numeric_limits<T>::max()) {
  auto input = std::make_unique<byte[]>(num_elems * sizeof(T));

  std::mt19937 generator;
  std::uniform_int_distribution<T> distribution(min, max);

  auto *typed_input = reinterpret_cast<T *>(input.get());
  for (u32 i = 0; i < num_elems; i++) {
    typed_input[i] = distribution(generator);
  }

  return input;
}

std::pair<std::unique_ptr<u32[]>, u32> CreateRandomNullBitmap(u32 num_elems) {
  auto input = std::make_unique<u32[]>(util::BitUtil::Num32BitWordsFor(num_elems));
  u32 num_nulls = 0;
  util::BitUtil::Clear(input.get(), num_elems);
  std::mt19937 generator;
  std::uniform_int_distribution<u32> distribution(0, 10);

  for (u32 i = 0; i < num_elems; i++) {
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
  enum ColId : u8 { col_a = 0, col_b = 1, col_c = 2, col_d = 3 };

  struct ColData {
    std::unique_ptr<byte[]> data;
    std::unique_ptr<u32[]> nulls;
    u32 num_nulls;
    u32 num_tuples;
    u32 elem_size;

    ColData(std::unique_ptr<byte[]> data, std::unique_ptr<u32[]> nulls, u32 num_nulls, u32 num_tuples, u32 elem_size)
        : data(std::move(data)),
          nulls(std::move(nulls)),
          num_nulls(num_nulls),
          num_tuples(num_tuples),
          elem_size(elem_size) {}
  };

 public:
  ProjectedColumnsIteratorTest() : num_tuples_(kDefaultVectorSize) {}

  void SetUp() override {
    SqlBasedTest::SetUp();
    exec_ctx_ = MakeExecCtx();

    // NOTE: the storage layer reoder's columns by size. So let's filter them now.
    auto cola_data = CreateMonotonicallyIncreasing<i16>(num_tuples());
    auto colb_data = CreateRandom<i32>(num_tuples());
    auto colb_null = CreateRandomNullBitmap(num_tuples());
    auto colc_data = CreateRandom<i32>(num_tuples(), 0, 1000);
    auto cold_data = CreateRandom<i64>(num_tuples());
    auto cold_null = CreateRandomNullBitmap(num_tuples());

    data_.emplace_back(std::move(cola_data), nullptr, 0, num_tuples(), sizeof(i16));
    data_.emplace_back(std::move(colb_data), std::move(colb_null.first), colb_null.second, num_tuples(), sizeof(i32));
    data_.emplace_back(std::move(colc_data), nullptr, 0, num_tuples(), sizeof(i32));
    data_.emplace_back(std::move(cold_data), std::move(cold_null.first), cold_null.second, num_tuples(), sizeof(i64));

    InitializeColumns();
    //  Fill up data
    for (u16 col_idx = 0; col_idx < data_.size(); col_idx++) {
      // NOLINTNEXTLINE
      auto &[data, nulls, num_nulls, num_tuples, elem_size] = data_[col_idx];
      (void)num_nulls;
      u16 col_offset = GetColOffset(static_cast<ColId>(col_idx));
      projected_columns_->SetNumTuples(num_tuples);
      if (nulls != nullptr) {
        // Fill up the null bitmap.
        std::memcpy(projected_columns_->ColumnNullBitmap(col_offset), nulls.get(), num_tuples / kBitsPerByte);
        // Because the storage layer treats 1 as non-null, we have to flip the
        // bits.
        for (uint32_t i = 0; i < num_tuples; i++) {
          projected_columns_->ColumnNullBitmap(col_offset)->Flip(i);
        }
      } else {
        // Set all rows to non-null.
        std::memset(projected_columns_->ColumnNullBitmap(col_offset), 0, num_tuples / kBitsPerByte);
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
    auto initializer_map = sql_table->InitializerForProjectedColumns(col_oids, kDefaultVectorSize);
    pm_ = initializer_map.second;
    buffer_ = common::AllocationUtil::AllocateAligned(initializer_map.first.ProjectedColumnsSize());
    projected_columns_ = initializer_map.first.Initialize(buffer_);
    projected_columns_->SetNumTuples(kDefaultVectorSize);
  }

  // Delete allocated objects and remove the created table.
  ~ProjectedColumnsIteratorTest() override { delete[] buffer_; }

  // Used to test various ProjectedColumn sizes
  void SetSize(u32 size) { projected_columns_->SetNumTuples(size); }

 protected:
  u32 num_tuples() const { return num_tuples_; }

  storage::ProjectedColumns *GetProjectedColumn() { return projected_columns_; }

  // Compute the offset of the column
  u16 GetColOffset(ColId col) { return pm_.at(catalog::col_oid_t(col + 1)); }

  const ColData &column_data(u32 col_idx) const { return data_[col_idx]; }

 private:
  u32 num_tuples_;
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
    u32 tuple_count = 0;
    ProjectedColumnsIterator iter(GetProjectedColumn());
    SetSize(kDefaultVectorSize);

    for (; iter.HasNext(); iter.Advance()) {
      tuple_count++;
    }

    EXPECT_EQ(kDefaultVectorSize, tuple_count);
    EXPECT_FALSE(iter.IsFiltered());
  }

  //
  // Check to see that column A is monotonically increasing
  //

  {
    ProjectedColumnsIterator iter(GetProjectedColumn());
    SetSize(kDefaultVectorSize);

    bool entered = false;
    for (i16 last = -1; iter.HasNext(); iter.Advance()) {
      entered = true;
      auto *ptr = iter.Get<i16, false>(GetColOffset(ColId::col_a), nullptr);
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
  SetSize(kDefaultVectorSize);

  u32 num_nulls = 0;
  for (; iter.HasNext(); iter.Advance()) {
    bool null = false;
    auto *ptr = iter.Get<i32, true>(GetColOffset(ColId::col_b), &null);
    EXPECT_NE(nullptr, ptr);
    num_nulls += static_cast<u32>(null);
  }

  EXPECT_EQ(column_data(ColId::col_b).num_nulls, num_nulls);
}

// NOLINTNEXTLINE
TEST_F(ProjectedColumnsIteratorTest, ManualFilterTest) {
  //
  // Check to see that we can correctly manually apply a single filter on a
  // single column. We apply the filter IS_NOT_NULL(col_b)
  //

  {
    ProjectedColumnsIterator iter(GetProjectedColumn());
    SetSize(kDefaultVectorSize);

    for (; iter.HasNext(); iter.Advance()) {
      bool null = false;
      iter.Get<i32, true>(GetColOffset(ColId::col_b), &null);
      iter.Match(!null);
    }

    iter.ResetFiltered();

    // Now all selected/active elements must not be null
    u32 num_non_null = 0;
    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      bool null = false;
      iter.Get<i32, true>(GetColOffset(ColId::col_b), &null);
      EXPECT_FALSE(null);
      num_non_null++;
    }

    const auto &col_data = column_data(ColId::col_b);
    u32 actual_non_null = col_data.num_tuples - col_data.num_nulls;
    EXPECT_EQ(actual_non_null, num_non_null);
    EXPECT_EQ(actual_non_null, iter.num_selected());
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
    SetSize(kDefaultVectorSize);

    for (; iter.HasNext(); iter.Advance()) {
      auto *val = iter.Get<i16, false>(GetColOffset(ColId::col_a), nullptr);
      iter.Match(*val < 100);
    }

    iter.ResetFiltered();

    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      bool null = false;
      iter.Get<i32, true>(GetColOffset(ColId::col_b), &null);
      iter.Match(null);
    }

    iter.ResetFiltered();

    EXPECT_LE(iter.num_selected(), 100u);

    for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
      // col_a must be less than 100
      {
        auto *val = iter.Get<i16, false>(GetColOffset(ColId::col_a), nullptr);
        EXPECT_LT(*val, 100);
      }

      // col_b must be NULL
      {
        bool null = false;
        iter.Get<i32, true>(GetColOffset(ColId::col_b), &null);
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
  SetSize(kDefaultVectorSize);

  iter.RunFilter([&iter, this]() {
    bool null = false;
    iter.Get<i32, true>(this->GetColOffset(ColId::col_b), &null);
    return !null;
  });

  const auto &col_data = column_data(ColId::col_b);
  u32 actual_non_null = col_data.num_tuples - col_data.num_nulls;
  EXPECT_EQ(actual_non_null, iter.num_selected());

  //
  // Ensure subsequent iterations only work on selected items
  //
  {
    u32 c = 0;
    iter.ForEach([&iter, &c, this]() {
      c++;
      bool null = false;
      iter.Get<i32, true>(this->GetColOffset(ColId::col_b), &null);
      EXPECT_FALSE(null);
    });

    EXPECT_EQ(actual_non_null, iter.num_selected());
    EXPECT_EQ(iter.num_selected(), c);
  }
}

// NOLINTNEXTLINE
TEST_F(ProjectedColumnsIteratorTest, SimpleVectorizedFilterTest) {
  //
  // Check to see that we can correctly apply a single vectorized filter. Here
  // we just check col_c < 100
  //

  ProjectedColumnsIterator iter(GetProjectedColumn());
  SetSize(kDefaultVectorSize);

  // Compute expected result
  u32 expected = 0;
  for (; iter.HasNext(); iter.Advance()) {
    auto val = *iter.Get<i32, false>(GetColOffset(ColId::col_c), nullptr);
    if (val < 100) {
      expected++;
    }
  }

  // Filter
  iter.FilterColByVal<std::less>(GetColOffset(ColId::col_c), type::TypeId::INTEGER,
                                 ProjectedColumnsIterator::FilterVal{.i = 100});

  // Check
  u32 count = 0;
  for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
    auto val = *iter.Get<i32, false>(GetColOffset(ColId::col_c), nullptr);
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
  SetSize(kDefaultVectorSize);

  iter.FilterColByVal<std::less>(GetColOffset(ColId::col_c), type::TypeId::INTEGER,
                                 ProjectedColumnsIterator::FilterVal{.i = 750});
  iter.FilterColByVal<std::less>(GetColOffset(ColId::col_a), type::TypeId::SMALLINT,
                                 ProjectedColumnsIterator::FilterVal{.si = 10});

  // Check
  u32 count = 0;
  for (; iter.HasNextFiltered(); iter.AdvanceFiltered()) {
    auto col_a_val = *iter.Get<i16, false>(GetColOffset(ColId::col_a), nullptr);
    auto col_c_val = *iter.Get<i32, false>(GetColOffset(ColId::col_c), nullptr);
    EXPECT_LT(col_a_val, 10);
    EXPECT_LT(col_c_val, 750);
    count++;
  }

  EXECUTION_LOG_INFO("Selected: {}", count);
  EXPECT_LE(count, 10u);
}

}  // namespace terrier::execution::sql::test
