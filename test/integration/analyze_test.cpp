
#include "execution/compiler/output_checker.h"
#include "integration/end_to_end_test.h"
#include "optimizer/statistics/histogram.h"
#include "optimizer/statistics/top_k_elements.h"
#include "spdlog/fmt/fmt.h"
#include "test_util/test_harness.h"

namespace noisepage::test {

class AnalyzeTest : public EndToEndTest {
 public:
  void SetUp() override {
    EndToEndTest::SetUp();
    auto exec_ctx = MakeExecCtx();
    GenerateTestTables(exec_ctx.get());
  }
};

class PgStatisticOutputChecker : public execution::compiler::test::OutputChecker {
 public:
  PgStatisticOutputChecker(int64_t table_oid, int64_t col_oid, int64_t num_rows, int64_t non_null_rows,
                           int64_t distinct_rows, bool topk_null, bool histogram_null)
      : row_checker_({}) {
    output_checkers_.emplace_back(
        new execution::compiler::test::SingleIntComparisonChecker(std::equal_to<>(), 0, table_oid));
    auto *table_oid_checker = output_checkers_.back();

    output_checkers_.emplace_back(
        new execution::compiler::test::SingleIntComparisonChecker(std::equal_to<>(), 1, col_oid));
    auto *col_oid_checker = output_checkers_.back();

    output_checkers_.emplace_back(
        new execution::compiler::test::SingleIntComparisonChecker(std::equal_to<>(), 2, num_rows));
    auto *num_rows_checker = output_checkers_.back();

    output_checkers_.emplace_back(
        new execution::compiler::test::SingleIntComparisonChecker(std::equal_to<>(), 3, non_null_rows));
    auto *non_null_checker = output_checkers_.back();

    output_checkers_.emplace_back(
        new execution::compiler::test::SingleIntComparisonChecker(std::equal_to<>(), 4, distinct_rows));
    auto *distinct_row_checker = output_checkers_.back();

    output_checkers_.emplace_back(new execution::compiler::test::NullChecker(5, topk_null));
    auto *topk_checker = output_checkers_.back();

    output_checkers_.emplace_back(new execution::compiler::test::NullChecker(6, histogram_null));
    auto *histogram_checker = output_checkers_.back();

    row_checker_ =
        execution::compiler::test::MultiChecker({table_oid_checker, col_oid_checker, num_rows_checker, non_null_checker,
                                                 distinct_row_checker, topk_checker, histogram_checker});
  }

  PgStatisticOutputChecker(int64_t table_oid, int64_t col_oid, int64_t num_rows, int64_t non_null_rows,
                           int64_t distinct_rows, const std::string &topk, const std::string &histogram)
      : row_checker_({}) {
    output_checkers_.emplace_back(
        new execution::compiler::test::SingleIntComparisonChecker(std::equal_to<>(), 0, table_oid));
    auto *table_oid_checker = output_checkers_.back();

    output_checkers_.emplace_back(
        new execution::compiler::test::SingleIntComparisonChecker(std::equal_to<>(), 1, col_oid));
    auto *col_oid_checker = output_checkers_.back();

    output_checkers_.emplace_back(
        new execution::compiler::test::SingleIntComparisonChecker(std::equal_to<>(), 2, num_rows));
    auto *num_rows_checker = output_checkers_.back();

    output_checkers_.emplace_back(
        new execution::compiler::test::SingleIntComparisonChecker(std::equal_to<>(), 3, non_null_rows));
    auto *non_null_checker = output_checkers_.back();

    output_checkers_.emplace_back(
        new execution::compiler::test::SingleIntComparisonChecker(std::equal_to<>(), 4, distinct_rows));
    auto *distinct_row_checker = output_checkers_.back();

    output_checkers_.emplace_back(
        new execution::compiler::test::SingleStringComparisonChecker(std::equal_to<>(), 5, topk));
    auto *topk_checker = output_checkers_.back();

    output_checkers_.emplace_back(
        new execution::compiler::test::SingleStringComparisonChecker(std::equal_to<>(), 6, histogram));
    auto *histogram_checker = output_checkers_.back();

    row_checker_ =
        execution::compiler::test::MultiChecker({table_oid_checker, col_oid_checker, num_rows_checker, non_null_checker,
                                                 distinct_row_checker, topk_checker, histogram_checker});
  }

  ~PgStatisticOutputChecker() override {
    for (auto *checker : output_checkers_) {
      delete checker;
    }
  }

  DISALLOW_COPY_AND_MOVE(PgStatisticOutputChecker);

  void CheckCorrectness() override { row_checker_.CheckCorrectness(); }

  void ProcessBatch(const std::vector<std::vector<execution::sql::Val *>> &output) override {
    row_checker_.ProcessBatch(output);
  }

 private:
  std::vector<execution::compiler::test::OutputChecker *> output_checkers_;
  execution::compiler::test::MultiChecker row_checker_;
};

// NOLINTNEXTLINE
TEST_F(AnalyzeTest, SingleColumnTest) {
  auto table_name = "empty_nullable_table";
  auto table_oid = accessor_->GetTableOid(table_name);
  optimizer::TopKElements<int64_t> top_k(16, 64);
  optimizer::Histogram<int64_t> histogram(64);

  // Row is initially empty for column
  PgStatisticOutputChecker empty_checker(table_oid.UnderlyingValue(), 1, 0, 0, 0, true, true);
  RunQuery(
      fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = 1;", table_oid.UnderlyingValue()),
      &empty_checker);

  // Insert 1
  RunQuery(fmt::format("INSERT INTO {} VALUES (1);", table_name));
  RunQuery(fmt::format("ANALYZE {};", table_name));

  top_k.Increment(1, 1);
  size_t top_k_size;
  auto top_k_serialized = top_k.Serialize(&top_k_size);
  std::string top_k_serialized_str(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);

  histogram.Increment(1);
  size_t histogram_size;
  auto histogram_serialized = histogram.Serialize(&histogram_size);
  std::string histogram_serialized_str(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);

  auto single_checker =
      PgStatisticOutputChecker(table_oid.UnderlyingValue(), 1, 1, 1, 1, top_k_serialized_str, histogram_serialized_str);
  RunQuery(
      fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = 1;", table_oid.UnderlyingValue()),
      &single_checker);

  // Insert 666
  RunQuery(fmt::format("INSERT INTO {} VALUES (666);", table_name));
  RunQuery(fmt::format("ANALYZE {};", table_name));

  top_k.Increment(666, 1);
  top_k_serialized = top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);

  histogram.Increment(666);
  histogram_serialized = histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);

  auto second_checker =
      PgStatisticOutputChecker(table_oid.UnderlyingValue(), 1, 2, 2, 2, top_k_serialized_str, histogram_serialized_str);
  RunQuery(
      fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = 1;", table_oid.UnderlyingValue()),
      &second_checker);

  // Insert NULL
  RunQuery(fmt::format("INSERT INTO {} VALUES (NULL);", table_name));
  RunQuery(fmt::format("ANALYZE {};", table_name));

  auto null_checker =
      PgStatisticOutputChecker(table_oid.UnderlyingValue(), 1, 3, 2, 2, top_k_serialized_str, histogram_serialized_str);
  RunQuery(
      fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = 1;", table_oid.UnderlyingValue()),
      &null_checker);

  // Insert duplicate
  RunQuery(fmt::format("INSERT INTO {} VALUES (666);", table_name));
  RunQuery(fmt::format("ANALYZE {};", table_name));

  top_k.Increment(666, 1);
  top_k_serialized = top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);

  histogram.Increment(666);
  histogram_serialized = histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);

  auto duplicate_checker =
      PgStatisticOutputChecker(table_oid.UnderlyingValue(), 1, 4, 3, 2, top_k_serialized_str, histogram_serialized_str);
  RunQuery(
      fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = 1;", table_oid.UnderlyingValue()),
      &duplicate_checker);
}

// NOLINTNEXTLINE
TEST_F(AnalyzeTest, MultiColumnTest) {
  /*
   * TODO (Joe) This test was a pain to write. It would be slightly better and robust to have different values for all
   *  the int columns, and also have the statistics differ for each column
   */
  const auto *table_name = "all_types_empty_nullable_table";
  auto table_oid = accessor_->GetTableOid(table_name);
  auto num_cols = accessor_->GetSchema(table_oid).GetColumns().size();

  optimizer::TopKElements<storage::VarlenEntry> string_top_k(16, 64);
  optimizer::Histogram<storage::VarlenEntry> string_histogram(64);
  optimizer::TopKElements<execution::sql::Date> date_top_k(16, 64);
  optimizer::Histogram<execution::sql::Date> date_histogram(64);
  optimizer::TopKElements<bool> bool_top_k(16, 64);
  optimizer::Histogram<bool> bool_histogram(64);
  optimizer::TopKElements<int64_t> int_top_k(16, 64);
  optimizer::Histogram<int64_t> int_histogram(64);

  // Row is initially empty for columns
  for (int64_t i = 0; static_cast<size_t>(i) < num_cols; i++) {
    auto empty_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), i + 1, 0, 0, 0, true, true);
    RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                         table_oid.UnderlyingValue(), i + 1),
             &empty_checker);
  }

  // Insert non null values
  RunQuery(fmt::format("INSERT INTO {} VALUES ('beta fish', '1964-12-24', 42.6, true, 1, 1, 1, 1);", table_name));
  RunQuery(fmt::format("ANALYZE {};", table_name));

  // Check String col
  int col_oid = 1;
  string_top_k.Increment(storage::VarlenEntry::Create("beta fish"), 1);
  size_t top_k_size;
  auto top_k_serialized = string_top_k.Serialize(&top_k_size);
  std::string top_k_serialized_str(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  string_histogram.Increment(storage::VarlenEntry::Create("beta fish"));
  size_t histogram_size;
  auto histogram_serialized = string_histogram.Serialize(&histogram_size);
  std::string histogram_serialized_str(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  auto string_single_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 1, 1, 1,
                                                        top_k_serialized_str, histogram_serialized_str);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &string_single_checker);

  // Check Date col
  col_oid = 2;
  date_top_k.Increment(execution::sql::Date::FromString("1964-12-24"), 1);
  top_k_serialized = date_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  date_histogram.Increment(execution::sql::Date::FromString("1964-12-24"));
  histogram_serialized = date_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  auto date_single_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 1, 1, 1,
                                                      top_k_serialized_str, histogram_serialized_str);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &date_single_checker);

  // Check Real col
  col_oid = 3;
  // Unfortunately due to floating point precision issues we can't check the serialized contents exactly
  auto real_single_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 1, 1, 1, false, false);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &real_single_checker);

  // Check bool col
  col_oid = 4;
  bool_top_k.Increment(true, 1);
  top_k_serialized = bool_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  bool_histogram.Increment(true);
  histogram_serialized = bool_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  auto bool_single_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 1, 1, 1,
                                                      top_k_serialized_str, histogram_serialized_str);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &bool_single_checker);

  // Check int cols
  int_top_k.Increment(1, 1);
  top_k_serialized = int_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  int_histogram.Increment(1);
  histogram_serialized = int_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  for (int i = 5; i <= 8; i++) {
    col_oid = i;
    auto int_single_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 1, 1, 1,
                                                       top_k_serialized_str, histogram_serialized_str);
    RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                         table_oid.UnderlyingValue(), col_oid),
             &int_single_checker);
  }

  // Insert distinct non-null values
  RunQuery(fmt::format("INSERT INTO {} VALUES ('Koshy', '1995-08-06', 666.42, false, 13, 13, 13, 13);", table_name));
  RunQuery(fmt::format("ANALYZE {};", table_name));

  // Check String col
  col_oid = 1;
  string_top_k.Increment(storage::VarlenEntry::Create("Koshy"), 1);
  top_k_serialized = string_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  string_histogram.Increment(storage::VarlenEntry::Create("Koshy"));
  histogram_serialized = string_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  auto string_second_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 2, 2, 2,
                                                        top_k_serialized_str, histogram_serialized_str);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &string_second_checker);

  // Check Date col
  col_oid = 2;
  date_top_k.Increment(execution::sql::Date::FromString("1995-08-06"), 1);
  top_k_serialized = date_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  date_histogram.Increment(execution::sql::Date::FromString("1995-08-06"));
  histogram_serialized = date_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  auto date_second_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 2, 2, 2,
                                                      top_k_serialized_str, histogram_serialized_str);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &date_second_checker);

  // Check Real col
  col_oid = 3;
  // Unfortunately due to floating point precision issues we can't check the serialized contents exactly
  auto real_second_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 2, 2, 2, false, false);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &real_second_checker);

  // Check bool col
  col_oid = 4;
  bool_top_k.Increment(false, 1);
  top_k_serialized = bool_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  bool_histogram.Increment(false);
  histogram_serialized = bool_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  auto bool_second_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 2, 2, 2,
                                                      top_k_serialized_str, histogram_serialized_str);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &bool_second_checker);

  // Check int cols
  int_top_k.Increment(13, 1);
  top_k_serialized = int_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  int_histogram.Increment(13);
  histogram_serialized = int_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  for (int i = 5; i <= 8; i++) {
    col_oid = i;
    auto int_second_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 2, 2, 2,
                                                       top_k_serialized_str, histogram_serialized_str);
    RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                         table_oid.UnderlyingValue(), col_oid),
             &int_second_checker);
  }

  // Insert NULL values
  RunQuery(fmt::format("INSERT INTO {} VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);", table_name));
  RunQuery(fmt::format("ANALYZE {};", table_name));

  // Check String col
  col_oid = 1;
  top_k_serialized = string_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  histogram_serialized = string_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  auto string_null_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 3, 2, 2,
                                                      top_k_serialized_str, histogram_serialized_str);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &string_null_checker);

  // Check Date col
  col_oid = 2;
  top_k_serialized = date_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  histogram_serialized = date_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  auto date_null_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 3, 2, 2, top_k_serialized_str,
                                                    histogram_serialized_str);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &date_null_checker);

  // Check Real col
  col_oid = 3;
  // Unfortunately due to floating point precision issues we can't check the serialized contents exactly
  auto real_null_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 3, 2, 2, false, false);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &real_null_checker);

  // Check bool col
  col_oid = 4;
  top_k_serialized = bool_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  histogram_serialized = bool_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  auto bool_null_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 3, 2, 2, top_k_serialized_str,
                                                    histogram_serialized_str);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &bool_null_checker);

  // Check int cols
  top_k_serialized = int_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  histogram_serialized = int_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  for (int i = 5; i <= 8; i++) {
    col_oid = i;
    auto int_null_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 3, 2, 2,
                                                     top_k_serialized_str, histogram_serialized_str);
    RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                         table_oid.UnderlyingValue(), col_oid),
             &int_null_checker);
  }

  // Add non-distinct values
  RunQuery(fmt::format("INSERT INTO {} VALUES ('Koshy', '1995-08-06', 666.42, false, 13, 13, 13, 13);", table_name));
  RunQuery(fmt::format("ANALYZE {};", table_name));

  // Check String col
  col_oid = 1;
  string_top_k.Increment(storage::VarlenEntry::Create("Koshy"), 1);
  top_k_serialized = string_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  string_histogram.Increment(storage::VarlenEntry::Create("Koshy"));
  histogram_serialized = string_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  auto string_duplicate_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 4, 3, 2,
                                                           top_k_serialized_str, histogram_serialized_str);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &string_duplicate_checker);

  // Check Date col
  col_oid = 2;
  date_top_k.Increment(execution::sql::Date::FromString("1995-08-06"), 1);
  top_k_serialized = date_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  date_histogram.Increment(execution::sql::Date::FromString("1995-08-06"));
  histogram_serialized = date_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  auto date_duplicate_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 4, 3, 2,
                                                         top_k_serialized_str, histogram_serialized_str);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &date_duplicate_checker);

  // Check Real col
  col_oid = 3;
  // Unfortunately due to floating point precision issues we can't check the serialized contents exactly
  auto real_duplicate_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 4, 3, 2, false, false);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &real_duplicate_checker);

  // Check bool col
  col_oid = 4;
  bool_top_k.Increment(false, 1);
  top_k_serialized = bool_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  bool_histogram.Increment(false);
  histogram_serialized = bool_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  auto bool_duplicate_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 4, 3, 2,
                                                         top_k_serialized_str, histogram_serialized_str);
  RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                       table_oid.UnderlyingValue(), col_oid),
           &bool_duplicate_checker);

  // Check int cols
  int_top_k.Increment(13, 1);
  top_k_serialized = int_top_k.Serialize(&top_k_size);
  top_k_serialized_str = std::string(reinterpret_cast<char *>(top_k_serialized.get()), top_k_size);
  int_histogram.Increment(13);
  histogram_serialized = int_histogram.Serialize(&histogram_size);
  histogram_serialized_str = std::string(reinterpret_cast<char *>(histogram_serialized.get()), histogram_size);
  for (int i = 5; i <= 8; i++) {
    col_oid = i;
    auto int_duplicate_checker = PgStatisticOutputChecker(table_oid.UnderlyingValue(), col_oid, 4, 3, 2,
                                                          top_k_serialized_str, histogram_serialized_str);
    RunQuery(fmt::format("SELECT * FROM pg_statistic WHERE starelid = {} AND staattnum = {};",
                         table_oid.UnderlyingValue(), col_oid),
             &int_duplicate_checker);
  }
}
}  // namespace noisepage::test
