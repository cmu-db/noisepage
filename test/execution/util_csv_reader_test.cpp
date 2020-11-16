#include "execution/tpl_test.h"
// #include "execution/util/csv_reader.h" Fix later.
#include "execution/util/file.h"

// TODO(WAN): csv is broken.
#if 0
namespace noisepage::execution::util::test {

class CSVReaderTest : public TplTest {
 protected:
  std::unique_ptr<CSVString> MakeSource(const std::string &s) { return std::make_unique<CSVString>(s); }
};

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, CheckEscaping) {
  {
    CSVReader reader(MakeSource("10,\"BLAHBLAH\",\"Special \"\"AF\"\" string\",1000\n"));

    ASSERT_TRUE(reader.Initialize());
    ASSERT_TRUE(reader.Advance());
    auto row = reader.GetRow();
    EXPECT_EQ(4u, row->count_);
    EXPECT_EQ(10, row->cells_[0].AsInteger());
    EXPECT_EQ("BLAHBLAH", row->cells_[1].AsString());
    EXPECT_EQ("Special \"AF\" string", row->cells_[2].AsString());
    EXPECT_EQ(1000, row->cells_[3].AsInteger());
    EXPECT_EQ(1u, reader.GetStatistics()->num_lines_);
  }

  {
    CSVReader reader(
        MakeSource("1,two,\"\nNewRow\n\"\n"
                   "3,four,NormalRow\n"));
    ASSERT_TRUE(reader.Initialize());

    // First row
    EXPECT_TRUE(reader.Advance());
    EXPECT_EQ(3u, reader.GetRow()->count_);
    EXPECT_EQ(1, reader.GetRow()->cells_[0].AsInteger());
    EXPECT_EQ("two", reader.GetRow()->cells_[1].AsString());
    EXPECT_EQ("\nNewRow\n", reader.GetRow()->cells_[2].AsString());
  }
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, Emptycells_AndRows) {
  CSVReader reader(
      MakeSource("1,two,three\n"
                 ",,\n"
                 "4,,six\n"));
  reader.Initialize();

  // First row
  EXPECT_TRUE(reader.Advance());
  EXPECT_EQ(3u, reader.GetRow()->count_);
  EXPECT_EQ(1, reader.GetRow()->cells_[0].AsInteger());
  EXPECT_EQ("two", reader.GetRow()->cells_[1].AsString());
  EXPECT_EQ("three", reader.GetRow()->cells_[2].AsString());

  // Second row is empty
  EXPECT_TRUE(reader.Advance());
  EXPECT_EQ(3u, reader.GetRow()->count_);
  EXPECT_TRUE(reader.GetRow()->cells_[0].IsEmpty());
  EXPECT_TRUE(reader.GetRow()->cells_[1].IsEmpty());
  EXPECT_TRUE(reader.GetRow()->cells_[2].IsEmpty());

  // Third row
  EXPECT_TRUE(reader.Advance());
  EXPECT_EQ(3u, reader.GetRow()->count_);
  EXPECT_EQ(4, reader.GetRow()->cells_[0].AsInteger());
  EXPECT_TRUE(reader.GetRow()->cells_[1].IsEmpty());
  EXPECT_EQ("six", reader.GetRow()->cells_[2].AsString());
}

// NOLINTNEXTLINE
TEST_F(CSVReaderTest, CheckUnquoted) {
  CSVReader reader(
      MakeSource("1,PA,498960,30.102261,-81.711777,Residential,Masonry,1\n"
                 "2,CA,132237,30.063936,101.704,Residential,Wood,3\n"
                 "3,NY,190724,29.089579,-81.700455,Residential,Masonry,1\n"
                 "4,FL,0,30.063236,0.7,Residential,Wood,3\n"
                 "5,WA,5,0.06,-0.75,Residential,Masonry,1\n"));
  ASSERT_TRUE(reader.Initialize());

  const CSVReader::CSVRow *row = nullptr;

  // First row.
  EXPECT_TRUE(reader.Advance());
  row = reader.GetRow();
  EXPECT_EQ(1, row->cells_[0].AsInteger());
  EXPECT_EQ("PA", row->cells_[1].AsString());
  EXPECT_DOUBLE_EQ(30.102261, row->cells_[3].AsDouble());
  EXPECT_DOUBLE_EQ(-81.711777, row->cells_[4].AsDouble());
  EXPECT_EQ("Residential", row->cells_[5].AsString());
  EXPECT_EQ("Masonry", row->cells_[6].AsString());

  // Second row.
  EXPECT_TRUE(reader.Advance());
  row = reader.GetRow();
  EXPECT_EQ(2, row->cells_[0].AsInteger());
  EXPECT_EQ("CA", row->cells_[1].AsString());
  EXPECT_DOUBLE_EQ(30.063936, row->cells_[3].AsDouble());
  EXPECT_DOUBLE_EQ(101.704, row->cells_[4].AsDouble());
  EXPECT_EQ("Residential", row->cells_[5].AsString());
  EXPECT_EQ("Wood", row->cells_[6].AsString());

  // Third row.
  EXPECT_TRUE(reader.Advance());
  row = reader.GetRow();
  EXPECT_EQ(3, row->cells_[0].AsInteger());
  EXPECT_EQ("NY", row->cells_[1].AsString());
  EXPECT_DOUBLE_EQ(29.089579, row->cells_[3].AsDouble());
  EXPECT_DOUBLE_EQ(-81.700455, row->cells_[4].AsDouble());
  EXPECT_EQ("Residential", row->cells_[5].AsString());
  EXPECT_EQ("Masonry", row->cells_[6].AsString());

  // Fourth row.
  EXPECT_TRUE(reader.Advance());
  row = reader.GetRow();
  EXPECT_EQ(4, row->cells_[0].AsInteger());
  EXPECT_EQ("FL", row->cells_[1].AsString());
  EXPECT_DOUBLE_EQ(30.063236, row->cells_[3].AsDouble());
  EXPECT_DOUBLE_EQ(0.7, row->cells_[4].AsDouble());
  EXPECT_EQ("Residential", row->cells_[5].AsString());
  EXPECT_EQ("Wood", row->cells_[6].AsString());

  // Fifth row.
  EXPECT_TRUE(reader.Advance());
  row = reader.GetRow();
  EXPECT_EQ(5, row->cells_[0].AsInteger());
  EXPECT_EQ("WA", row->cells_[1].AsString());
  EXPECT_EQ(5, row->cells_[2].AsInteger());
  EXPECT_DOUBLE_EQ(0.06, row->cells_[3].AsDouble());
  EXPECT_DOUBLE_EQ(-0.75, row->cells_[4].AsDouble());
  EXPECT_EQ("Residential", row->cells_[5].AsString());
  EXPECT_EQ("Masonry", row->cells_[6].AsString());

  EXPECT_FALSE(reader.Advance());
}

}  // namespace noisepage::execution::util::test
#endif
