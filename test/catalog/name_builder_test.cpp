#include "catalog/postgres/name_builder.h"

#include <test_util/test_harness.h>

namespace terrier {

struct NameBuilderTests : public TerrierTest {};

TEST_F(NameBuilderTests, ForeignKeyTest) {
  auto table_name = "foo";
  auto field_name = "bar";
  auto type = catalog::postgres::NameBuilder::FOREIGN_KEY;
  EXPECT_EQ("foo_bar_FOREIGN_KEY", catalog::postgres::NameBuilder::MakeName(table_name, field_name, type));
}

TEST_F(NameBuilderTests, UniquenKeyTest) {
  auto table_name = "foo";
  auto field_name = "bar";
  auto type = catalog::postgres::NameBuilder::UNIQUE_KEY;
  EXPECT_EQ("foo_bar_UNIQUE_KEY", catalog::postgres::NameBuilder::MakeName(table_name, field_name, type));
}

TEST_F(NameBuilderTests, NoneTest) {
  auto table_name = "foo";
  auto field_name = "bar";
  auto type = catalog::postgres::NameBuilder::NONE;
  EXPECT_EQ("foo_bar", catalog::postgres::NameBuilder::MakeName(table_name, field_name, type));
}

TEST_F(NameBuilderTests, LongNameTest) {
  std::string table_name = "";
  for (auto i = 0; i < 40; i++) {
    table_name += std::to_string(i);
  }
  auto field_name = "bar";
  auto type = catalog::postgres::NameBuilder::FOREIGN_KEY;
  EXPECT_EQ("01234567891011121314151617181920212223242526272_bar_FOREIGN_KEY",
            catalog::postgres::NameBuilder::MakeName(table_name, field_name, type));
}

TEST_F(NameBuilderTests, TableNameOnlyTest) {
  auto table_name = "foo";
  auto field_name = "";
  auto type = catalog::postgres::NameBuilder::NONE;
  EXPECT_EQ("foo", catalog::postgres::NameBuilder::MakeName(table_name, field_name, type));
}

}  // namespace terrier
