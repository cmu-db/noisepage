#include "planner/plannodes/output_schema.h"

#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "test_util/test_harness.h"
#include "type/type_id.h"

namespace noisepage::planner {

class OutputSchemaTests : public TerrierTest {
 public:
  /**
   * Constructs a dummy AbstractExpression predicate
   * @return dummy predicate
   */
  static std::unique_ptr<parser::AbstractExpression> BuildDummyPredicate() {
    return std::make_unique<parser::ConstantValueExpression>(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  }
};

// NOLINTNEXTLINE
TEST(OutputSchemaTests, OutputSchemaTest) {
  // Create two OutputSchema objects with the same info.
  // They should hash to the same values and be equivalent

  OutputSchema::Column col0("dummy_col", type::TypeId::INTEGER, OutputSchemaTests::BuildDummyPredicate());
  std::vector<OutputSchema::Column> cols0;
  cols0.emplace_back(col0.Copy());
  auto schema0 = std::make_unique<OutputSchema>(std::move(cols0));

  OutputSchema::Column col1("dummy_col", type::TypeId::INTEGER, OutputSchemaTests::BuildDummyPredicate());
  std::vector<OutputSchema::Column> cols1;
  cols1.emplace_back(col1.Copy());
  auto schema1 = std::make_unique<OutputSchema>(std::move(cols1));

  EXPECT_EQ(*schema0, *schema1);
  EXPECT_EQ(schema0->Hash(), schema1->Hash());

  // Now make a different schema and check to make sure that it is not
  // equivalent and the hash is different
  OutputSchema::Column col2("XXX", type::TypeId::BOOLEAN, OutputSchemaTests::BuildDummyPredicate());
  std::vector<OutputSchema::Column> cols2;
  cols2.emplace_back(col2.Copy());
  auto schema2 = std::make_unique<OutputSchema>(std::move(cols2));

  EXPECT_NE(*schema0, *schema2);
  EXPECT_NE(schema0->Hash(), schema2->Hash());

  // Just check to make sure that we correctly capture that having more columns
  // means that the schema is different too
  std::vector<OutputSchema::Column> cols3;
  cols3.emplace_back(col0.Copy());
  cols3.emplace_back(col1.Copy());
  auto schema3 = std::make_unique<OutputSchema>(std::move(cols3));
  EXPECT_NE(*schema0, *schema3);
  EXPECT_NE(schema0->Hash(), schema3->Hash());
}

// NOLINTNEXTLINE
TEST(OutputSchemaTests, ColumnTest) {
  std::string name = "xxx";
  type::TypeId type_id = type::TypeId::INTEGER;

  OutputSchema::Column col0(name, type_id, OutputSchemaTests::BuildDummyPredicate());
  OutputSchema::Column col1("xxx", type::TypeId::INTEGER, OutputSchemaTests::BuildDummyPredicate());
  EXPECT_EQ(col0, col1);
  EXPECT_EQ(col0.Hash(), col1.Hash());

  // Swap out different parts of the column info and make sure that
  // it is never equal to or have the same hash as the original column
  for (int i = 0; i < 2; i++) {
    std::string other_name = name;
    type::TypeId other_type_id = type_id;

    switch (i) {
      case 0:
        other_name = "YYY";
        break;
      case 1:
        other_type_id = type::TypeId::BOOLEAN;
        break;
    }
    OutputSchema::Column other_col(other_name, other_type_id, OutputSchemaTests::BuildDummyPredicate());
    EXPECT_NE(col0, other_col) << "Iteration #" << i;
    EXPECT_NE(col0.Hash(), other_col.Hash()) << "Iteration #" << i;
  }
}

}  // namespace noisepage::planner
