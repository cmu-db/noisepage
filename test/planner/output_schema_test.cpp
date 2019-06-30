#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "planner/plannodes/output_schema.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

#include "util/test_harness.h"

namespace terrier::planner {

class OutputSchemaTests : public TerrierTest {
 public:
  /**
   * Constructs a dummy AbstractExpression predicate
   * @return dummy predicate
   */
  static std::shared_ptr<parser::AbstractExpression> BuildDummyPredicate() {
    return std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true));
  }
};

// NOLINTNEXTLINE
TEST(OutputSchemaTests, OutputSchemaTest) {
  // Create two OutputSchema objects with the same info.
  // They should hash to the same values and be equivalent

  OutputSchema::Column col0(type::TypeId::INTEGER, true, nullptr);
  std::vector<OutputSchema::Column> cols0 = {col0};
  auto schema0 = std::make_shared<OutputSchema>(cols0);

  OutputSchema::Column col1(type::TypeId::INTEGER, true, nullptr);
  std::vector<OutputSchema::Column> cols1 = {col1};
  auto schema1 = std::make_shared<OutputSchema>(cols1);

  EXPECT_EQ(*schema0, *schema1);
  EXPECT_EQ(schema0->Hash(), schema1->Hash());

  // Now make a different schema and check to make sure that it is not
  // equivalent and the hash is different
  OutputSchema::Column col2(type::TypeId::BOOLEAN, true, nullptr);
  std::vector<OutputSchema::Column> cols2 = {col2};
  auto schema2 = std::make_shared<OutputSchema>(cols2);

  EXPECT_NE(*schema0, *schema2);
  EXPECT_NE(schema0->Hash(), schema2->Hash());

  // Just check to make sure that we correctly capture that having more columns
  // means that the schema is different too
  std::vector<OutputSchema::Column> cols3 = {col0, col1};
  auto schema3 = std::make_shared<OutputSchema>(cols3);
  EXPECT_NE(*schema0, *schema3);
  EXPECT_NE(schema0->Hash(), schema3->Hash());
}

// NOLINTNEXTLINE
TEST(OutputSchemaTests, ColumnTest) {
  type::TypeId type_id = type::TypeId::INTEGER;
  bool nullable = true;
  std::shared_ptr<parser::AbstractExpression> expr = nullptr;

  OutputSchema::Column col0(type_id, nullable, expr);
  OutputSchema::Column col1(type::TypeId::INTEGER, true, nullptr);
  EXPECT_EQ(col0, col1);
  EXPECT_EQ(col0.Hash(), col1.Hash());

  // Swap out different parts of the column info and make sure that
  // it is never equal to or have the same hash as the original column
  for (int i = 0; i < 4; i++) {
    type::TypeId other_type_id = type_id;
    bool other_nullable = nullable;
    auto other_expr = expr;

    switch (i) {
      case 0:
        other_type_id = type::TypeId::BOOLEAN;
        break;
      case 1:
        other_nullable = !nullable;
        break;
      case 2:
        other_expr = std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetInteger(37));
        break;
    }
    OutputSchema::Column other_col(other_type_id, other_nullable, other_expr);
    EXPECT_NE(col0, other_col) << "Iteration #" << i;
    EXPECT_NE(col0.Hash(), other_col.Hash()) << "Iteration #" << i;
  }
}
}  // namespace terrier::planner
