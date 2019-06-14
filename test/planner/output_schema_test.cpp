#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "planner/plannodes/output_schema.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

#include "util/test_harness.h"

namespace terrier::planner {

class OutputSchemaTests : public TerrierTest {


};

// NOLINTNEXTLINE
TEST(OutputSchemaTests, EquivalenceTest) {

  // Create two OutputSchema objects with the same info.
  // They should hash to the same values and be equivalent

  OutputSchema::Column col0("dummy_col", type::TypeId::INTEGER, true, catalog::col_oid_t(0));
  std::vector<OutputSchema::Column> cols0 = { col0 };
  auto schema0 = std::make_shared<OutputSchema>(cols0);

  OutputSchema::Column col1("dummy_col", type::TypeId::INTEGER, true, catalog::col_oid_t(0));
  std::vector<OutputSchema::Column> cols1 = { col1 };
  auto schema1 = std::make_shared<OutputSchema>(cols1);

  EXPECT_EQ(*schema0, *schema1);
  EXPECT_EQ(schema0->Hash(), schema1->Hash());

  // Now make a different schema and check to make sure that it is not
  // equivalent and the hash is different
  OutputSchema::Column col2("XXX", type::TypeId::BOOLEAN, true, catalog::col_oid_t(1));
  std::vector<OutputSchema::Column> cols2 = { col2 };
  auto schema2 = std::make_shared<OutputSchema>(cols2);

  EXPECT_NE(*schema0, *schema2);
  EXPECT_NE(schema0->Hash(), schema2->Hash());

  // Just check to make sure that we correctly capture that having more columns
  // means that the schema is different too
  std::vector<OutputSchema::Column> cols3 = { col0, col1 };
  auto schema3 = std::make_shared<OutputSchema>(cols3);
  EXPECT_NE(*schema0, *schema3);
  EXPECT_NE(schema0->Hash(), schema3->Hash());

}


}  // namespace terrier::planner
