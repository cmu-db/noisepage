#include "catalog/schema.h"
#include "plan_node/limit_plan_node.h"
#include "type/type_id.h"

#include "util/test_harness.h"

namespace terrier::plan_node {

/**
 * A very basic test, to verify that we can invoke the parser in libpg_query.
 */

// TODO(Gus): Possible design flag, must cast to AbstractPlanNode in order to serialize correctly

// NOLINTNEXTLINE
TEST(PlanNodeJsonTests, BasicTest) {
  // Create Limit plan node
  size_t limit = 10;
  size_t offset = 10;
  catalog::col_oid_t col_oid(0);
  catalog::Schema::Column column("test", type::TypeId::INTEGER, true, col_oid);
  std::shared_ptr<catalog::Schema> schema(new catalog::Schema({column}));
  std::unique_ptr<AbstractPlanNode> original_plan(new LimitPlanNode(schema, limit, offset));

  // Serialize to Json
  nlohmann::json json = dynamic_cast<AbstractPlanNode *>(original_plan.get())->ToJson();
  EXPECT_FALSE(json.is_null());

  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_EQ(PlanNodeType::LIMIT, deserialized_plan->GetPlanNodeType());
  EXPECT_EQ(*original_plan, *deserialized_plan);

  auto limit_plan = static_cast<LimitPlanNode *>(deserialized_plan.get());
  EXPECT_EQ(limit, limit_plan->GetLimit());
  EXPECT_EQ(offset, limit_plan->GetOffset());
}

}  // namespace terrier::plan_node