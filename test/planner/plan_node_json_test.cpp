#include <memory>
#include "planner/plannodes/abstract_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "type/type_id.h"

#include "util/test_harness.h"

namespace terrier::plan_node {

// TODO(Gus): Possible design flaw, must cast to AbstractPlanNode in order to serialize correctly

// NOLINTNEXTLINE
TEST(PlanNodeJsonTests, DISABLED_BasicTest) {
  // Create Limit plan node
  //  size_t limit = 10;
  //  size_t offset = 10;
  //  catalog::col_oid_t col_oid(0);
  //  OutputSchema::Column column("test", type::TypeId::INTEGER, true, col_oid);
  //  std::shared_ptr<OutputSchema> schema(new OutputSchema({column}));
  //  std::unique_ptr<AbstractPlanNode> original_plan(new LimitPlanNode(schema, limit, offset));
  //
  //  // Serialize to Json
  //  nlohmann::json json = dynamic_cast<AbstractPlanNode *>(original_plan.get())->ToJson();
  //  EXPECT_FALSE(json.is_null());
  //
  //  auto deserialized_plan = DeserializePlanNode(json);
  //  EXPECT_EQ(PlanNodeType::LIMIT, deserialized_plan->GetPlanNodeType());
  //  EXPECT_EQ(*original_plan, *deserialized_plan);
  //
  //  auto limit_plan = static_cast<LimitPlanNode *>(deserialized_plan.get());
  //  EXPECT_EQ(limit, limit_plan->GetLimit());
  //  EXPECT_EQ(offset, limit_plan->GetOffset());
}

}  // namespace terrier::plan_node
