#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/tuple_value_expression.h"
#include "planner/plannodes/output_schema.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

#include "util/test_harness.h"

namespace terrier::planner {

// NOLINTNEXTLINE
TEST(PlanNodeJsonTests, OutputSchemaJsonTest) {
  // Test Column serialization
  OutputSchema::Column col("col1", type::TypeId::BOOLEAN, false /* nullable */, catalog::col_oid_t(0));
  auto col_json = col.ToJson();
  EXPECT_FALSE(col_json.is_null());

  OutputSchema::Column deserialized_col;
  deserialized_col.FromJson(col_json);
  EXPECT_EQ(col, deserialized_col);

  // Test DerivedColumn serialization
  std::vector<std::shared_ptr<parser::AbstractExpression>> children;
  children.emplace_back(std::make_shared<parser::TupleValueExpression>("table1", "col1"));
  children.emplace_back(
      std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true)));
  auto expr =
      std::make_shared<parser::ComparisonExpression>(parser::ExpressionType::CONJUNCTION_OR, std::move(children));

  OutputSchema::DerivedColumn derived_col(col, expr);
  auto derived_col_json = derived_col.ToJson();
  EXPECT_FALSE(derived_col_json.is_null());

  OutputSchema::DerivedColumn deserialized_derived_col;
  deserialized_derived_col.FromJson(derived_col_json);
  EXPECT_EQ(derived_col, deserialized_derived_col);

  // Test OutputSchema Serialization
  std::vector<OutputSchema::Column> cols;
  cols.push_back(col);
  std::vector<OutputSchema::DerivedTarget> targets;
  targets.emplace_back(0, derived_col);
  auto output_schema = std::make_shared<OutputSchema>(cols, targets);
  auto output_schema_json = output_schema->ToJson();
  EXPECT_FALSE(output_schema_json.is_null());

  std::shared_ptr<OutputSchema> deserialized_output_schema = std::make_shared<OutputSchema>();
  deserialized_output_schema->FromJson(output_schema_json);
  EXPECT_EQ(*output_schema, *deserialized_output_schema);
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTests, BasicTest) {
  EXPECT_TRUE(true);
  //   Create Limit plan node
  //    size_t limit = 10;
  //    size_t offset = 10;
  //    catalog::col_oid_t col_oid(0);
  //    OutputSchema::Column column("test", type::TypeId::INTEGER, true, col_oid);
  //    std::shared_ptr<OutputSchema> schema(new OutputSchema({column}));
  //    std::unique_ptr<AbstractPlanNode> original_plan(new LimitPlanNode(schema, limit, offset));
  //
  //    // Serialize to Json
  //    nlohmann::json json = dynamic_cast<AbstractPlanNode *>(original_plan.get())->ToJson();
  //    EXPECT_FALSE(json.is_null());
  //
  //    auto deserialized_plan = DeserializePlanNode(json);
  //    EXPECT_EQ(PlanNodeType::LIMIT, deserialized_plan->GetPlanNodeType());
  //    EXPECT_EQ(*original_plan, *deserialized_plan);
  //
  //    auto limit_plan = static_cast<LimitPlanNode *>(deserialized_plan.get());
  //    EXPECT_EQ(limit, limit_plan->GetLimit());
  //    EXPECT_EQ(offset, limit_plan->GetOffset());
}



}  // namespace terrier::planner
