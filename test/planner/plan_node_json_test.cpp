#include <memory>
#include <tuple>
#include <utility>
#include <vector>

#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/expression/tuple_value_expression.h"
#include "planner/plannodes/aggregate_plan_node.h"
#include "planner/plannodes/analyze_plan_node.h"
#include "planner/plannodes/create_database_plan_node.h"
#include "planner/plannodes/create_function_plan_node.h"
#include "planner/plannodes/create_index_plan_node.h"
#include "planner/plannodes/create_namespace_plan_node.h"
#include "planner/plannodes/create_table_plan_node.h"
#include "planner/plannodes/create_trigger_plan_node.h"
#include "planner/plannodes/create_view_plan_node.h"
#include "planner/plannodes/csv_scan_plan_node.h"
#include "planner/plannodes/delete_plan_node.h"
#include "planner/plannodes/drop_database_plan_node.h"
#include "planner/plannodes/drop_index_plan_node.h"
#include "planner/plannodes/drop_namespace_plan_node.h"
#include "planner/plannodes/drop_table_plan_node.h"
#include "planner/plannodes/drop_trigger_plan_node.h"
#include "planner/plannodes/drop_view_plan_node.h"
#include "planner/plannodes/export_external_file_plan_node.h"
#include "planner/plannodes/hash_join_plan_node.h"
#include "planner/plannodes/hash_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "planner/plannodes/insert_plan_node.h"
#include "planner/plannodes/limit_plan_node.h"
#include "planner/plannodes/nested_loop_join_plan_node.h"
#include "planner/plannodes/order_by_plan_node.h"
#include "planner/plannodes/output_schema.h"
#include "planner/plannodes/projection_plan_node.h"
#include "planner/plannodes/result_plan_node.h"
#include "planner/plannodes/seq_scan_plan_node.h"
#include "planner/plannodes/set_op_plan_node.h"
#include "planner/plannodes/update_plan_node.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/type_id.h"

#include "util/test_harness.h"

namespace terrier::planner {

class PlanNodeJsonTest : public TerrierTest {
 public:
  /**
   * Constructs a dummy OutputSchema object with a single column
   * @return dummy output schema
   */
  static std::shared_ptr<OutputSchema> BuildDummyOutputSchema() {
    OutputSchema::Column col("dummy_col", type::TypeId::INTEGER, true, catalog::col_oid_t(0));
    std::vector<OutputSchema::Column> cols;
    cols.push_back(col);
    auto schema = std::make_shared<OutputSchema>(cols);
    return schema;
  }

  /**
   * Constructs a dummy AbstractExpression predicate
   * @return dummy predicate
   */
  static std::shared_ptr<parser::AbstractExpression> BuildDummyPredicate() {
    return std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetBoolean(true));
  }
};

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, OutputSchemaJsonTest) {
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
  children.emplace_back(PlanNodeJsonTest::BuildDummyPredicate());
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
TEST(PlanNodeJsonTest, LimitPlanNodeJsonTest) {
  // Construct LimitPlanNode
  LimitPlanNode::Builder builder;
  auto plan_node =
      builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema()).SetLimit(10).SetOffset(10).Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::LIMIT, deserialized_plan->GetPlanNodeType());
  auto limit_plan = std::dynamic_pointer_cast<LimitPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *limit_plan);
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, IndexScanPlanNodeJsonTest) {
  // Construct IndexScanPlanNode
  IndexScanPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .SetScanPredicate(PlanNodeJsonTest::BuildDummyPredicate())
                       .SetIsParallelFlag(true)
                       .SetIsForUpdateFlag(false)
                       .SetDatabaseOid(catalog::db_oid_t(0))
                       .SetIndexOid(catalog::index_oid_t(0))
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::INDEXSCAN, deserialized_plan->GetPlanNodeType());
  auto index_scan_plan = std::dynamic_pointer_cast<IndexScanPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *index_scan_plan);
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, NestedLoopJoinPlanNodeJoinTest) {
  // Construct NestedLoopJoinPlanNode
  NestedLoopJoinPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .SetJoinType(LogicalJoinType::INNER)
                       .SetJoinPredicate(PlanNodeJsonTest::BuildDummyPredicate())
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::NESTLOOP, deserialized_plan->GetPlanNodeType());
  auto nested_loop_join_plan = std::dynamic_pointer_cast<NestedLoopJoinPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *nested_loop_join_plan);
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, ProjectionPlanNodeJsonTest) {
  // Construct ProjectionPlanNode
  ProjectionPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema()).Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::PROJECTION, deserialized_plan->GetPlanNodeType());
  auto projection_plan = std::dynamic_pointer_cast<ProjectionPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *projection_plan);
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, SeqScanPlanNodeJsonTest) {
  // Construct SeqScanPlanNode
  SeqScanPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .SetScanPredicate(PlanNodeJsonTest::BuildDummyPredicate())
                       .SetIsParallelFlag(true)
                       .SetIsForUpdateFlag(false)
                       .SetDatabaseOid(catalog::db_oid_t(0))
                       .SetTableOid(catalog::table_oid_t(0))
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::SEQSCAN, deserialized_plan->GetPlanNodeType());
  auto seq_scan_plan = std::dynamic_pointer_cast<SeqScanPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *seq_scan_plan);
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, OrderByPlanNodeJsonTest) {
  // Construct OrderByPlanNode
  OrderByPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .AddSortKey(catalog::col_oid_t(0), OrderByOrderingType::ASC)
                       .AddSortKey(catalog::col_oid_t(1), OrderByOrderingType::DESC)
                       .SetLimit(10)
                       .SetOffset(10)
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::ORDERBY, deserialized_plan->GetPlanNodeType());
  auto order_by_plan = std::dynamic_pointer_cast<OrderByPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *order_by_plan);
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, InsertPlanNodeJsonTest) {
  // Construct InsertPlanNode
  std::vector<type::TransientValue> values;
  values.push_back(type::TransientValueFactory::GetInteger(0));
  values.push_back(type::TransientValueFactory::GetBoolean(true));
  std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> parameter_info;
  parameter_info.emplace_back(0, 1, 2);
  parameter_info.emplace_back(3, 4, 5);
  InsertPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .SetDatabaseOid(catalog::db_oid_t(0))
                       .SetTableOid(catalog::table_oid_t(1))
                       .SetValues(std::move(values))
                       .SetParameterInfo(std::move(parameter_info))
                       .SetBulkInsertCount(1)
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::INSERT, deserialized_plan->GetPlanNodeType());
  auto insert_plan = std::dynamic_pointer_cast<InsertPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *insert_plan);
}

}  // namespace terrier::planner
