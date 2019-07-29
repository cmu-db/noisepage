#include <catalog/catalog_defs.h>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "parser/expression/column_value_expression.h"
#include "parser/expression/comparison_expression.h"
#include "parser/expression/conjunction_expression.h"
#include "parser/expression/constant_value_expression.h"
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

#include "util/storage_test_util.h"
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

  /**
   * Constructs a dummy SeqScanPlanNode to be used as a child for another plan
   */
  static std::shared_ptr<AbstractPlanNode> BuildDummySeqScanPlan() {
    SeqScanPlanNode::Builder builder;
    return builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
        .SetScanPredicate(PlanNodeJsonTest::BuildDummyPredicate())
        .SetIsParallelFlag(true)
        .SetIsForUpdateFlag(false)
        .SetDatabaseOid(catalog::db_oid_t(0))
        .SetTableOid(catalog::table_oid_t(0))
        .SetNamespaceOid(catalog::namespace_oid_t(0))
        .Build();
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
  children.emplace_back(std::make_shared<parser::ColumnValueExpression>("table1", "col1"));
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
  EXPECT_EQ(output_schema->Hash(), deserialized_output_schema->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, AggregatePlanNodeJsonTest) {
  // Construct AggregatePlanNode

  std::vector<std::shared_ptr<parser::AbstractExpression>> children;
  children.push_back(PlanNodeJsonTest::BuildDummyPredicate());
  auto agg_term = std::make_shared<parser::AggregateExpression>(parser::ExpressionType::AGGREGATE_COUNT,
                                                                std::move(children), false);
  AggregatePlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .SetAggregateStrategyType(AggregateStrategyType::HASH)
                       .SetHavingClausePredicate(PlanNodeJsonTest::BuildDummyPredicate())
                       .AddAggregateTerm(std::move(agg_term))
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::AGGREGATE, deserialized_plan->GetPlanNodeType());
  auto aggregate_plan = std::dynamic_pointer_cast<AggregatePlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *aggregate_plan);
  EXPECT_EQ(plan_node->Hash(), aggregate_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, AnalyzePlanNodeJsonTest) {
  // Construct AnalyzePlanNode
  AnalyzePlanNode::Builder builder;
  std::vector<catalog::col_oid_t> col_oids = {catalog::col_oid_t(1), catalog::col_oid_t(2), catalog::col_oid_t(3),
                                              catalog::col_oid_t(4), catalog::col_oid_t(5)};
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .SetDatabaseOid(catalog::db_oid_t(1))
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
                       .SetTableOid(catalog::table_oid_t(2))
                       .SetColumnOIDs(std::move(col_oids))
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::ANALYZE, deserialized_plan->GetPlanNodeType());
  auto analyze_plan = std::dynamic_pointer_cast<AnalyzePlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *analyze_plan);
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateDatabasePlanNodeTest) {
  // Construct CreateDatabasePlanNode
  CreateDatabasePlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseName("test_db").Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::CREATE_DATABASE, deserialized_plan->GetPlanNodeType());
  auto create_database_plan = std::dynamic_pointer_cast<CreateDatabasePlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *create_database_plan);
  EXPECT_EQ(plan_node->Hash(), create_database_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateFunctionPlanNodeTest) {
  // Construct CreateFunctionPlanNode
  CreateFunctionPlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(1))
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
                       .SetLanguage(parser::PLType::PL_PGSQL)
                       .SetFunctionParamNames({"i"})
                       .SetFunctionParamTypes({parser::BaseFunctionParameter::DataType::INT})
                       .SetColumnNames({"RETURN i+1;"})
                       .SetIsReplace(true)
                       .SetFunctionName("test_func")
                       .SetReturnType(parser::BaseFunctionParameter::DataType::INT)
                       .SetParamCount(1)
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::CREATE_FUNC, deserialized_plan->GetPlanNodeType());
  auto create_func_plan = std::dynamic_pointer_cast<CreateFunctionPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *create_func_plan);
  EXPECT_EQ(plan_node->Hash(), create_func_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateIndexPlanNodeTest) {
  // Construct CreateIndexPlanNode
  CreateIndexPlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(1))
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
                       .SetTableOid(catalog::table_oid_t(2))
                       .SetIndexName("test_index")
                       .SetUniqueIndex(true)
                       .SetIndexAttrs({"a", "foo"})
                       .SetKeyAttrs({"a", "bar"})
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::CREATE_INDEX, deserialized_plan->GetPlanNodeType());
  auto create_index_plan = std::dynamic_pointer_cast<CreateIndexPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *create_index_plan);
  EXPECT_EQ(plan_node->Hash(), create_index_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateNamespacePlanNodeTest) {
  // Construct CreateNamespacePlanNode
  CreateNamespacePlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(2)).SetNamespaceName("test_namespace").Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::CREATE_NAMESPACE, deserialized_plan->GetPlanNodeType());
  auto create_namespace_plan = std::dynamic_pointer_cast<CreateNamespacePlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *create_namespace_plan);
  EXPECT_EQ(plan_node->Hash(), create_namespace_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateTablePlanNodeTest) {
  // PRIMARY KEY
  auto get_pk_info = []() {
    PrimaryKeyInfo pk = {.primary_key_cols_ = {"a"}, .constraint_name_ = "pk_a"};
    return pk;
  };

  // FOREIGN KEY
  auto get_fk_info = []() {
    std::vector<ForeignKeyInfo> checks;
    ForeignKeyInfo fk = {.foreign_key_sources_ = {"b"},
                         .foreign_key_sinks_ = {"b"},
                         .sink_table_name_ = {"tbl2"},
                         .constraint_name_ = "fk_b",
                         .upd_action_ = parser::FKConstrActionType::CASCADE,
                         .del_action_ = parser::FKConstrActionType::CASCADE};
    checks.emplace_back(fk);
    return checks;
  };

  // UNIQUE CONSTRAINT
  auto get_unique_info = []() {
    std::vector<UniqueInfo> checks;
    UniqueInfo uk = {.unique_cols_ = {"u_a", "u_b"}, .constraint_name_ = "uk_a_b"};
    checks.emplace_back(uk);
    return checks;
  };

  // CHECK CONSTRAINT
  auto get_check_info = []() {
    type::TransientValue val = type::TransientValueFactory::GetInteger(1);
    std::vector<CheckInfo> checks;
    std::vector<std::string> cks = {"ck_a"};
    checks.emplace_back(cks, "ck_a", parser::ExpressionType::COMPARE_GREATER_THAN, std::move(val));
    return checks;
  };

  // Columns
  auto get_schema = []() {
    std::vector<catalog::Schema::Column> columns = {
        catalog::Schema::Column(
            "a", type::TypeId::INTEGER, false,
            parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::INTEGER))),
        catalog::Schema::Column(
            "u_a", type::TypeId::DECIMAL, false,
            parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::DECIMAL))),
        catalog::Schema::Column(
            "u_b", type::TypeId::DATE, true,
            parser::ConstantValueExpression(type::TransientValueFactory::GetNull(type::TypeId::DATE)))};
    StorageTestUtil::ForceOid(&(columns[0]), catalog::col_oid_t(1));
    StorageTestUtil::ForceOid(&(columns[1]), catalog::col_oid_t(2));
    StorageTestUtil::ForceOid(&(columns[2]), catalog::col_oid_t(3));
    return std::make_shared<catalog::Schema>(columns);
  };

  // Construct CreateTablePlanNode (1 with PK and 1 without PK)
  CreateTablePlanNode::Builder builder;
  auto pk_plan_node = builder.SetDatabaseOid(catalog::db_oid_t(1))
                          .SetNamespaceOid(catalog::namespace_oid_t(2))
                          .SetTableName("test_tbl")
                          .SetTableSchema(get_schema())
                          .SetHasPrimaryKey(true)
                          .SetPrimaryKey(get_pk_info())
                          .SetForeignKeys(get_fk_info())
                          .SetUniqueConstraints(get_unique_info())
                          .SetCheckConstraints(get_check_info())
                          .Build();

  auto no_pk_plan_node = builder.SetDatabaseOid(catalog::db_oid_t(1))
                             .SetNamespaceOid(catalog::namespace_oid_t(2))
                             .SetTableName("test_tbl")
                             .SetTableSchema(get_schema())
                             .SetHasPrimaryKey(false)
                             .SetPrimaryKey(get_pk_info())
                             .SetForeignKeys(get_fk_info())
                             .SetUniqueConstraints(get_unique_info())
                             .SetCheckConstraints(get_check_info())
                             .Build();
  EXPECT_NE(*pk_plan_node, *no_pk_plan_node);
  EXPECT_NE(pk_plan_node->Hash(), no_pk_plan_node->Hash());

  // Serialize to Json
  auto pk_json = pk_plan_node->ToJson();
  auto no_pk_json = no_pk_plan_node->ToJson();
  EXPECT_FALSE(pk_json.is_null());
  EXPECT_FALSE(no_pk_json.is_null());

  // Deserialize plan node
  auto deserialized_pk_plan = DeserializePlanNode(pk_json);
  auto deserialized_no_pk_plan = DeserializePlanNode(no_pk_json);
  EXPECT_TRUE(deserialized_pk_plan != nullptr);
  EXPECT_TRUE(deserialized_no_pk_plan != nullptr);

  EXPECT_EQ(PlanNodeType::CREATE_TABLE, deserialized_pk_plan->GetPlanNodeType());
  EXPECT_EQ(PlanNodeType::CREATE_TABLE, deserialized_no_pk_plan->GetPlanNodeType());

  auto create_table_pk_plan = std::dynamic_pointer_cast<CreateTablePlanNode>(deserialized_pk_plan);
  auto create_table_no_pk_plan = std::dynamic_pointer_cast<CreateTablePlanNode>(deserialized_no_pk_plan);

  EXPECT_NE(*create_table_pk_plan, *create_table_no_pk_plan);
  EXPECT_NE(create_table_pk_plan->Hash(), create_table_no_pk_plan->Hash());

  // PRIMARY KEY
  EXPECT_EQ(*pk_plan_node, *create_table_pk_plan);
  EXPECT_EQ(pk_plan_node->Hash(), create_table_pk_plan->Hash());

  // NO PRIMARY KEY
  EXPECT_EQ(*no_pk_plan_node, *create_table_no_pk_plan);
  EXPECT_EQ(no_pk_plan_node->Hash(), create_table_no_pk_plan->Hash());

  // Foreign Key Constraints
  EXPECT_EQ(create_table_pk_plan->GetForeignKeys().size(), 1);
  EXPECT_EQ(create_table_pk_plan->GetForeignKeys()[0], get_fk_info()[0]);
  EXPECT_EQ(create_table_pk_plan->GetForeignKeys()[0].Hash(), get_fk_info()[0].Hash());

  // Unique Constraints
  EXPECT_EQ(create_table_pk_plan->GetUniqueConstraints().size(), 1);
  EXPECT_EQ(create_table_pk_plan->GetUniqueConstraints()[0], get_unique_info()[0]);
  EXPECT_EQ(create_table_pk_plan->GetUniqueConstraints()[0].Hash(), get_unique_info()[0].Hash());

  // Check Constraints
  EXPECT_EQ(create_table_pk_plan->GetCheckConstraints().size(), 1);
  EXPECT_EQ(create_table_pk_plan->GetCheckConstraints()[0], get_check_info()[0]);
  EXPECT_EQ(create_table_pk_plan->GetCheckConstraints()[0].Hash(), get_check_info()[0].Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateTriggerPlanNodeTest) {
  // Construct CreateTriggerPlanNode
  CreateTriggerPlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(2))
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
                       .SetTableOid(catalog::table_oid_t(3))
                       .SetTriggerName("test_trigger")
                       .SetTriggerFuncnames({"test_trigger_func"})
                       .SetTriggerArgs({"a", "b"})
                       .SetTriggerColumns({catalog::col_oid_t(0), catalog::col_oid_t(1)})
                       .SetTriggerWhen(PlanNodeJsonTest::BuildDummyPredicate())
                       .SetTriggerType(23)
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::CREATE_TRIGGER, deserialized_plan->GetPlanNodeType());
  auto create_trigger_plan = std::dynamic_pointer_cast<CreateTriggerPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *create_trigger_plan);
  EXPECT_EQ(plan_node->Hash(), create_trigger_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CreateViewPlanNodeTest) {
  // Construct CreateViewPlanNode
  CreateViewPlanNode::Builder builder;
  std::shared_ptr<parser::SelectStatement> select_stmt = std::make_shared<parser::SelectStatement>();
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(2))
                       .SetNamespaceOid(catalog::namespace_oid_t(3))
                       .SetViewName("test_view")
                       .SetViewQuery(select_stmt)
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::CREATE_VIEW, deserialized_plan->GetPlanNodeType());
  auto create_view_plan = std::dynamic_pointer_cast<CreateViewPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *create_view_plan);
  EXPECT_EQ(plan_node->Hash(), create_view_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, CSVScanPlanNodeTest) {
  // Construct CSVScanPlanNode
  CSVScanPlanNode::Builder builder;
  auto plan_node =
      builder.SetFileName("/dev/null").SetDelimiter(',').SetQuote('\'').SetEscape('`').SetNullString("").Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::CSVSCAN, deserialized_plan->GetPlanNodeType());
  auto csv_scan_plan = std::dynamic_pointer_cast<CSVScanPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *csv_scan_plan);
  EXPECT_EQ(plan_node->Hash(), csv_scan_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DeletePlanNodeTest) {
  // Construct DeletePlanNode
  DeletePlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(1))
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
                       .SetTableOid(catalog::table_oid_t(2))
                       .SetDeleteCondition(PlanNodeJsonTest::BuildDummyPredicate())
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::DELETE, deserialized_plan->GetPlanNodeType());
  auto delete_plan = std::dynamic_pointer_cast<DeletePlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *delete_plan);
  EXPECT_EQ(plan_node->Hash(), delete_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DropDatabasePlanNodeTest) {
  // Construct DropDatabasePlanNode
  DropDatabasePlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(7)).SetIfExist(true).Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::DROP_DATABASE, deserialized_plan->GetPlanNodeType());
  auto drop_database_plan = std::dynamic_pointer_cast<DropDatabasePlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *drop_database_plan);
  EXPECT_EQ(plan_node->Hash(), drop_database_plan->Hash());

  // Sanity check to make sure that it actually fails if the plan nodes are truly different
  DropDatabasePlanNode::Builder builder2;
  auto plan_node2 = builder2.SetDatabaseOid(catalog::db_oid_t(9999)).SetIfExist(true).Build();
  auto json2 = plan_node2->ToJson();
  auto deserialized_plan2 = DeserializePlanNode(json2);
  auto drop_database_plan2 = std::dynamic_pointer_cast<DropDatabasePlanNode>(deserialized_plan2);
  EXPECT_NE(*plan_node, *drop_database_plan2);
  EXPECT_NE(*drop_database_plan, *drop_database_plan2);
  EXPECT_NE(plan_node->Hash(), drop_database_plan2->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DropIndexPlanNodeTest) {
  // Construct DropIndexPlanNode
  DropIndexPlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(7))
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
                       .SetIndexOid(catalog::index_oid_t(8))
                       .SetIfExist(true)
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::DROP_INDEX, deserialized_plan->GetPlanNodeType());
  auto drop_index_plan = std::dynamic_pointer_cast<DropIndexPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *drop_index_plan);
  EXPECT_EQ(plan_node->Hash(), drop_index_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DropNamespacePlanNodeTest) {
  // Construct DropNamespacePlanNode
  DropNamespacePlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(8))
                       .SetNamespaceOid(catalog::namespace_oid_t(9))
                       .SetIfExist(true)
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::DROP_NAMESPACE, deserialized_plan->GetPlanNodeType());
  auto drop_namespace_plan = std::dynamic_pointer_cast<DropNamespacePlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *drop_namespace_plan);
  EXPECT_EQ(plan_node->Hash(), drop_namespace_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DropTablePlanNodeTest) {
  // Construct DropTablePlanNode
  DropTablePlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(9))
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
                       .SetTableOid(catalog::table_oid_t(10))
                       .SetIfExist(true)
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::DROP_TABLE, deserialized_plan->GetPlanNodeType());
  auto drop_table_plan = std::dynamic_pointer_cast<DropTablePlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *drop_table_plan);
  EXPECT_EQ(plan_node->Hash(), drop_table_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DropTriggerPlanNodeTest) {
  // Construct DropTriggerPlanNode
  DropTriggerPlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(10))
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
                       .SetTriggerOid(catalog::trigger_oid_t(11))
                       .SetIfExist(true)
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::DROP_TRIGGER, deserialized_plan->GetPlanNodeType());
  auto drop_trigger_plan = std::dynamic_pointer_cast<DropTriggerPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *drop_trigger_plan);
  EXPECT_EQ(plan_node->Hash(), drop_trigger_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, DropViewPlanNodeTest) {
  // Construct DropViewPlanNode
  DropViewPlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(11))
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
                       .SetViewOid(catalog::view_oid_t(12))
                       .SetIfExist(true)
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::DROP_VIEW, deserialized_plan->GetPlanNodeType());
  auto drop_view_plan = std::dynamic_pointer_cast<DropViewPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *drop_view_plan);
  EXPECT_EQ(plan_node->Hash(), drop_view_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, ExportExternalFilePlanNodeJsonTest) {
  // Construct ExportExternalFilePlanNode
  ExportExternalFilePlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .SetFileName("test_file")
                       .SetDelimiter(',')
                       .SetEscape('"')
                       .SetQuote('"')
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::EXPORT_EXTERNAL_FILE, deserialized_plan->GetPlanNodeType());
  auto export_external_file_plan = std::dynamic_pointer_cast<ExportExternalFilePlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *export_external_file_plan);
  EXPECT_EQ(plan_node->Hash(), export_external_file_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, HashJoinPlanNodeJoinTest) {
  // Construct HashJoinPlanNode
  HashJoinPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .SetJoinType(LogicalJoinType::INNER)
                       .SetJoinPredicate(PlanNodeJsonTest::BuildDummyPredicate())
                       .AddLeftHashKey(std::make_shared<parser::ColumnValueExpression>("table1", "col1"))
                       .AddRightHashKey(std::make_shared<parser::ColumnValueExpression>("table2", "col2"))
                       .SetBuildBloomFilterFlag(false)
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::HASHJOIN, deserialized_plan->GetPlanNodeType());
  auto hash_join_plan = std::dynamic_pointer_cast<HashJoinPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *hash_join_plan);
  EXPECT_EQ(plan_node->Hash(), hash_join_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, HashPlanNodeJsonTest) {
  // Construct HashPlanNode
  HashPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .AddHashKey(std::make_shared<parser::ColumnValueExpression>("table1", "col1"))
                       .AddHashKey(std::make_shared<parser::ColumnValueExpression>("col2", "table1"))
                       .AddChild(PlanNodeJsonTest::BuildDummySeqScanPlan())
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::HASH, deserialized_plan->GetPlanNodeType());
  auto hash_plan = std::dynamic_pointer_cast<HashPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *hash_plan);
  EXPECT_EQ(plan_node->Hash(), hash_plan->Hash());
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
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
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
  EXPECT_EQ(plan_node->Hash(), index_scan_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, InsertPlanNodeJsonTest) {
  // Construct InsertPlanNode

  // Values Generator
  auto get_values = [](int offset, int num_cols) {
    std::vector<type::TransientValue> tuple;
    tuple.push_back(type::TransientValueFactory::GetInteger(offset));
    for (; num_cols - 1 > 0; num_cols--) {
      tuple.push_back(type::TransientValueFactory::GetBoolean(true));
    }
    return tuple;
  };

  InsertPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .SetDatabaseOid(catalog::db_oid_t(0))
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
                       .SetTableOid(catalog::table_oid_t(1))
                       .AddValues(get_values(0, 2))
                       .AddValues(get_values(1, 2))
                       .AddParameterInfo(0, catalog::col_oid_t(0))
                       .AddParameterInfo(1, catalog::col_oid_t(1))
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
  EXPECT_EQ(plan_node->Hash(), insert_plan->Hash());

  // Make sure that we are checking the ParameterInfo map correctly!
  InsertPlanNode::Builder builder2;
  auto plan_node2 = builder2.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                        .SetDatabaseOid(catalog::db_oid_t(0))
                        .SetNamespaceOid(catalog::namespace_oid_t(0))
                        .SetTableOid(catalog::table_oid_t(1))
                        .AddValues(get_values(0, 3))
                        .AddValues(get_values(1, 3))
                        .AddParameterInfo(0, catalog::col_oid_t(0))
                        .AddParameterInfo(1, catalog::col_oid_t(1))
                        .AddParameterInfo(8, catalog::col_oid_t(999))
                        .Build();
  auto json2 = plan_node2->ToJson();
  EXPECT_FALSE(json2.is_null());
  auto deserialized_plan2 = DeserializePlanNode(json2);
  EXPECT_TRUE(deserialized_plan2 != nullptr);
  EXPECT_EQ(PlanNodeType::INSERT, deserialized_plan2->GetPlanNodeType());
  auto insert_plan2 = std::dynamic_pointer_cast<InsertPlanNode>(deserialized_plan2);
  EXPECT_NE(*plan_node, *insert_plan2);
  EXPECT_NE(plan_node->Hash(), insert_plan2->Hash());
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
  EXPECT_EQ(plan_node->Hash(), limit_plan->Hash());
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
  EXPECT_EQ(plan_node->Hash(), nested_loop_join_plan->Hash());
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
  EXPECT_EQ(plan_node->Hash(), order_by_plan->Hash());
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
  EXPECT_EQ(plan_node->Hash(), projection_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, ResultPlanNodeJsonTest) {
  // Construct ResultPlanNode
  ResultPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .SetExpr(PlanNodeJsonTest::BuildDummyPredicate())
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::RESULT, deserialized_plan->GetPlanNodeType());
  auto result_plan = std::dynamic_pointer_cast<ResultPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *result_plan);
  EXPECT_EQ(plan_node->Hash(), result_plan->Hash());
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
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
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
  EXPECT_EQ(plan_node->Hash(), seq_scan_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, SetOpPlanNodeJsonTest) {
  // Construct SetOpPlanNode
  SetOpPlanNode::Builder builder;
  auto plan_node =
      builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema()).SetSetOp(SetOpType::INTERSECT).Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::SETOP, deserialized_plan->GetPlanNodeType());
  auto set_op_plan = std::dynamic_pointer_cast<SetOpPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *set_op_plan);
  EXPECT_EQ(plan_node->Hash(), set_op_plan->Hash());
}

// NOLINTNEXTLINE
TEST(PlanNodeJsonTest, UpdatePlanNodeJsonTest) {
  UpdatePlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(PlanNodeJsonTest::BuildDummyOutputSchema())
                       .SetDatabaseOid(catalog::db_oid_t(1000))
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
                       .SetTableOid(catalog::table_oid_t(200))
                       .SetUpdatePrimaryKey(true)
                       .Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::UPDATE, deserialized_plan->GetPlanNodeType());
  auto update_plan = std::dynamic_pointer_cast<UpdatePlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *update_plan);
  EXPECT_EQ(plan_node->Hash(), update_plan->Hash());
}

}  // namespace terrier::planner
