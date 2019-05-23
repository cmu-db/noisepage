#include <catalog/catalog_defs.h>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>
#include <unordered_map>

#include "execution/ast/ast_dump.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/execution_consumer.h"
#include "execution/compiler/query.h"
#include "execution/exec/execution_context.h"
#include "execution/exec/output.h"
#include "execution/sema/sema.h"
#include "execution/sql/execution_structures.h"
#include "execution/sql/value.h"
#include "execution/util/cpu_info.h"
#include "execution/vm/bytecode_generator.h"
#include "execution/vm/bytecode_module.h"
#include "execution/vm/llvm_engine.h"

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

class CompilerTest : public TerrierTest {
 public:
  static void CompileAndRun(terrier::planner::AbstractPlanNode *node, const std::string &schema = "output1.tpl") {
    std::cout << "Input Plan: " << std::endl;
    std::cout << node->ToJson().dump(2) << std::endl << std::endl;

    // Create the query object, whose region must outlive all the processing.
    tpl::compiler::Query query(*node);

    // Figure out the final schema.
    auto exec = tpl::sql::ExecutionStructures::Instance();
    auto os_cols = query.GetPlan().GetOutputSchema()->GetColumns();

    std::vector<catalog::Schema::Column> cols;
    cols.reserve(os_cols.size());
    std::unordered_map<u32, u32> offsets;
    offsets.reserve(os_cols.size());
    for (u32 i = 0, cur_sz = 0, col_sz = static_cast<u32>(os_cols.size()); i < col_sz; ++i) {
      auto column = os_cols[i];
      cols.emplace_back(column.GetName(), column.GetType(), column.GetNullable(), column.GetOid());
      offsets[i] = cur_sz;
      cur_sz += tpl::sql::ValUtil::GetSqlSize(column.GetType());
    }
    auto final = std::make_shared<tpl::exec::FinalSchema>(cols, offsets);

    // Use the final schema to create the appropriate ExecutionConsumer.
    tpl::compiler::ExecutionConsumer consumer(final);
    tpl::compiler::CompilationContext ctx(&query, &consumer);
    ctx.GeneratePlan(&query);

    if (ctx.GetCodeGen()->GetCodeContext()->GetReporter()->HasErrors()) {
      EXECUTION_LOG_ERROR("Type-checking error!");
      ctx.GetCodeGen()->GetCodeContext()->GetReporter()->PrintErrors();
    }

    std::cout << "Converted: " << std::endl;
    auto ast = query.GetCompiledFunction();
    tpl::ast::AstDump::Dump(ast);

    // init TPL
    tpl::CpuInfo::Instance();

    tpl::sql::ExecutionStructures::Instance();
    tpl::vm::LLVMEngine::Initialize();

    auto *txn = exec->GetTxnManager()->BeginTransaction();

    tpl::exec::OutputPrinter printer(*final);
    auto exec_context = std::make_shared<tpl::exec::ExecutionContext>(txn, printer, final);

    std::function<u32()> main_func;
    auto module = tpl::vm::BytecodeGenerator::Compile(ast, "main", exec_context);
    if (!module->GetFunction("main", tpl::vm::ExecutionMode::Interpret, &main_func)) {
      EXECUTION_LOG_ERROR("Couldn't find main()?");
      // TODO(WAN): throw error
    }

    std::cout << "Executed: " << std::endl;
    main_func();
    exec->GetTxnManager()->Commit(txn, nullptr, nullptr);

    // shutdown TPL
    tpl::vm::LLVMEngine::Shutdown();
  }

  /**
   * Constructs a dummy OutputSchema object with a single column
   * @return dummy output schema
   */
  static std::shared_ptr<OutputSchema> BuildDummyOutputSchema() {
    OutputSchema::Column col("colA", type::TypeId::INTEGER, true, catalog::col_oid_t(0));
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
    return builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
        .SetScanPredicate(CompilerTest::BuildDummyPredicate())
        .SetIsParallelFlag(true)
        .SetIsForUpdateFlag(false)
        .SetDatabaseOid(catalog::DEFAULT_DATABASE_OID)
        .SetTableOid(catalog::table_oid_t())
        .SetNamespaceOid(catalog::namespace_oid_t(0))
        .Build();
  }

  static std::shared_ptr<parser::AbstractExpression> BuildConstantComparisonPredicate(const std::string &table_name,
                                                                                      const std::string &col_name,
                                                                                      uint32_t val) {
    auto tuple_expr = std::make_shared<parser::TupleValueExpression>(col_name, table_name);
    auto val_expr = std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetInteger(val));
    return std::make_shared<parser::ComparisonExpression>(
        parser::ExpressionType::COMPARE_LESS_THAN,
        std::vector<std::shared_ptr<parser::AbstractExpression>>{tuple_expr, val_expr});
  }
};

/*
// NOLINTNEXTLINE
TEST_F(CompilerTest, OutputSchemaJsonTest) {
  // Test Column serialization
  OutputSchema::Column col("col1", type::TypeId::BOOLEAN, false, catalog::col_oid_t(0));
  auto col_json = col.ToJson();
  EXPECT_FALSE(col_json.is_null());

  OutputSchema::Column deserialized_col;
  deserialized_col.FromJson(col_json);
  EXPECT_EQ(col, deserialized_col);

  // Test DerivedColumn serialization
  std::vector<std::shared_ptr<parser::AbstractExpression>> children;
  children.emplace_back(std::make_shared<parser::TupleValueExpression>("table1", "col1"));
  children.emplace_back(CompilerTest::BuildDummyPredicate());
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
TEST_F(CompilerTest, AggregatePlanNodeJsonTest) {
  // Construct AggregatePlanNode

  std::vector<std::shared_ptr<parser::AbstractExpression>> children;
  children.push_back(CompilerTest::BuildDummyPredicate());
  auto agg_term = std::make_shared<parser::AggregateExpression>(parser::ExpressionType::AGGREGATE_COUNT_STAR,
                                                                std::move(children), false);
  AggregatePlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
      .SetAggregateStrategyType(AggregateStrategyType::HASH)
      .SetHavingClausePredicate(CompilerTest::BuildDummyPredicate())
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, AnalyzePlanNodeJsonTest) {
  // Construct AnalyzePlanNode
  AnalyzePlanNode::Builder builder;
  std::vector<catalog::col_oid_t> col_oids = {catalog::col_oid_t(1), catalog::col_oid_t(2), catalog::col_oid_t(3),
                                              catalog::col_oid_t(4), catalog::col_oid_t(5)};
  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
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
TEST_F(CompilerTest, CreateDatabasePlanNodeTest) {
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, CreateFunctionPlanNodeTest) {
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, CreateIndexPlanNodeTest) {
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, CreateNamespacePlanNodeTest) {
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, CreateTablePlanNodeTest) {
  auto get_pk_info = []() {
    PrimaryKeyInfo pk = {.primary_key_cols_ = {"a"}, .constraint_name_ = "pk_a"};

    return pk;
  };

  auto get_fk_info = []() {
    ForeignKeyInfo fk = {.foreign_key_sources_ = {"b"},
        .foreign_key_sinks_ = {"b"},
        .sink_table_name_ = {"tbl2"},
        .constraint_name_ = "fk_b",
        .upd_action_ = parser::FKConstrActionType::CASCADE,
        .del_action_ = parser::FKConstrActionType::CASCADE};

    return fk;
  };

  auto get_unique_info = []() {
    UniqueInfo uk = {.unique_cols_ = {"u_a", "u_b"}, .constraint_name_ = "uk_a_b"};

    return uk;
  };

  auto get_check_info = []() {
    type::TransientValue val = type::TransientValueFactory::GetInteger(1);
    std::vector<CheckInfo> checks;
    std::vector<std::string> cks = {"ck_a"};
    checks.emplace_back(cks, "ck_a", parser::ExpressionType::COMPARE_GREATER_THAN, std::move(val));
    return checks;
  };

  auto get_schema = []() {
    std::vector<catalog::Schema::Column> columns = {
        catalog::Schema::Column("a", type::TypeId::INTEGER, false, catalog::col_oid_t(1)),
        catalog::Schema::Column("u_a", type::TypeId::DECIMAL, false, catalog::col_oid_t(2)),
        catalog::Schema::Column("u_b", type::TypeId::DATE, true, catalog::col_oid_t(3))};

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
      .SetForeignKeys({get_fk_info()})
      .SetUniqueConstraints({get_unique_info()})
      .SetCheckConstraints(get_check_info())
      .Build();

  auto no_pk_plan_node = builder.SetDatabaseOid(catalog::db_oid_t(1))
      .SetNamespaceOid(catalog::namespace_oid_t(2))
      .SetTableName("test_tbl")
      .SetTableSchema(get_schema())
      .SetHasPrimaryKey(false)
      .SetPrimaryKey(get_pk_info())
      .SetForeignKeys({get_fk_info()})
      .SetUniqueConstraints({get_unique_info()})
      .SetCheckConstraints(get_check_info())
      .Build();
  EXPECT_TRUE(*pk_plan_node != *no_pk_plan_node);

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
  EXPECT_TRUE(*create_table_pk_plan != *create_table_no_pk_plan);
  EXPECT_EQ(*pk_plan_node, *create_table_pk_plan);
  EXPECT_EQ(*no_pk_plan_node, *create_table_no_pk_plan);
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, CreateTriggerPlanNodeTest) {
  // Construct CreateTriggerPlanNode
  CreateTriggerPlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(2))
      .SetNamespaceOid(catalog::namespace_oid_t(0))
      .SetTableOid(catalog::table_oid_t(3))
      .SetTriggerName("test_trigger")
      .SetTriggerFuncnames({"test_trigger_func"})
      .SetTriggerArgs({"a", "b"})
      .SetTriggerColumns({catalog::col_oid_t(0), catalog::col_oid_t(1)})
      .SetTriggerWhen(CompilerTest::BuildDummyPredicate())
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, CreateViewPlanNodeTest) {
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, CSVScanPlanNodeTest) {
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, DeletePlanNodeTest) {
  // Construct DeletePlanNode
  DeletePlanNode::Builder builder;
  auto plan_node = builder.SetDatabaseOid(catalog::db_oid_t(1))
      .SetNamespaceOid(catalog::namespace_oid_t(0))
      .SetTableOid(catalog::table_oid_t(2))
      .SetDeleteCondition(CompilerTest::BuildDummyPredicate())
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, DropDatabasePlanNodeTest) {
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, DropIndexPlanNodeTest) {
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, DropNamespacePlanNodeTest) {
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, DropTablePlanNodeTest) {
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, DropTriggerPlanNodeTest) {
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, DropViewPlanNodeTest) {
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, ExportExternalFilePlanNodeJsonTest) {
  // Construct ExportExternalFilePlanNode
  ExportExternalFilePlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, HashJoinPlanNodeJoinTest) {
  // Construct HashJoinPlanNode
  HashJoinPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
      .SetJoinType(LogicalJoinType::INNER)
      .SetJoinPredicate(CompilerTest::BuildDummyPredicate())
      .AddLeftHashKey(std::make_shared<parser::TupleValueExpression>("col1", "table1"))
      .AddRightHashKey(std::make_shared<parser::TupleValueExpression>("col2", "table2"))
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, HashPlanNodeJsonTest) {
  // Construct HashPlanNode
  HashPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
      .AddHashKey(std::make_shared<parser::TupleValueExpression>("col1", "table1"))
      .AddHashKey(std::make_shared<parser::TupleValueExpression>("col2", "table1"))
      .AddChild(CompilerTest::BuildDummySeqScanPlan())
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
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, IndexScanPlanNodeJsonTest) {
  // Construct IndexScanPlanNode
  IndexScanPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
      .SetScanPredicate(CompilerTest::BuildDummyPredicate())
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
}
*/
// NOLINTNEXTLINE
TEST_F(CompilerTest, InsertPlanNodeJsonTest) {
  std::vector<type::TransientValue> values;
  values.emplace_back(type::TransientValueFactory::GetInteger(15));
  values.emplace_back(type::TransientValueFactory::GetBoolean(false));

  std::vector<OutputSchema::Column> cols;
  cols.emplace_back("colA", type::TypeId::INTEGER, true, catalog::col_oid_t(13));
  cols.emplace_back("colB", type::TypeId::BOOLEAN, true, catalog::col_oid_t(14));
  auto schema = std::make_shared<OutputSchema>(cols);

  InsertPlanNode::Builder builder;

  auto empty_table2_oid = tpl::sql::ExecutionStructures::Instance()
                              ->GetCatalog()
                              ->GetCatalogTable(catalog::DEFAULT_DATABASE_OID, "empty_table2")
                              ->Oid();

  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
                       .SetDatabaseOid(catalog::DEFAULT_DATABASE_OID)
                       .SetNamespaceOid(catalog::namespace_oid_t(0))  // TODO(WAN): currently unused
                       .SetTableOid(empty_table2_oid)
                       .SetValues(std::move(values))
                       .AddParameterInfo(0, 1, 2)  // TODO(WAN): ask Gus if he needs this, I don't think we do
                       .AddParameterInfo(3, 4, 5)
                       .SetBulkInsertCount(1)  // TODO(WAN): this parameter still makes no sense
                       .Build();

  CompileAndRun(plan_node.get());
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, InsertSelectPlanNodeJsonTest) {
  std::vector<type::TransientValue> values;
  values.emplace_back(type::TransientValueFactory::GetInteger(15));
  values.emplace_back(type::TransientValueFactory::GetBoolean(false));

  std::vector<OutputSchema::Column> cols;
  cols.emplace_back("colA", type::TypeId::INTEGER, true, catalog::col_oid_t(13));
  cols.emplace_back("colB", type::TypeId::BOOLEAN, true, catalog::col_oid_t(14));
  auto schema = std::make_shared<OutputSchema>(cols);

  InsertPlanNode::Builder builder;

  SeqScanPlanNode::Builder seq_builder;
  auto table_oid = tpl::sql::ExecutionStructures::Instance()
                       ->GetCatalog()
                       ->GetCatalogTable(catalog::DEFAULT_DATABASE_OID, "test_1")
                       ->Oid();
  auto seq_plan_node = seq_builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
                           .SetScanPredicate(CompilerTest::BuildConstantComparisonPredicate("test_1", "colA", 500))
                           .SetIsParallelFlag(true)
                           .SetIsForUpdateFlag(false)
                           .SetDatabaseOid(catalog::DEFAULT_DATABASE_OID)
                           .SetNamespaceOid(catalog::namespace_oid_t(0))
                           .SetTableOid(table_oid)
                           .Build();

  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
                       .SetDatabaseOid(catalog::db_oid_t(1))
                       .SetNamespaceOid(catalog::namespace_oid_t(0))  // TODO(WAN): currently unused
                       .SetTableOid(catalog::table_oid_t(15))         // TODO(WAN): testing like this is miserable
                       .SetValues(std::move(values))
                       .AddParameterInfo(0, 1, 2)  // TODO(WAN): ask Gus if he needs this, I don't think we do
                       .AddParameterInfo(3, 4, 5)
                       .SetBulkInsertCount(1)  // TODO(WAN): this parameter still makes no sense
                       .AddChild(seq_plan_node)
                       .Build();

  CompileAndRun(plan_node.get());
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, SimpleInsertTest) {
  auto empty_table2 = tpl::sql::ExecutionStructures::Instance()
                          ->GetCatalog()
                          ->GetCatalogTable(catalog::DEFAULT_DATABASE_OID, "empty_table2")
                          ->GetSqlTable();

  std::vector<OutputSchema::Column> cols;
  cols.emplace_back("colA", type::TypeId::INTEGER, true, catalog::col_oid_t(13));
  cols.emplace_back("colB", type::TypeId::BOOLEAN, true, catalog::col_oid_t(14));
  auto schema = std::make_shared<OutputSchema>(cols);

  // scan empty_table2, should be empty
  {
    SeqScanPlanNode::Builder builder;
    auto plan_node = builder.SetOutputSchema(schema)
                         .SetScanPredicate(nullptr)
                         .SetIsParallelFlag(true)
                         .SetIsForUpdateFlag(false)
                         .SetDatabaseOid(catalog::DEFAULT_DATABASE_OID)
                         .SetNamespaceOid(catalog::namespace_oid_t(0))
                         .SetTableOid(empty_table2->Oid())
                         .Build();

    CompileAndRun(plan_node.get());
  }

  // insert {15, false}
  {
    std::vector<type::TransientValue> values;
    values.emplace_back(type::TransientValueFactory::GetInteger(15));
    values.emplace_back(type::TransientValueFactory::GetBoolean(false));

    InsertPlanNode::Builder builder;
    auto plan_node = builder.SetOutputSchema(schema)
                         .SetDatabaseOid(catalog::DEFAULT_DATABASE_OID)
                         .SetNamespaceOid(catalog::namespace_oid_t(0))  // TODO(WAN): currently unused
                         .SetTableOid(empty_table2->Oid())
                         .SetValues(std::move(values))
                         .AddParameterInfo(0, 1, 2)
                         .AddParameterInfo(3, 4, 5)
                         .SetBulkInsertCount(1)
                         .Build();

    CompileAndRun(plan_node.get());
  }

  // insert {721, true}
  {
    std::vector<type::TransientValue> values;
    values.emplace_back(type::TransientValueFactory::GetInteger(721));
    values.emplace_back(type::TransientValueFactory::GetBoolean(true));

    InsertPlanNode::Builder builder;
    auto plan_node = builder.SetOutputSchema(schema)
                         .SetDatabaseOid(catalog::DEFAULT_DATABASE_OID)
                         .SetNamespaceOid(catalog::namespace_oid_t(0))  // TODO(WAN): currently unused
                         .SetTableOid(empty_table2->Oid())
                         .SetValues(std::move(values))
                         .AddParameterInfo(0, 1, 2)
                         .AddParameterInfo(3, 4, 5)
                         .SetBulkInsertCount(1)
                         .Build();

    CompileAndRun(plan_node.get());
  }

  // scan empty_table2, should have two tuples now
  {
    SeqScanPlanNode::Builder builder;
    auto plan_node = builder.SetOutputSchema(schema)
                         .SetScanPredicate(nullptr)
                         .SetIsParallelFlag(true)
                         .SetIsForUpdateFlag(false)
                         .SetDatabaseOid(catalog::DEFAULT_DATABASE_OID)
                         .SetNamespaceOid(catalog::namespace_oid_t(0))
                         .SetTableOid(empty_table2->Oid())
                         .Build();

    CompileAndRun(plan_node.get());
  }
}
/*
// NOLINTNEXTLINE
TEST_F(CompilerTest, LimitPlanNodeJsonTest) {
  // Construct LimitPlanNode
  LimitPlanNode::Builder builder;
  auto plan_node =
      builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema()).SetLimit(10).SetOffset(10).Build();

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
TEST_F(CompilerTest, NestedLoopJoinPlanNodeJoinTest) {
  // Construct NestedLoopJoinPlanNode
  NestedLoopJoinPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
      .SetJoinType(LogicalJoinType::INNER)
      .SetJoinPredicate(CompilerTest::BuildDummyPredicate())
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
TEST_F(CompilerTest, OrderByPlanNodeJsonTest) {
  // Construct OrderByPlanNode
  OrderByPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
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
TEST_F(CompilerTest, ProjectionPlanNodeJsonTest) {
  // Construct ProjectionPlanNode
  ProjectionPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema()).Build();

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
TEST_F(CompilerTest, ResultPlanNodeJsonTest) {
  // Construct ResultPlanNode
  ResultPlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
      .SetExpr(CompilerTest::BuildDummyPredicate())
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
}
*/

// NOLINTNEXTLINE
TEST_F(CompilerTest, SeqScanPlanNodeJsonTest) {
  // Construct SeqScanPlanNode
  SeqScanPlanNode::Builder builder;
  auto table_oid = tpl::sql::ExecutionStructures::Instance()
                       ->GetCatalog()
                       ->GetCatalogTable(catalog::DEFAULT_DATABASE_OID, "test_1")
                       ->Oid();
  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
                       .SetScanPredicate(CompilerTest::BuildConstantComparisonPredicate("test_1", "colA", 500))
                       .SetIsParallelFlag(true)
                       .SetIsForUpdateFlag(false)
                       .SetDatabaseOid(catalog::DEFAULT_DATABASE_OID)
                       .SetNamespaceOid(catalog::namespace_oid_t(0))
                       .SetTableOid(table_oid)
                       .Build();

  CompileAndRun(plan_node.get());
}
/*
// NOLINTNEXTLINE
TEST_F(CompilerTest, SetOpPlanNodeJsonTest) {
  // Construct SetOpPlanNode
  SetOpPlanNode::Builder builder;
  auto plan_node =
      builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema()).SetSetOp(SetOpType::INTERSECT).Build();

  // Serialize to Json
  auto json = plan_node->ToJson();
  EXPECT_FALSE(json.is_null());

  // Deserialize plan node
  auto deserialized_plan = DeserializePlanNode(json);
  EXPECT_TRUE(deserialized_plan != nullptr);
  EXPECT_EQ(PlanNodeType::SETOP, deserialized_plan->GetPlanNodeType());
  auto set_op_plan = std::dynamic_pointer_cast<SetOpPlanNode>(deserialized_plan);
  EXPECT_EQ(*plan_node, *set_op_plan);
}

// NOLINTNEXTLINE
TEST_F(CompilerTest, UpdatePlanNodeJsonTest) {
  UpdatePlanNode::Builder builder;
  auto plan_node = builder.SetOutputSchema(CompilerTest::BuildDummyOutputSchema())
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
}
 */
}  // namespace terrier::planner
