#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "optimizer/operator_expression.h"
#include "optimizer/operators.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/update_statement.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"

#include "gtest/gtest.h"

namespace terrier::optimizer {

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalUpdateTest) {
  std::string column = "abc";
  std::shared_ptr<parser::AbstractExpression> value =
      std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetTinyInt(1));
  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);

  // Check that all of our GET methods work as expected
  Operator op1 = LogicalUpdate::make(database_oid, namespace_oid, table_oid, {});
  EXPECT_EQ(op1.GetType(), OpType::LOGICALUPDATE);
  EXPECT_EQ(op1.As<LogicalUpdate>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.As<LogicalUpdate>()->GetNamespaceOid(), namespace_oid);
  EXPECT_EQ(op1.As<LogicalUpdate>()->GetTableOid(), table_oid);
  EXPECT_EQ(op1.As<LogicalUpdate>()->GetUpdateClauses().size(), 0);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 = LogicalUpdate::make(database_oid, namespace_oid, table_oid, {});
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  catalog::db_oid_t other_database_oid(999);
  Operator op3 = LogicalUpdate::make(other_database_oid, namespace_oid, table_oid, {});
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalExportExternalFileTest) {
  std::string file_name = "fakefile.txt";
  char delimiter = 'X';
  char quote = 'Y';
  char escape = 'Z';

  // Check that all of our GET methods work as expected
  Operator op1 =
      LogicalExportExternalFile::make(parser::ExternalFileFormat::BINARY, file_name, delimiter, quote, escape);
  EXPECT_EQ(op1.GetType(), OpType::LOGICALEXPORTEXTERNALFILE);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetFilename(), file_name);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetDelimiter(), delimiter);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetQuote(), quote);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetEscape(), escape);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  const std::string &file_name_copy = file_name;
  Operator op2 =
      LogicalExportExternalFile::make(parser::ExternalFileFormat::BINARY, file_name_copy, delimiter, quote, escape);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  Operator op3 = LogicalExportExternalFile::make(parser::ExternalFileFormat::CSV, file_name, delimiter, quote, escape);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalGetTest) {
  //===--------------------------------------------------------------------===//
  // LogicalGet
  //===--------------------------------------------------------------------===//
  Operator logical_get_1 = LogicalGet::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>(), "table", false);
  Operator logical_get_2 = LogicalGet::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>(), "table", false);

  auto annotated_expr = AnnotatedExpression(nullptr, std::unordered_set<std::string>());
  Operator logical_get_3 = LogicalGet::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>{annotated_expr}, "table", false);

  EXPECT_EQ(logical_get_1.GetType(), OpType::LOGICALGET);
  EXPECT_EQ(logical_get_1.As<LogicalGet>()->GetDatabaseOID(), catalog::db_oid_t(1));
  EXPECT_EQ(logical_get_1.As<LogicalGet>()->GetNamespaceOID(), catalog::namespace_oid_t(2));
  EXPECT_EQ(logical_get_1.As<LogicalGet>()->GetTableOID(), catalog::table_oid_t(3));
  EXPECT_EQ(logical_get_1.As<LogicalGet>()->GetPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_get_3.As<LogicalGet>()->GetPredicates(), std::vector<AnnotatedExpression>{annotated_expr});
  EXPECT_EQ(logical_get_1.As<LogicalGet>()->GetTableAlias(), "table");
  EXPECT_EQ(logical_get_1.As<LogicalGet>()->GetIsForUpdate(), false);
  EXPECT_EQ(logical_get_1.GetName(), "LogicalGet");
  EXPECT_TRUE(logical_get_1 == logical_get_2);
  EXPECT_FALSE(logical_get_1 == logical_get_3);
  EXPECT_EQ(logical_get_1.Hash(), logical_get_2.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalExternalFileGetTest) {
  //===--------------------------------------------------------------------===//
  // LogicalExternalFileGet
  //===--------------------------------------------------------------------===//
  Operator logical_ext_file_get_1 =
      LogicalExternalFileGet::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\');
  Operator logical_ext_file_get_2 =
      LogicalExternalFileGet::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\');
  Operator logical_ext_file_get_3 =
      LogicalExternalFileGet::make(parser::ExternalFileFormat::CSV, "file2.txt", ',', '"', '\\');
  Operator logical_ext_file_get_4 =
      LogicalExternalFileGet::make(parser::ExternalFileFormat::BINARY, "file.txt", ',', '"', '\\');
  Operator logical_ext_file_get_5 =
      LogicalExternalFileGet::make(parser::ExternalFileFormat::CSV, "file.txt", ' ', '"', '\\');
  Operator logical_ext_file_get_6 =
      LogicalExternalFileGet::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '\'', '\\');
  Operator logical_ext_file_get_7 =
      LogicalExternalFileGet::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '&');

  EXPECT_EQ(logical_ext_file_get_1.GetType(), OpType::LOGICALEXTERNALFILEGET);
  EXPECT_EQ(logical_ext_file_get_1.GetName(), "LogicalExternalFileGet");
  EXPECT_EQ(logical_ext_file_get_1.As<LogicalExternalFileGet>()->GetFormat(), parser::ExternalFileFormat::CSV);
  EXPECT_EQ(logical_ext_file_get_1.As<LogicalExternalFileGet>()->GetFilename(), "file.txt");
  EXPECT_EQ(logical_ext_file_get_1.As<LogicalExternalFileGet>()->GetDelimiter(), ',');
  EXPECT_EQ(logical_ext_file_get_1.As<LogicalExternalFileGet>()->GetQuote(), '"');
  EXPECT_EQ(logical_ext_file_get_1.As<LogicalExternalFileGet>()->GetEscape(), '\\');
  EXPECT_TRUE(logical_ext_file_get_1 == logical_ext_file_get_2);
  EXPECT_FALSE(logical_ext_file_get_1 == logical_ext_file_get_3);
  EXPECT_FALSE(logical_ext_file_get_1 == logical_ext_file_get_4);
  EXPECT_FALSE(logical_ext_file_get_1 == logical_ext_file_get_5);
  EXPECT_FALSE(logical_ext_file_get_1 == logical_ext_file_get_6);
  EXPECT_FALSE(logical_ext_file_get_1 == logical_ext_file_get_7);
  EXPECT_TRUE(logical_ext_file_get_1.Hash() == logical_ext_file_get_2.Hash());
  EXPECT_FALSE(logical_ext_file_get_1.Hash() == logical_ext_file_get_3.Hash());
  EXPECT_FALSE(logical_ext_file_get_1.Hash() == logical_ext_file_get_4.Hash());
  EXPECT_FALSE(logical_ext_file_get_1.Hash() == logical_ext_file_get_5.Hash());
  EXPECT_FALSE(logical_ext_file_get_1.Hash() == logical_ext_file_get_6.Hash());
  EXPECT_FALSE(logical_ext_file_get_1.Hash() == logical_ext_file_get_7.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalQueryDerivedGetTest) {
  //===--------------------------------------------------------------------===//
  // LogicalQueryDerivedGet
  //===--------------------------------------------------------------------===//
  auto alias_to_expr_map_1 = std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>();
  auto alias_to_expr_map_1_1 = std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>();
  auto alias_to_expr_map_2 = std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>();
  auto alias_to_expr_map_3 = std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>();
  auto alias_to_expr_map_4 = std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>();
  auto alias_to_expr_map_5 = std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>();

  auto expr1 = std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetTinyInt(1));
  auto expr2 = std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetTinyInt(1));
  alias_to_expr_map_1["constant expr"] = expr1;
  alias_to_expr_map_1_1["constant expr"] = expr1;
  alias_to_expr_map_2["constant expr"] = expr1;
  alias_to_expr_map_3["constant expr"] = expr2;
  alias_to_expr_map_4["constant expr2"] = expr1;
  alias_to_expr_map_5["constant expr"] = expr1;
  alias_to_expr_map_5["constant expr2"] = expr2;

  Operator logical_query_derived_get_1 = LogicalQueryDerivedGet::make("alias", std::move(alias_to_expr_map_1));
  Operator logical_query_derived_get_2 = LogicalQueryDerivedGet::make("alias", std::move(alias_to_expr_map_2));
  Operator logical_query_derived_get_3 = LogicalQueryDerivedGet::make(
      "alias", std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>());
  Operator logical_query_derived_get_4 = LogicalQueryDerivedGet::make("alias", std::move(alias_to_expr_map_3));
  Operator logical_query_derived_get_5 = LogicalQueryDerivedGet::make("alias", std::move(alias_to_expr_map_4));
  Operator logical_query_derived_get_6 = LogicalQueryDerivedGet::make("alias", std::move(alias_to_expr_map_5));

  EXPECT_EQ(logical_query_derived_get_1.GetType(), OpType::LOGICALQUERYDERIVEDGET);
  EXPECT_EQ(logical_query_derived_get_1.GetName(), "LogicalQueryDerivedGet");
  EXPECT_EQ(logical_query_derived_get_1.As<LogicalQueryDerivedGet>()->GetTableAlias(), "alias");
  EXPECT_EQ(logical_query_derived_get_1.As<LogicalQueryDerivedGet>()->GetAliasToExprMap(), alias_to_expr_map_1_1);
  EXPECT_TRUE(logical_query_derived_get_1 == logical_query_derived_get_2);
  EXPECT_FALSE(logical_query_derived_get_1 == logical_query_derived_get_3);
  EXPECT_FALSE(logical_query_derived_get_1 == logical_query_derived_get_4);
  EXPECT_FALSE(logical_query_derived_get_1 == logical_query_derived_get_5);
  EXPECT_FALSE(logical_query_derived_get_1 == logical_query_derived_get_6);
  EXPECT_TRUE(logical_query_derived_get_1.Hash() == logical_query_derived_get_2.Hash());
  EXPECT_FALSE(logical_query_derived_get_1.Hash() == logical_query_derived_get_3.Hash());
  EXPECT_FALSE(logical_query_derived_get_1.Hash() == logical_query_derived_get_4.Hash());
  EXPECT_FALSE(logical_query_derived_get_1.Hash() == logical_query_derived_get_5.Hash());
  EXPECT_FALSE(logical_query_derived_get_1.Hash() == logical_query_derived_get_6.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalDependentJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalDependentJoin
  //===--------------------------------------------------------------------===//
  Operator logical_dep_join_0 = LogicalDependentJoin::make();
  Operator logical_dep_join_1 = LogicalDependentJoin::make(std::vector<AnnotatedExpression>());
  Operator logical_dep_join_2 = LogicalDependentJoin::make(std::vector<AnnotatedExpression>());
  auto annotated_expr = AnnotatedExpression(nullptr, std::unordered_set<std::string>());
  Operator logical_dep_join_3 = LogicalDependentJoin::make(std::vector<AnnotatedExpression>{annotated_expr});

  EXPECT_EQ(logical_dep_join_1.GetType(), OpType::LOGICALDEPENDENTJOIN);
  EXPECT_EQ(logical_dep_join_3.GetType(), OpType::LOGICALDEPENDENTJOIN);
  EXPECT_EQ(logical_dep_join_1.GetName(), "LogicalDependentJoin");
  EXPECT_TRUE(logical_dep_join_1 == logical_dep_join_0);
  EXPECT_TRUE(logical_dep_join_1.Hash() == logical_dep_join_0.Hash());
  EXPECT_TRUE(logical_dep_join_1.Hash() == logical_dep_join_2.Hash());
  EXPECT_FALSE(logical_dep_join_1.Hash() == logical_dep_join_3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalMarkJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalMarkJoin
  //===--------------------------------------------------------------------===//
  Operator logical_mark_join_0 = LogicalMarkJoin::make();
  Operator logical_mark_join_1 = LogicalMarkJoin::make(std::vector<AnnotatedExpression>());
  Operator logical_mark_join_2 = LogicalMarkJoin::make(std::vector<AnnotatedExpression>());
  auto annotated_expr = AnnotatedExpression(nullptr, std::unordered_set<std::string>());
  Operator logical_mark_join_3 = LogicalMarkJoin::make(std::vector<AnnotatedExpression>{annotated_expr});

  EXPECT_EQ(logical_mark_join_1.GetType(), OpType::LOGICALMARKJOIN);
  EXPECT_EQ(logical_mark_join_3.GetType(), OpType::LOGICALMARKJOIN);
  EXPECT_EQ(logical_mark_join_1.GetName(), "LogicalMarkJoin");
  EXPECT_EQ(logical_mark_join_1.As<LogicalMarkJoin>()->GetJoinPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_mark_join_3.As<LogicalMarkJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr});
  EXPECT_TRUE(logical_mark_join_1 == logical_mark_join_2);
  EXPECT_FALSE(logical_mark_join_1 == logical_mark_join_3);
  EXPECT_TRUE(logical_mark_join_1 == logical_mark_join_0);
  EXPECT_TRUE(logical_mark_join_1.Hash() == logical_mark_join_0.Hash());
  EXPECT_TRUE(logical_mark_join_1.Hash() == logical_mark_join_2.Hash());
  EXPECT_FALSE(logical_mark_join_1.Hash() == logical_mark_join_3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalSingleJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalSingleJoin
  //===--------------------------------------------------------------------===//
  Operator logical_single_join_0 = LogicalSingleJoin::make();
  Operator logical_single_join_1 = LogicalSingleJoin::make(std::vector<AnnotatedExpression>());
  Operator logical_single_join_2 = LogicalSingleJoin::make(std::vector<AnnotatedExpression>());
  auto annotated_expr = AnnotatedExpression(nullptr, std::unordered_set<std::string>());
  Operator logical_single_join_3 = LogicalSingleJoin::make(std::vector<AnnotatedExpression>{annotated_expr});

  EXPECT_EQ(logical_single_join_1.GetType(), OpType::LOGICALSINGLEJOIN);
  EXPECT_EQ(logical_single_join_3.GetType(), OpType::LOGICALSINGLEJOIN);
  EXPECT_EQ(logical_single_join_1.GetName(), "LogicalSingleJoin");
  EXPECT_EQ(logical_single_join_1.As<LogicalSingleJoin>()->GetJoinPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_single_join_3.As<LogicalSingleJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr});
  EXPECT_TRUE(logical_single_join_1 == logical_single_join_2);
  EXPECT_FALSE(logical_single_join_1 == logical_single_join_3);
  EXPECT_TRUE(logical_single_join_1 == logical_single_join_0);
  EXPECT_TRUE(logical_single_join_1.Hash() == logical_single_join_0.Hash());
  EXPECT_TRUE(logical_single_join_1.Hash() == logical_single_join_2.Hash());
  EXPECT_FALSE(logical_single_join_1.Hash() == logical_single_join_3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalInnerJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalInnerJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  std::shared_ptr<parser::AbstractExpression> x_1 = std::shared_ptr<parser::AbstractExpression>(expr_b_1);
  std::shared_ptr<parser::AbstractExpression> x_2 = std::shared_ptr<parser::AbstractExpression>(expr_b_2);
  std::shared_ptr<parser::AbstractExpression> x_3 = std::shared_ptr<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 = AnnotatedExpression(nullptr, std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_inner_join_0 = LogicalInnerJoin::make();
  Operator logical_inner_join_1 = LogicalInnerJoin::make(std::vector<AnnotatedExpression>());
  Operator logical_inner_join_2 = LogicalInnerJoin::make(std::vector<AnnotatedExpression>());
  Operator logical_inner_join_3 = LogicalInnerJoin::make(std::vector<AnnotatedExpression>{annotated_expr_0});
  Operator logical_inner_join_4 = LogicalInnerJoin::make(std::vector<AnnotatedExpression>{annotated_expr_1});
  Operator logical_inner_join_5 = LogicalInnerJoin::make(std::vector<AnnotatedExpression>{annotated_expr_2});
  Operator logical_inner_join_6 = LogicalInnerJoin::make(std::vector<AnnotatedExpression>{annotated_expr_3});


  EXPECT_EQ(logical_inner_join_1.GetType(), OpType::LOGICALINNERJOIN);
  EXPECT_EQ(logical_inner_join_3.GetType(), OpType::LOGICALINNERJOIN);
  EXPECT_EQ(logical_inner_join_1.GetName(), "LogicalInnerJoin");
  EXPECT_EQ(logical_inner_join_1.As<LogicalInnerJoin>()->GetJoinPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_inner_join_3.As<LogicalInnerJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(logical_inner_join_4.As<LogicalInnerJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_TRUE(logical_inner_join_0 == logical_inner_join_1);
  EXPECT_TRUE(logical_inner_join_1 == logical_inner_join_2);
  EXPECT_FALSE(logical_inner_join_1 == logical_inner_join_3);
  EXPECT_TRUE(logical_inner_join_4 == logical_inner_join_5);
  EXPECT_FALSE(logical_inner_join_4 == logical_inner_join_6);
  EXPECT_TRUE(logical_inner_join_1.Hash() == logical_inner_join_0.Hash());
  EXPECT_TRUE(logical_inner_join_1.Hash() == logical_inner_join_2.Hash());
  EXPECT_FALSE(logical_inner_join_1.Hash() == logical_inner_join_3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalLeftJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalLeftJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));
  std::shared_ptr<parser::AbstractExpression> x_1 = std::shared_ptr<parser::AbstractExpression>(expr_b_1);
  std::shared_ptr<parser::AbstractExpression> x_2 = std::shared_ptr<parser::AbstractExpression>(expr_b_2);
  std::shared_ptr<parser::AbstractExpression> x_3 = std::shared_ptr<parser::AbstractExpression>(expr_b_3);

  Operator logical_left_join_1 = LogicalLeftJoin::make(x_1);
  Operator logical_left_join_2 = LogicalLeftJoin::make(x_2);
  Operator logical_left_join_3 = LogicalLeftJoin::make(x_3);

  EXPECT_EQ(logical_left_join_1.GetType(), OpType::LOGICALLEFTJOIN);
  EXPECT_EQ(logical_left_join_3.GetType(), OpType::LOGICALLEFTJOIN);
  EXPECT_EQ(logical_left_join_1.GetName(), "LogicalLeftJoin");
  EXPECT_EQ(*(logical_left_join_1.As<LogicalLeftJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(logical_left_join_2.As<LogicalLeftJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(logical_left_join_3.As<LogicalLeftJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(logical_left_join_1 == logical_left_join_2);
  EXPECT_FALSE(logical_left_join_1 == logical_left_join_3);
  EXPECT_TRUE(logical_left_join_1.Hash() == logical_left_join_2.Hash());
  EXPECT_FALSE(logical_left_join_1.Hash() == logical_left_join_3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalRightJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalRightJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));
  std::shared_ptr<parser::AbstractExpression> x_1 = std::shared_ptr<parser::AbstractExpression>(expr_b_1);
  std::shared_ptr<parser::AbstractExpression> x_2 = std::shared_ptr<parser::AbstractExpression>(expr_b_2);
  std::shared_ptr<parser::AbstractExpression> x_3 = std::shared_ptr<parser::AbstractExpression>(expr_b_3);

  Operator logical_right_join_1 = LogicalRightJoin::make(x_1);
  Operator logical_right_join_2 = LogicalRightJoin::make(x_2);
  Operator logical_right_join_3 = LogicalRightJoin::make(x_3);

  EXPECT_EQ(logical_right_join_1.GetType(), OpType::LOGICALRIGHTJOIN);
  EXPECT_EQ(logical_right_join_3.GetType(), OpType::LOGICALRIGHTJOIN);
  EXPECT_EQ(logical_right_join_1.GetName(), "LogicalRightJoin");
  EXPECT_EQ(*(logical_right_join_1.As<LogicalRightJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(logical_right_join_2.As<LogicalRightJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(logical_right_join_3.As<LogicalRightJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(logical_right_join_1 == logical_right_join_2);
  EXPECT_FALSE(logical_right_join_1 == logical_right_join_3);
  EXPECT_TRUE(logical_right_join_1.Hash() == logical_right_join_2.Hash());
  EXPECT_FALSE(logical_right_join_1.Hash() == logical_right_join_3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalOuterJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalOuterJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));
  std::shared_ptr<parser::AbstractExpression> x_1 = std::shared_ptr<parser::AbstractExpression>(expr_b_1);
  std::shared_ptr<parser::AbstractExpression> x_2 = std::shared_ptr<parser::AbstractExpression>(expr_b_2);
  std::shared_ptr<parser::AbstractExpression> x_3 = std::shared_ptr<parser::AbstractExpression>(expr_b_3);

  Operator logical_outer_join_1 = LogicalOuterJoin::make(x_1);
  Operator logical_outer_join_2 = LogicalOuterJoin::make(x_2);
  Operator logical_outer_join_3 = LogicalOuterJoin::make(x_3);

  EXPECT_EQ(logical_outer_join_1.GetType(), OpType::LOGICALOUTERJOIN);
  EXPECT_EQ(logical_outer_join_3.GetType(), OpType::LOGICALOUTERJOIN);
  EXPECT_EQ(logical_outer_join_1.GetName(), "LogicalOuterJoin");
  EXPECT_EQ(*(logical_outer_join_1.As<LogicalOuterJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(logical_outer_join_2.As<LogicalOuterJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(logical_outer_join_3.As<LogicalOuterJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(logical_outer_join_1 == logical_outer_join_2);
  EXPECT_FALSE(logical_outer_join_1 == logical_outer_join_3);
  EXPECT_TRUE(logical_outer_join_1.Hash() == logical_outer_join_2.Hash());
  EXPECT_FALSE(logical_outer_join_1.Hash() == logical_outer_join_3.Hash());
}
// NOLINTNEXTLINE
TEST(OperatorTests, SeqScanTest) {
  //===--------------------------------------------------------------------===//
  // SeqScan
  //===--------------------------------------------------------------------===//
  Operator seq_scan_1 = SeqScan::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                      "table", std::vector<AnnotatedExpression>(), false);
  Operator seq_scan_2 = SeqScan::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                      "table", std::vector<AnnotatedExpression>(), false);

  auto annotated_expr = AnnotatedExpression(nullptr, std::unordered_set<std::string>());
  Operator seq_scan_3 = SeqScan::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                      "table", std::vector<AnnotatedExpression>{annotated_expr}, false);

  EXPECT_EQ(seq_scan_1.GetType(), OpType::SEQSCAN);
  EXPECT_EQ(seq_scan_1.GetName(), "SeqScan");
  EXPECT_EQ(seq_scan_1.As<IndexScan>(), nullptr);
  EXPECT_TRUE(seq_scan_1 == seq_scan_2);
  EXPECT_FALSE(seq_scan_1 == seq_scan_3);
}

// NOLINTNEXTLINE
TEST(OperatorTests, IndexScanTest) {
  //===--------------------------------------------------------------------===//
  // IndexScan
  //===--------------------------------------------------------------------===//
  Operator index_scan_1 =
      IndexScan::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3), "table",
                      std::vector<AnnotatedExpression>(), false, std::vector<catalog::col_oid_t>(),
                      std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  Operator index_scan_2 =
      IndexScan::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3), "table",
                      std::vector<AnnotatedExpression>(), false, std::vector<catalog::col_oid_t>(),
                      std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  Operator index_scan_3 =
      IndexScan::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(4), "table",
                      std::vector<AnnotatedExpression>(), false, std::vector<catalog::col_oid_t>(),
                      std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());

  EXPECT_EQ(index_scan_1.GetType(), OpType::INDEXSCAN);
  EXPECT_EQ(index_scan_1.GetName(), "IndexScan");
  EXPECT_TRUE(index_scan_1 == index_scan_2);
  EXPECT_FALSE(index_scan_1 == index_scan_3);
}

// NOLINTNEXTLINE
TEST(OperatorTests, ExternalFileScanTest) {
  //===--------------------------------------------------------------------===//
  // ExternalFileScan
  //===--------------------------------------------------------------------===//
  Operator ext_file_scan_1 = ExternalFileScan::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\');
  Operator ext_file_scan_2 = ExternalFileScan::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\');
  Operator ext_file_scan_3 = ExternalFileScan::make(parser::ExternalFileFormat::CSV, "file2.txt", ',', '"', '\\');

  EXPECT_EQ(ext_file_scan_1.GetType(), OpType::EXTERNALFILESCAN);
  EXPECT_EQ(ext_file_scan_1.GetName(), "ExternalFileScan");
  EXPECT_TRUE(ext_file_scan_1 == ext_file_scan_2);
  EXPECT_FALSE(ext_file_scan_1 == ext_file_scan_3);
}

// NOLINTNEXTLINE
TEST(OperatorTests, QueryDerivedScanTest) {
  //===--------------------------------------------------------------------===//
  // QueryDerivedScan
  //===--------------------------------------------------------------------===//
  auto alias_to_expr_map_1 = std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>();
  auto alias_to_expr_map_2 = std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>();
  auto expr_query_derived_scan =
      std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetTinyInt(1));
  alias_to_expr_map_1["constant expr"] = expr_query_derived_scan;
  alias_to_expr_map_2["constant expr"] = expr_query_derived_scan;
  Operator query_derived_scan_1 = QueryDerivedScan::make("alias", std::move(alias_to_expr_map_1));
  Operator query_derived_scan_2 = QueryDerivedScan::make("alias", std::move(alias_to_expr_map_2));
  Operator query_derived_scan_3 =
      QueryDerivedScan::make("alias", std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>());

  EXPECT_EQ(query_derived_scan_1.GetType(), OpType::QUERYDERIVEDSCAN);
  EXPECT_EQ(query_derived_scan_1.GetName(), "QueryDerivedScan");
  EXPECT_TRUE(query_derived_scan_1 == query_derived_scan_2);
  EXPECT_FALSE(query_derived_scan_1 == query_derived_scan_3);
}

// NOLINTNEXTLINE
TEST(OperatorTests, OrderByTest) {
  //===--------------------------------------------------------------------===//
  // OrderBy
  //===--------------------------------------------------------------------===//
  Operator order_by = OrderBy::make();

  EXPECT_EQ(order_by.GetType(), OpType::ORDERBY);
  EXPECT_EQ(order_by.GetName(), "OrderBy");
}

// NOLINTNEXTLINE
TEST(OperatorTests, LimitTest) {
  //===--------------------------------------------------------------------===//
  // Limit
  //===--------------------------------------------------------------------===//
  Operator limit = Limit::make(0, 0, std::vector<parser::AbstractExpression *>(), std::vector<bool>());

  EXPECT_EQ(limit.GetType(), OpType::LIMIT);
  EXPECT_EQ(limit.GetName(), "Limit");
}

// NOLINTNEXTLINE
TEST(OperatorTests, InnerNLJoinTest) {
  //===--------------------------------------------------------------------===//
  // InnerNLJoin
  //===--------------------------------------------------------------------===//
  Operator inner_nl_join_1 =
      InnerNLJoin::make(std::vector<AnnotatedExpression>(), std::vector<std::unique_ptr<parser::AbstractExpression>>(),
                        std::vector<std::unique_ptr<parser::AbstractExpression>>());
  Operator inner_nl_join_2 =
      InnerNLJoin::make(std::vector<AnnotatedExpression>(), std::vector<std::unique_ptr<parser::AbstractExpression>>(),
                        std::vector<std::unique_ptr<parser::AbstractExpression>>());

  EXPECT_EQ(inner_nl_join_1.GetType(), OpType::INNERNLJOIN);
  EXPECT_EQ(inner_nl_join_1.GetName(), "InnerNLJoin");
  EXPECT_TRUE(inner_nl_join_1 == inner_nl_join_2);
}

// NOLINTNEXTLINE
TEST(OperatorTests, LeftNLJoinTest) {
  //===--------------------------------------------------------------------===//
  // LeftNLJoin
  //===--------------------------------------------------------------------===//
  Operator left_nl_join = LeftNLJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(left_nl_join.GetType(), OpType::LEFTNLJOIN);
  EXPECT_EQ(left_nl_join.GetName(), "LeftNLJoin");
}

// NOLINTNEXTLINE
TEST(OperatorTests, RightNLJoinTest) {
  //===--------------------------------------------------------------------===//
  // RightNLJoin
  //===--------------------------------------------------------------------===//
  Operator right_nl_join = RightNLJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(right_nl_join.GetType(), OpType::RIGHTNLJOIN);
  EXPECT_EQ(right_nl_join.GetName(), "RightNLJoin");
}

// NOLINTNEXTLINE
TEST(OperatorTests, OuterNLJoin) {
  //===--------------------------------------------------------------------===//
  // OuterNLJoin
  //===--------------------------------------------------------------------===//
  Operator outer_nl_join = OuterNLJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(outer_nl_join.GetType(), OpType::OUTERNLJOIN);
  EXPECT_EQ(outer_nl_join.GetName(), "OuterNLJoin");
}

// NOLINTNEXTLINE
TEST(OperatorTests, InnerHashJoinTest) {
  //===--------------------------------------------------------------------===//
  // InnerHashJoin
  //===--------------------------------------------------------------------===//
  Operator inner_hash_join_1 = InnerHashJoin::make(std::vector<AnnotatedExpression>(),
                                                   std::vector<std::unique_ptr<parser::AbstractExpression>>(),
                                                   std::vector<std::unique_ptr<parser::AbstractExpression>>());
  Operator inner_hash_join_2 = InnerHashJoin::make(std::vector<AnnotatedExpression>(),
                                                   std::vector<std::unique_ptr<parser::AbstractExpression>>(),
                                                   std::vector<std::unique_ptr<parser::AbstractExpression>>());

  EXPECT_EQ(inner_hash_join_1.GetType(), OpType::INNERHASHJOIN);
  EXPECT_EQ(inner_hash_join_1.GetName(), "InnerHashJoin");
  EXPECT_TRUE(inner_hash_join_1 == inner_hash_join_2);
}

// NOLINTNEXTLINE
TEST(OperatorTests, LeftHashJoinTest) {
  //===--------------------------------------------------------------------===//
  // LeftHashJoin
  //===--------------------------------------------------------------------===//
  Operator left_hash_join = LeftHashJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(left_hash_join.GetType(), OpType::LEFTHASHJOIN);
  EXPECT_EQ(left_hash_join.GetName(), "LeftHashJoin");
}

// NOLINTNEXTLINE
TEST(OperatorTests, RightHashJoinTest) {
  //===--------------------------------------------------------------------===//
  // RightHashJoin
  //===--------------------------------------------------------------------===//
  Operator right_hash_join = RightHashJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(right_hash_join.GetType(), OpType::RIGHTHASHJOIN);
  EXPECT_EQ(right_hash_join.GetName(), "RightHashJoin");
}

// NOLINTNEXTLINE
TEST(OperatorTests, OuterHashJoinTest) {
  //===--------------------------------------------------------------------===//
  // OuterHashJoin
  //===--------------------------------------------------------------------===//
  Operator outer_hash_join = OuterHashJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(outer_hash_join.GetType(), OpType::OUTERHASHJOIN);
  EXPECT_EQ(outer_hash_join.GetName(), "OuterHashJoin");
}

// NOLINTNEXTLINE
TEST(OperatorTests, InsertTest) {
  //===--------------------------------------------------------------------===//
  // Insert
  //===--------------------------------------------------------------------===//
  auto columns = new std::vector<catalog::col_oid_t>;
  auto values = new std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>>;
  Operator insert =
      Insert::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), columns, values);

  EXPECT_EQ(insert.GetType(), OpType::INSERT);
  EXPECT_EQ(insert.GetName(), "Insert");

  delete columns;
  delete values;
}

// NOLINTNEXTLINE
TEST(OperatorTests, InsertSelectTest) {
  //===--------------------------------------------------------------------===//
  // InsertSelect
  //===--------------------------------------------------------------------===//
  Operator insert_select =
      InsertSelect::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3));

  EXPECT_EQ(insert_select.GetType(), OpType::INSERTSELECT);
  EXPECT_EQ(insert_select.GetName(), "InsertSelect");
}

// NOLINTNEXTLINE
TEST(OperatorTests, DeleteTest) {
  //===--------------------------------------------------------------------===//
  // Delete
  //===--------------------------------------------------------------------===//
  Operator del = Delete::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), nullptr);

  EXPECT_EQ(del.GetType(), OpType::DELETE);
  EXPECT_EQ(del.GetName(), "Delete");
}

// NOLINTNEXTLINE
TEST(OperatorTests, ExportExternalFileTest) {
  //===--------------------------------------------------------------------===//
  // ExportExternalFile
  //===--------------------------------------------------------------------===//
  Operator export_ext_file_1 = ExportExternalFile::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\'');
  Operator export_ext_file_2 = ExportExternalFile::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\'');
  Operator export_ext_file_3 = ExportExternalFile::make(parser::ExternalFileFormat::CSV, "file2.txt", ',', '"', '\'');

  EXPECT_EQ(export_ext_file_1.GetType(), OpType::EXPORTEXTERNALFILE);
  EXPECT_EQ(export_ext_file_1.GetName(), "ExportExternalFile");
  EXPECT_TRUE(export_ext_file_1 == export_ext_file_2);
  EXPECT_FALSE(export_ext_file_1 == export_ext_file_3);
}

// NOLINTNEXTLINE
TEST(OperatorTests, UpdateTest) {
  //===--------------------------------------------------------------------===//
  // Update
  //===--------------------------------------------------------------------===//
  auto updates = new std::vector<std::unique_ptr<parser::UpdateClause>>;
  Operator update = Update::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), updates);

  EXPECT_EQ(update.GetType(), OpType::UPDATE);
  EXPECT_EQ(update.GetName(), "Update");

  delete updates;
}

// NOLINTNEXTLINE
TEST(OperatorTests, HashGroupByTest) {
  //===--------------------------------------------------------------------===//
  // HashGroupBy
  //===--------------------------------------------------------------------===//
  Operator hash_group_by_1 =
      HashGroupBy::make(std::vector<std::shared_ptr<parser::AbstractExpression>>(), std::vector<AnnotatedExpression>());
  Operator hash_group_by_2 =
      HashGroupBy::make(std::vector<std::shared_ptr<parser::AbstractExpression>>(), std::vector<AnnotatedExpression>());
  auto expr_hash_group_by =
      std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetTinyInt(1));
  Operator hash_group_by_3 = HashGroupBy::make(
      std::vector<std::shared_ptr<parser::AbstractExpression>>{expr_hash_group_by}, std::vector<AnnotatedExpression>());

  EXPECT_EQ(hash_group_by_1.GetType(), OpType::HASHGROUPBY);
  EXPECT_EQ(hash_group_by_2.GetName(), "HashGroupBy");
  EXPECT_TRUE(hash_group_by_1 == hash_group_by_2);
  EXPECT_FALSE(hash_group_by_1 == hash_group_by_3);
}

// NOLINTNEXTLINE
TEST(OperatorTests, SortGroupByTest) {
  //===--------------------------------------------------------------------===//
  // SortGroupBy
  //===--------------------------------------------------------------------===//
  Operator sort_group_by_1 =
      SortGroupBy::make(std::vector<std::shared_ptr<parser::AbstractExpression>>(), std::vector<AnnotatedExpression>());
  Operator sort_group_by_2 =
      SortGroupBy::make(std::vector<std::shared_ptr<parser::AbstractExpression>>(), std::vector<AnnotatedExpression>());
  auto expr_sort_group_by =
      std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetTinyInt(1));
  Operator sort_group_by_3 = SortGroupBy::make(
      std::vector<std::shared_ptr<parser::AbstractExpression>>{expr_sort_group_by}, std::vector<AnnotatedExpression>());

  EXPECT_EQ(sort_group_by_1.GetType(), OpType::SORTGROUPBY);
  EXPECT_EQ(sort_group_by_1.GetName(), "SortGroupBy");
  EXPECT_TRUE(sort_group_by_1 == sort_group_by_2);
  EXPECT_FALSE(sort_group_by_1 == sort_group_by_3);
}

// NOLINTNEXTLINE
TEST(OperatorTests, AggregateTest) {
  //===--------------------------------------------------------------------===//
  // Aggregate
  //===--------------------------------------------------------------------===//
  Operator aggr = Aggregate::make();

  EXPECT_EQ(aggr.GetType(), OpType::AGGREGATE);
  EXPECT_EQ(aggr.GetName(), "Aggregate");
}

// NOLINTNEXTLINE
TEST(OperatorTests, DistinctTest) {
  //===--------------------------------------------------------------------===//
  // Distinct
  //===--------------------------------------------------------------------===//
  Operator distinct = Distinct::make();

  EXPECT_EQ(distinct.GetType(), OpType::DISTINCT);
  EXPECT_EQ(distinct.GetName(), "Distinct");
}

}  // namespace terrier::optimizer
