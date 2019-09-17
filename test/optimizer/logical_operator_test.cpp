#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "optimizer/logical_operators.h"
#include "optimizer/operator_expression.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/update_statement.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"

#include "gtest/gtest.h"

namespace terrier::optimizer {

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalInsertTest) {
  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);
  catalog::col_oid_t columns[] = {catalog::col_oid_t(1), catalog::col_oid_t(2)};
  parser::AbstractExpression *raw_values[] = {
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1)),
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(9))};
  std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> values = {
      std::vector<common::ManagedPointer<parser::AbstractExpression>>(raw_values, std::end(raw_values))};

  // Check that all of our GET methods work as expected
  Operator op1 = LogicalInsert::Make(
      database_oid, namespace_oid, table_oid, std::vector<catalog::col_oid_t>(columns, std::end(columns)),
      std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>(values));
  EXPECT_EQ(op1.GetType(), OpType::LOGICALINSERT);
  EXPECT_EQ(op1.As<LogicalInsert>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.As<LogicalInsert>()->GetNamespaceOid(), namespace_oid);
  EXPECT_EQ(op1.As<LogicalInsert>()->GetTableOid(), table_oid);
  EXPECT_EQ(op1.As<LogicalInsert>()->GetColumns(),
            (std::vector<catalog::col_oid_t>{catalog::col_oid_t(1), catalog::col_oid_t(2)}));
  EXPECT_EQ(op1.As<LogicalInsert>()->GetValues(), values);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 = LogicalInsert::Make(
      database_oid, namespace_oid, table_oid, std::vector<catalog::col_oid_t>(columns, std::end(columns)),
      std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>(values));
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // For this last check, we are going to give it more rows to insert
  // This will make sure that our hash is going deep into the vectors
  std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> other_values = {
      std::vector<common::ManagedPointer<parser::AbstractExpression>>(raw_values, std::end(raw_values)),
      std::vector<common::ManagedPointer<parser::AbstractExpression>>(raw_values, std::end(raw_values))};
  Operator op3 = LogicalInsert::Make(
      database_oid, namespace_oid, table_oid, std::vector<catalog::col_oid_t>(columns, std::end(columns)),
      std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>(other_values));
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  // Make sure that we catch when the insert values do not match the
  // number of columns that we are trying to insert into
  // NOTE: We only do this for debug builds
#ifndef NDEBUG
  parser::AbstractExpression *bad_raw_values[] = {
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1)),
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(2)),
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(3))};
  std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> bad_values = {
      std::vector<common::ManagedPointer<parser::AbstractExpression>>(bad_raw_values, std::end(bad_raw_values))};
  EXPECT_DEATH(LogicalInsert::Make(
                   database_oid, namespace_oid, table_oid, std::vector<catalog::col_oid_t>(columns, std::end(columns)),
                   std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>(bad_values)),
               "Mismatched");
  for (auto entry : bad_raw_values) delete entry;
#endif

  for (auto entry : raw_values) delete entry;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalInsertSelectTest) {
  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);

  // Check that all of our GET methods work as expected
  Operator op1 = LogicalInsertSelect::Make(database_oid, namespace_oid, table_oid);
  EXPECT_EQ(op1.GetType(), OpType::LOGICALINSERTSELECT);
  EXPECT_EQ(op1.As<LogicalInsertSelect>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.As<LogicalInsertSelect>()->GetNamespaceOid(), namespace_oid);
  EXPECT_EQ(op1.As<LogicalInsertSelect>()->GetTableOid(), table_oid);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 = LogicalInsertSelect::Make(database_oid, namespace_oid, table_oid);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  catalog::db_oid_t other_database_oid(999);
  Operator op3 = LogicalInsertSelect::Make(other_database_oid, namespace_oid, table_oid);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalDistinctTest) {
  // DISTINCT operator does not have any data members.
  // So we just need to make sure that all instantiations
  // of the object are equivalent.
  Operator op1 = LogicalDistinct::Make();
  EXPECT_EQ(op1.GetType(), OpType::LOGICALDISTINCT);

  Operator op2 = LogicalDistinct::Make();
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalLimitTest) {
  size_t offset = 90;
  size_t limit = 22;
  auto sort_expr_ori = new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  auto sort_expr = common::ManagedPointer<parser::AbstractExpression>(sort_expr_ori);
  planner::OrderByOrderingType sort_dir = planner::OrderByOrderingType::ASC;

  // Check that all of our GET methods work as expected
  Operator op1 = LogicalLimit::Make(offset, limit, {sort_expr}, {sort_dir});
  EXPECT_EQ(op1.GetType(), OpType::LOGICALLIMIT);
  EXPECT_EQ(op1.As<LogicalLimit>()->GetOffset(), offset);
  EXPECT_EQ(op1.As<LogicalLimit>()->GetLimit(), limit);
  EXPECT_EQ(op1.As<LogicalLimit>()->GetSortExpressions().size(), 1);
  EXPECT_EQ(op1.As<LogicalLimit>()->GetSortExpressions()[0], sort_expr);
  EXPECT_EQ(op1.As<LogicalLimit>()->GetSortDirections().size(), 1);
  EXPECT_EQ(op1.As<LogicalLimit>()->GetSortDirections()[0], sort_dir);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 = LogicalLimit::Make(offset, limit, {sort_expr}, {sort_dir});
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  size_t other_offset = 1111;
  Operator op3 = LogicalLimit::Make(other_offset, limit, {sort_expr}, {sort_dir});
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  delete sort_expr_ori;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalDeleteTest) {
  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);

  // Check that all of our GET methods work as expected
  Operator op1 = LogicalDelete::Make(database_oid, namespace_oid, table_oid);
  EXPECT_EQ(op1.GetType(), OpType::LOGICALDELETE);
  EXPECT_EQ(op1.As<LogicalDelete>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.As<LogicalDelete>()->GetNamespaceOid(), namespace_oid);
  EXPECT_EQ(op1.As<LogicalDelete>()->GetTableOid(), table_oid);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 = LogicalDelete::Make(database_oid, namespace_oid, table_oid);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  catalog::db_oid_t other_database_oid(999);
  Operator op3 = LogicalDelete::Make(other_database_oid, namespace_oid, table_oid);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalUpdateTest) {
  std::string column = "abc";
  parser::AbstractExpression *value = new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  auto raw_update_clause = new parser::UpdateClause(column, common::ManagedPointer<parser::AbstractExpression>(value));
  auto update_clause = common::ManagedPointer(raw_update_clause);
  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);

  // Check that all of our GET methods work as expected
  Operator op1 = LogicalUpdate::Make(database_oid, namespace_oid, table_oid, {update_clause});
  EXPECT_EQ(op1.GetType(), OpType::LOGICALUPDATE);
  EXPECT_EQ(op1.As<LogicalUpdate>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.As<LogicalUpdate>()->GetNamespaceOid(), namespace_oid);
  EXPECT_EQ(op1.As<LogicalUpdate>()->GetTableOid(), table_oid);
  EXPECT_EQ(op1.As<LogicalUpdate>()->GetUpdateClauses().size(), 1);
  EXPECT_EQ(op1.As<LogicalUpdate>()->GetUpdateClauses()[0], update_clause);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 = LogicalUpdate::Make(database_oid, namespace_oid, table_oid, {update_clause});
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  Operator op3 = LogicalUpdate::Make(database_oid, namespace_oid, table_oid, {});
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  delete value;
  delete raw_update_clause;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalExportExternalFileTest) {
  std::string file_name = "fakefile.txt";
  char delimiter = 'X';
  char quote = 'Y';
  char escape = 'Z';

  // Check that all of our GET methods work as expected
  Operator op1 =
      LogicalExportExternalFile::Make(parser::ExternalFileFormat::BINARY, file_name, delimiter, quote, escape);
  EXPECT_EQ(op1.GetType(), OpType::LOGICALEXPORTEXTERNALFILE);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetFormat(), parser::ExternalFileFormat::BINARY);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetFilename(), file_name);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetDelimiter(), delimiter);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetQuote(), quote);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetEscape(), escape);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  std::string file_name_copy = file_name;  // NOLINT
  Operator op2 =

      LogicalExportExternalFile::Make(parser::ExternalFileFormat::BINARY, file_name_copy, delimiter, quote, escape);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  Operator op3 = LogicalExportExternalFile::Make(parser::ExternalFileFormat::CSV, file_name, delimiter, quote, escape);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalGetTest) {
  //===--------------------------------------------------------------------===//
  // LogicalGet
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_get_01 = LogicalGet::Make(catalog::db_oid_t(2), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                             std::vector<AnnotatedExpression>(), "table", false);
  Operator logical_get_02 = LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(3), catalog::table_oid_t(3),
                                             std::vector<AnnotatedExpression>(), "table", false);
  Operator logical_get_03 = LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(4),
                                             std::vector<AnnotatedExpression>(), "table", false);
  Operator logical_get_04 = LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                             std::vector<AnnotatedExpression>(), "tableTable", false);
  Operator logical_get_05 = LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                             std::vector<AnnotatedExpression>(), "table", true);
  Operator logical_get_1 = LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>(), "table", false);
  Operator logical_get_2 = LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>(), "table", false);
  Operator logical_get_3 = LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>{annotated_expr_0}, "table", false);
  Operator logical_get_4 = LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>{annotated_expr_1}, "table", false);
  Operator logical_get_5 = LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>{annotated_expr_2}, "table", false);
  Operator logical_get_6 = LogicalGet::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>{annotated_expr_3}, "table", false);

  EXPECT_EQ(logical_get_1.GetType(), OpType::LOGICALGET);
  EXPECT_EQ(logical_get_1.As<LogicalGet>()->GetDatabaseOID(), catalog::db_oid_t(1));
  EXPECT_EQ(logical_get_1.As<LogicalGet>()->GetNamespaceOID(), catalog::namespace_oid_t(2));
  EXPECT_EQ(logical_get_1.As<LogicalGet>()->GetTableOID(), catalog::table_oid_t(3));
  EXPECT_EQ(logical_get_1.As<LogicalGet>()->GetPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_get_3.As<LogicalGet>()->GetPredicates(), std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(logical_get_4.As<LogicalGet>()->GetPredicates(), std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_EQ(logical_get_1.As<LogicalGet>()->GetTableAlias(), "table");
  EXPECT_EQ(logical_get_1.As<LogicalGet>()->GetIsForUpdate(), false);
  EXPECT_EQ(logical_get_1.GetName(), "LogicalGet");
  EXPECT_TRUE(logical_get_1 == logical_get_2);
  EXPECT_FALSE(logical_get_1 == logical_get_3);
  EXPECT_FALSE(logical_get_1 == logical_get_01);
  EXPECT_FALSE(logical_get_1 == logical_get_02);
  EXPECT_FALSE(logical_get_1 == logical_get_03);
  EXPECT_FALSE(logical_get_1 == logical_get_04);
  EXPECT_FALSE(logical_get_1 == logical_get_05);
  EXPECT_FALSE(logical_get_1 == logical_get_4);
  EXPECT_FALSE(logical_get_4 == logical_get_5);
  EXPECT_FALSE(logical_get_1 == logical_get_6);
  EXPECT_EQ(logical_get_1.Hash(), logical_get_2.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_3.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_01.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_02.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_03.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_04.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_05.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_4.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_5.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_6.Hash());
  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalExternalFileGetTest) {
  //===--------------------------------------------------------------------===//
  // LogicalExternalFileGet
  //===--------------------------------------------------------------------===//
  Operator logical_ext_file_get_1 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\');
  Operator logical_ext_file_get_2 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\');
  Operator logical_ext_file_get_3 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file2.txt", ',', '"', '\\');
  Operator logical_ext_file_get_4 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::BINARY, "file.txt", ',', '"', '\\');
  Operator logical_ext_file_get_5 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ' ', '"', '\\');
  Operator logical_ext_file_get_6 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '\'', '\\');
  Operator logical_ext_file_get_7 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '&');

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
  EXPECT_EQ(logical_ext_file_get_1.Hash(), logical_ext_file_get_2.Hash());
  EXPECT_NE(logical_ext_file_get_1.Hash(), logical_ext_file_get_3.Hash());
  EXPECT_NE(logical_ext_file_get_1.Hash(), logical_ext_file_get_4.Hash());
  EXPECT_NE(logical_ext_file_get_1.Hash(), logical_ext_file_get_5.Hash());
  EXPECT_NE(logical_ext_file_get_1.Hash(), logical_ext_file_get_6.Hash());
  EXPECT_NE(logical_ext_file_get_1.Hash(), logical_ext_file_get_7.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalQueryDerivedGetTest) {
  //===--------------------------------------------------------------------===//
  // LogicalQueryDerivedGet
  //===--------------------------------------------------------------------===//
  auto alias_to_expr_map_1 = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();
  auto alias_to_expr_map_1_1 = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();
  auto alias_to_expr_map_2 = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();
  auto alias_to_expr_map_3 = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();
  auto alias_to_expr_map_4 = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();
  auto alias_to_expr_map_5 = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  auto expr1 = common::ManagedPointer(expr_b_1);
  auto expr2 = common::ManagedPointer(expr_b_2);

  alias_to_expr_map_1["constant expr"] = expr1;
  alias_to_expr_map_1_1["constant expr"] = expr1;
  alias_to_expr_map_2["constant expr"] = expr1;
  alias_to_expr_map_3["constant expr"] = expr2;
  alias_to_expr_map_4["constant expr2"] = expr1;
  alias_to_expr_map_5["constant expr"] = expr1;
  alias_to_expr_map_5["constant expr2"] = expr2;

  Operator logical_query_derived_get_1 = LogicalQueryDerivedGet::Make("alias", std::move(alias_to_expr_map_1));
  Operator logical_query_derived_get_2 = LogicalQueryDerivedGet::Make("alias", std::move(alias_to_expr_map_2));
  Operator logical_query_derived_get_3 = LogicalQueryDerivedGet::Make(
      "alias", std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>());
  Operator logical_query_derived_get_4 = LogicalQueryDerivedGet::Make("alias", std::move(alias_to_expr_map_3));
  Operator logical_query_derived_get_5 = LogicalQueryDerivedGet::Make("alias", std::move(alias_to_expr_map_4));
  Operator logical_query_derived_get_6 = LogicalQueryDerivedGet::Make("alias", std::move(alias_to_expr_map_5));

  EXPECT_EQ(logical_query_derived_get_1.GetType(), OpType::LOGICALQUERYDERIVEDGET);
  EXPECT_EQ(logical_query_derived_get_1.GetName(), "LogicalQueryDerivedGet");
  EXPECT_EQ(logical_query_derived_get_1.As<LogicalQueryDerivedGet>()->GetTableAlias(), "alias");
  EXPECT_EQ(logical_query_derived_get_1.As<LogicalQueryDerivedGet>()->GetAliasToExprMap(), alias_to_expr_map_1_1);
  EXPECT_TRUE(logical_query_derived_get_1 == logical_query_derived_get_2);
  EXPECT_FALSE(logical_query_derived_get_1 == logical_query_derived_get_3);
  EXPECT_FALSE(logical_query_derived_get_1 == logical_query_derived_get_4);
  EXPECT_FALSE(logical_query_derived_get_1 == logical_query_derived_get_5);
  EXPECT_FALSE(logical_query_derived_get_1 == logical_query_derived_get_6);
  EXPECT_EQ(logical_query_derived_get_1.Hash(), logical_query_derived_get_2.Hash());
  EXPECT_NE(logical_query_derived_get_1.Hash(), logical_query_derived_get_3.Hash());
  EXPECT_NE(logical_query_derived_get_1.Hash(), logical_query_derived_get_4.Hash());
  EXPECT_NE(logical_query_derived_get_1.Hash(), logical_query_derived_get_5.Hash());
  EXPECT_NE(logical_query_derived_get_1.Hash(), logical_query_derived_get_6.Hash());

  delete expr_b_1;
  delete expr_b_2;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalFilterTest) {
  //===--------------------------------------------------------------------===//
  // LogicalFilter
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_filter_1 = LogicalFilter::Make(std::vector<AnnotatedExpression>());
  Operator logical_filter_2 = LogicalFilter::Make(std::vector<AnnotatedExpression>());
  Operator logical_filter_3 = LogicalFilter::Make(std::vector<AnnotatedExpression>{annotated_expr_0});
  Operator logical_filter_4 = LogicalFilter::Make(std::vector<AnnotatedExpression>{annotated_expr_1});
  Operator logical_filter_5 = LogicalFilter::Make(std::vector<AnnotatedExpression>{annotated_expr_2});
  Operator logical_filter_6 = LogicalFilter::Make(std::vector<AnnotatedExpression>{annotated_expr_3});

  EXPECT_EQ(logical_filter_1.GetType(), OpType::LOGICALFILTER);
  EXPECT_EQ(logical_filter_3.GetType(), OpType::LOGICALFILTER);
  EXPECT_EQ(logical_filter_1.GetName(), "LogicalFilter");
  EXPECT_EQ(logical_filter_1.As<LogicalFilter>()->GetPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_filter_3.As<LogicalFilter>()->GetPredicates(), std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(logical_filter_4.As<LogicalFilter>()->GetPredicates(), std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_TRUE(logical_filter_1 == logical_filter_2);
  EXPECT_FALSE(logical_filter_1 == logical_filter_3);
  EXPECT_TRUE(logical_filter_4 == logical_filter_5);
  EXPECT_FALSE(logical_filter_4 == logical_filter_6);
  EXPECT_EQ(logical_filter_1.Hash(), logical_filter_2.Hash());
  EXPECT_NE(logical_filter_1.Hash(), logical_filter_3.Hash());
  EXPECT_NE(logical_filter_4.Hash(), logical_filter_6.Hash());
  EXPECT_EQ(logical_filter_4.Hash(), logical_filter_5.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalProjectionTest) {
  //===--------------------------------------------------------------------===//
  // LogicalProjection
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  Operator logical_projection_1 =
      LogicalProjection::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1});
  Operator logical_projection_2 =
      LogicalProjection::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_2});
  Operator logical_projection_3 =
      LogicalProjection::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3});

  EXPECT_EQ(logical_projection_1.GetType(), OpType::LOGICALPROJECTION);
  EXPECT_EQ(logical_projection_3.GetType(), OpType::LOGICALPROJECTION);
  EXPECT_EQ(logical_projection_1.GetName(), "LogicalProjection");
  EXPECT_EQ(logical_projection_1.As<LogicalProjection>()->GetExpressions(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1});
  EXPECT_EQ(logical_projection_3.As<LogicalProjection>()->GetExpressions(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3});
  EXPECT_TRUE(logical_projection_1 == logical_projection_2);
  EXPECT_FALSE(logical_projection_1 == logical_projection_3);
  EXPECT_EQ(logical_projection_1.Hash(), logical_projection_2.Hash());
  EXPECT_NE(logical_projection_1.Hash(), logical_projection_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalDependentJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalDependentJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_dep_join_0 = LogicalDependentJoin::Make();
  Operator logical_dep_join_1 = LogicalDependentJoin::Make(std::vector<AnnotatedExpression>());
  Operator logical_dep_join_2 = LogicalDependentJoin::Make(std::vector<AnnotatedExpression>());
  Operator logical_dep_join_3 = LogicalDependentJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_0});
  Operator logical_dep_join_4 = LogicalDependentJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1});
  Operator logical_dep_join_5 = LogicalDependentJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_2});
  Operator logical_dep_join_6 = LogicalDependentJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_3});

  EXPECT_EQ(logical_dep_join_1.GetType(), OpType::LOGICALDEPENDENTJOIN);
  EXPECT_EQ(logical_dep_join_3.GetType(), OpType::LOGICALDEPENDENTJOIN);
  EXPECT_EQ(logical_dep_join_1.GetName(), "LogicalDependentJoin");
  EXPECT_EQ(logical_dep_join_1.As<LogicalDependentJoin>()->GetJoinPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_dep_join_3.As<LogicalDependentJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(logical_dep_join_4.As<LogicalDependentJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_TRUE(logical_dep_join_0 == logical_dep_join_1);
  EXPECT_TRUE(logical_dep_join_1 == logical_dep_join_2);
  EXPECT_FALSE(logical_dep_join_1 == logical_dep_join_3);
  EXPECT_TRUE(logical_dep_join_4 == logical_dep_join_5);
  EXPECT_FALSE(logical_dep_join_4 == logical_dep_join_6);
  EXPECT_EQ(logical_dep_join_1.Hash(), logical_dep_join_0.Hash());
  EXPECT_EQ(logical_dep_join_1.Hash(), logical_dep_join_2.Hash());
  EXPECT_NE(logical_dep_join_1.Hash(), logical_dep_join_3.Hash());
  EXPECT_NE(logical_dep_join_4.Hash(), logical_dep_join_6.Hash());
  EXPECT_EQ(logical_dep_join_4.Hash(), logical_dep_join_5.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalMarkJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalMarkJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_mark_join_0 = LogicalMarkJoin::Make();
  Operator logical_mark_join_1 = LogicalMarkJoin::Make(std::vector<AnnotatedExpression>());
  Operator logical_mark_join_2 = LogicalMarkJoin::Make(std::vector<AnnotatedExpression>());
  Operator logical_mark_join_3 = LogicalMarkJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_0});
  Operator logical_mark_join_4 = LogicalMarkJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1});
  Operator logical_mark_join_5 = LogicalMarkJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_2});
  Operator logical_mark_join_6 = LogicalMarkJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_3});

  EXPECT_EQ(logical_mark_join_1.GetType(), OpType::LOGICALMARKJOIN);
  EXPECT_EQ(logical_mark_join_3.GetType(), OpType::LOGICALMARKJOIN);
  EXPECT_EQ(logical_mark_join_1.GetName(), "LogicalMarkJoin");
  EXPECT_EQ(logical_mark_join_1.As<LogicalMarkJoin>()->GetJoinPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_mark_join_3.As<LogicalMarkJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(logical_mark_join_4.As<LogicalMarkJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_TRUE(logical_mark_join_0 == logical_mark_join_1);
  EXPECT_TRUE(logical_mark_join_1 == logical_mark_join_2);
  EXPECT_FALSE(logical_mark_join_1 == logical_mark_join_3);
  EXPECT_TRUE(logical_mark_join_4 == logical_mark_join_5);
  EXPECT_FALSE(logical_mark_join_4 == logical_mark_join_6);
  EXPECT_EQ(logical_mark_join_1.Hash(), logical_mark_join_0.Hash());
  EXPECT_EQ(logical_mark_join_1.Hash(), logical_mark_join_2.Hash());
  EXPECT_NE(logical_mark_join_1.Hash(), logical_mark_join_3.Hash());
  EXPECT_NE(logical_mark_join_4.Hash(), logical_mark_join_6.Hash());
  EXPECT_EQ(logical_mark_join_4.Hash(), logical_mark_join_5.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalSingleJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalSingleJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_single_join_0 = LogicalSingleJoin::Make();
  Operator logical_single_join_1 = LogicalSingleJoin::Make(std::vector<AnnotatedExpression>());
  Operator logical_single_join_2 = LogicalSingleJoin::Make(std::vector<AnnotatedExpression>());
  Operator logical_single_join_3 = LogicalSingleJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_0});
  Operator logical_single_join_4 = LogicalSingleJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1});
  Operator logical_single_join_5 = LogicalSingleJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_2});
  Operator logical_single_join_6 = LogicalSingleJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_3});

  EXPECT_EQ(logical_single_join_1.GetType(), OpType::LOGICALSINGLEJOIN);
  EXPECT_EQ(logical_single_join_3.GetType(), OpType::LOGICALSINGLEJOIN);
  EXPECT_EQ(logical_single_join_1.GetName(), "LogicalSingleJoin");
  EXPECT_EQ(logical_single_join_1.As<LogicalSingleJoin>()->GetJoinPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_single_join_3.As<LogicalSingleJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(logical_single_join_4.As<LogicalSingleJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_TRUE(logical_single_join_0 == logical_single_join_1);
  EXPECT_TRUE(logical_single_join_1 == logical_single_join_2);
  EXPECT_FALSE(logical_single_join_1 == logical_single_join_3);
  EXPECT_TRUE(logical_single_join_4 == logical_single_join_5);
  EXPECT_FALSE(logical_single_join_4 == logical_single_join_6);
  EXPECT_EQ(logical_single_join_1.Hash(), logical_single_join_0.Hash());
  EXPECT_EQ(logical_single_join_1.Hash(), logical_single_join_2.Hash());
  EXPECT_NE(logical_single_join_1.Hash(), logical_single_join_3.Hash());
  EXPECT_NE(logical_single_join_4.Hash(), logical_single_join_6.Hash());
  EXPECT_EQ(logical_single_join_4.Hash(), logical_single_join_5.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalInnerJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalInnerJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_inner_join_0 = LogicalInnerJoin::Make();
  Operator logical_inner_join_1 = LogicalInnerJoin::Make(std::vector<AnnotatedExpression>());
  Operator logical_inner_join_2 = LogicalInnerJoin::Make(std::vector<AnnotatedExpression>());
  Operator logical_inner_join_3 = LogicalInnerJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_0});
  Operator logical_inner_join_4 = LogicalInnerJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1});
  Operator logical_inner_join_5 = LogicalInnerJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_2});
  Operator logical_inner_join_6 = LogicalInnerJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_3});

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
  EXPECT_EQ(logical_inner_join_1.Hash(), logical_inner_join_0.Hash());
  EXPECT_EQ(logical_inner_join_1.Hash(), logical_inner_join_2.Hash());
  EXPECT_NE(logical_inner_join_1.Hash(), logical_inner_join_3.Hash());
  EXPECT_NE(logical_inner_join_4.Hash(), logical_inner_join_6.Hash());
  EXPECT_EQ(logical_inner_join_4.Hash(), logical_inner_join_5.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalLeftJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalLeftJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  Operator logical_left_join_1 = LogicalLeftJoin::Make(x_1);
  Operator logical_left_join_2 = LogicalLeftJoin::Make(x_2);
  Operator logical_left_join_3 = LogicalLeftJoin::Make(x_3);

  EXPECT_EQ(logical_left_join_1.GetType(), OpType::LOGICALLEFTJOIN);
  EXPECT_EQ(logical_left_join_3.GetType(), OpType::LOGICALLEFTJOIN);
  EXPECT_EQ(logical_left_join_1.GetName(), "LogicalLeftJoin");
  EXPECT_EQ(*(logical_left_join_1.As<LogicalLeftJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(logical_left_join_2.As<LogicalLeftJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(logical_left_join_3.As<LogicalLeftJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(logical_left_join_1 == logical_left_join_2);
  EXPECT_FALSE(logical_left_join_1 == logical_left_join_3);
  EXPECT_EQ(logical_left_join_1.Hash(), logical_left_join_2.Hash());
  EXPECT_NE(logical_left_join_1.Hash(), logical_left_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalRightJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalRightJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  Operator logical_right_join_1 = LogicalRightJoin::Make(x_1);
  Operator logical_right_join_2 = LogicalRightJoin::Make(x_2);
  Operator logical_right_join_3 = LogicalRightJoin::Make(x_3);

  EXPECT_EQ(logical_right_join_1.GetType(), OpType::LOGICALRIGHTJOIN);
  EXPECT_EQ(logical_right_join_3.GetType(), OpType::LOGICALRIGHTJOIN);
  EXPECT_EQ(logical_right_join_1.GetName(), "LogicalRightJoin");
  EXPECT_EQ(*(logical_right_join_1.As<LogicalRightJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(logical_right_join_2.As<LogicalRightJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(logical_right_join_3.As<LogicalRightJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(logical_right_join_1 == logical_right_join_2);
  EXPECT_FALSE(logical_right_join_1 == logical_right_join_3);
  EXPECT_EQ(logical_right_join_1.Hash(), logical_right_join_2.Hash());
  EXPECT_NE(logical_right_join_1.Hash(), logical_right_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalOuterJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalOuterJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  Operator logical_outer_join_1 = LogicalOuterJoin::Make(x_1);
  Operator logical_outer_join_2 = LogicalOuterJoin::Make(x_2);
  Operator logical_outer_join_3 = LogicalOuterJoin::Make(x_3);

  EXPECT_EQ(logical_outer_join_1.GetType(), OpType::LOGICALOUTERJOIN);
  EXPECT_EQ(logical_outer_join_3.GetType(), OpType::LOGICALOUTERJOIN);
  EXPECT_EQ(logical_outer_join_1.GetName(), "LogicalOuterJoin");
  EXPECT_EQ(*(logical_outer_join_1.As<LogicalOuterJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(logical_outer_join_2.As<LogicalOuterJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(logical_outer_join_3.As<LogicalOuterJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(logical_outer_join_1 == logical_outer_join_2);
  EXPECT_FALSE(logical_outer_join_1 == logical_outer_join_3);
  EXPECT_EQ(logical_outer_join_1.Hash(), logical_outer_join_2.Hash());
  EXPECT_NE(logical_outer_join_1.Hash(), logical_outer_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalSemiJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalSemiJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  Operator logical_semi_join_1 = LogicalSemiJoin::Make(x_1);
  Operator logical_semi_join_2 = LogicalSemiJoin::Make(x_2);
  Operator logical_semi_join_3 = LogicalSemiJoin::Make(x_3);

  EXPECT_EQ(logical_semi_join_1.GetType(), OpType::LOGICALSEMIJOIN);
  EXPECT_EQ(logical_semi_join_3.GetType(), OpType::LOGICALSEMIJOIN);
  EXPECT_EQ(logical_semi_join_1.GetName(), "LogicalSemiJoin");
  EXPECT_EQ(*(logical_semi_join_1.As<LogicalSemiJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(logical_semi_join_2.As<LogicalSemiJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(logical_semi_join_3.As<LogicalSemiJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(logical_semi_join_1 == logical_semi_join_2);
  EXPECT_FALSE(logical_semi_join_1 == logical_semi_join_3);
  EXPECT_EQ(logical_semi_join_1.Hash(), logical_semi_join_2.Hash());
  EXPECT_NE(logical_semi_join_1.Hash(), logical_semi_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalAggregateAndGroupByTest) {
  //===--------------------------------------------------------------------===//
  // LogicalAggregateAndGroupBy
  //===--------------------------------------------------------------------===//
  // ConstValueExpression subclass AbstractExpression
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));
  auto expr_b_7 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  // columns: vector of shared_ptr of AbstractExpression
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);
  auto x_7 = common::ManagedPointer<parser::AbstractExpression>(expr_b_7);

  // ConstValueExpression subclass AbstractExpression
  auto expr_b_4 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_5 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_8 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_6 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));
  auto x_4 = common::ManagedPointer<parser::AbstractExpression>(expr_b_4);
  auto x_5 = common::ManagedPointer<parser::AbstractExpression>(expr_b_5);
  auto x_6 = common::ManagedPointer<parser::AbstractExpression>(expr_b_6);
  auto x_8 = common::ManagedPointer<parser::AbstractExpression>(expr_b_8);

  // havings: vector of AnnotatedExpression
  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_4, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_5, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_6, std::unordered_set<std::string>());
  auto annotated_expr_4 = AnnotatedExpression(x_8, std::unordered_set<std::string>());

  Operator logical_group_1_0 =
      LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1},
                                       std::vector<AnnotatedExpression>{annotated_expr_0});
  Operator logical_group_1_1 =
      LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1},
                                       std::vector<AnnotatedExpression>{annotated_expr_1});
  Operator logical_group_2_2 =
      LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_2},
                                       std::vector<AnnotatedExpression>{annotated_expr_2});
  Operator logical_group_3 =
      LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3});
  Operator logical_group_7_4 =
      LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_7},
                                       std::vector<AnnotatedExpression>{annotated_expr_4});

  EXPECT_EQ(logical_group_1_1.GetType(), OpType::LOGICALAGGREGATEANDGROUPBY);
  EXPECT_EQ(logical_group_3.GetType(), OpType::LOGICALAGGREGATEANDGROUPBY);
  EXPECT_EQ(logical_group_7_4.GetName(), "LogicalAggregateAndGroupBy");
  EXPECT_EQ(logical_group_1_1.As<LogicalAggregateAndGroupBy>()->GetColumns(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1});
  EXPECT_EQ(logical_group_3.As<LogicalAggregateAndGroupBy>()->GetColumns(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3});
  EXPECT_EQ(logical_group_1_1.As<LogicalAggregateAndGroupBy>()->GetHaving(),
            std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_EQ(logical_group_7_4.As<LogicalAggregateAndGroupBy>()->GetHaving(),
            std::vector<AnnotatedExpression>{annotated_expr_4});
  EXPECT_TRUE(logical_group_1_1 == logical_group_2_2);
  EXPECT_FALSE(logical_group_1_1 == logical_group_7_4);
  EXPECT_FALSE(logical_group_1_0 == logical_group_1_1);
  EXPECT_FALSE(logical_group_3 == logical_group_7_4);

  EXPECT_EQ(logical_group_1_1.Hash(), logical_group_2_2.Hash());
  EXPECT_NE(logical_group_1_1.Hash(), logical_group_7_4.Hash());
  EXPECT_NE(logical_group_1_0.Hash(), logical_group_1_1.Hash());
  EXPECT_NE(logical_group_3.Hash(), logical_group_7_4.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
  delete expr_b_4;
  delete expr_b_5;
  delete expr_b_6;
  delete expr_b_7;
  delete expr_b_8;
}

}  // namespace terrier::optimizer
