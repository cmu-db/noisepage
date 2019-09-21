#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "optimizer/operator_expression.h"
#include "optimizer/physical_operators.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/update_statement.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"

#include "gtest/gtest.h"

namespace terrier::optimizer {

// NOLINTNEXTLINE
TEST(OperatorTests, TableFreeScanTest) {
  //===--------------------------------------------------------------------===//
  // TableFreeScan
  //===--------------------------------------------------------------------===//
  // TableFreeScan operator does not have any data members.
  // So we just need to make sure that all instantiations
  // of the object are equivalent.
  Operator op1 = TableFreeScan::Make();
  EXPECT_EQ(op1.GetType(), OpType::TABLEFREESCAN);

  Operator op2 = TableFreeScan::Make();
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, SeqScanTest) {
  //===--------------------------------------------------------------------===//
  // SeqScan
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

  Operator seq_scan_01 = SeqScan::Make(catalog::db_oid_t(2), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                       std::vector<AnnotatedExpression>(), "table", false);
  Operator seq_scan_02 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(3), catalog::table_oid_t(3),
                                       std::vector<AnnotatedExpression>(), "table", false);
  Operator seq_scan_03 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(4),
                                       std::vector<AnnotatedExpression>(), "table", false);
  Operator seq_scan_04 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                       std::vector<AnnotatedExpression>(), "tableTable", false);
  Operator seq_scan_05 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                       std::vector<AnnotatedExpression>(), "table", true);
  Operator seq_scan_1 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                      std::vector<AnnotatedExpression>(), "table", false);
  Operator seq_scan_2 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                      std::vector<AnnotatedExpression>(), "table", false);
  Operator seq_scan_3 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                      std::vector<AnnotatedExpression>{annotated_expr_0}, "table", false);
  Operator seq_scan_4 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                      std::vector<AnnotatedExpression>{annotated_expr_1}, "table", false);
  Operator seq_scan_5 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                      std::vector<AnnotatedExpression>{annotated_expr_2}, "table", false);
  Operator seq_scan_6 = SeqScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3),
                                      std::vector<AnnotatedExpression>{annotated_expr_3}, "table", false);

  EXPECT_EQ(seq_scan_1.GetType(), OpType::SEQSCAN);
  EXPECT_EQ(seq_scan_1.As<SeqScan>()->GetDatabaseOID(), catalog::db_oid_t(1));
  EXPECT_EQ(seq_scan_1.As<SeqScan>()->GetNamespaceOID(), catalog::namespace_oid_t(2));
  EXPECT_EQ(seq_scan_1.As<SeqScan>()->GetTableOID(), catalog::table_oid_t(3));
  EXPECT_EQ(seq_scan_1.As<SeqScan>()->GetPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(seq_scan_3.As<SeqScan>()->GetPredicates(), std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(seq_scan_4.As<SeqScan>()->GetPredicates(), std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_EQ(seq_scan_1.As<SeqScan>()->GetTableAlias(), "table");
  EXPECT_EQ(seq_scan_1.As<SeqScan>()->GetIsForUpdate(), false);
  EXPECT_EQ(seq_scan_1.GetName(), "SeqScan");
  EXPECT_TRUE(seq_scan_1 == seq_scan_2);
  EXPECT_FALSE(seq_scan_1 == seq_scan_3);
  EXPECT_FALSE(seq_scan_1 == seq_scan_01);
  EXPECT_FALSE(seq_scan_1 == seq_scan_02);
  EXPECT_FALSE(seq_scan_1 == seq_scan_03);
  EXPECT_FALSE(seq_scan_1 == seq_scan_04);
  EXPECT_FALSE(seq_scan_1 == seq_scan_05);
  EXPECT_FALSE(seq_scan_1 == seq_scan_4);
  EXPECT_FALSE(seq_scan_4 == seq_scan_5);
  EXPECT_FALSE(seq_scan_1 == seq_scan_6);
  EXPECT_EQ(seq_scan_1.Hash(), seq_scan_2.Hash());
  EXPECT_NE(seq_scan_1.Hash(), seq_scan_3.Hash());
  EXPECT_NE(seq_scan_1.Hash(), seq_scan_01.Hash());
  EXPECT_NE(seq_scan_1.Hash(), seq_scan_02.Hash());
  EXPECT_NE(seq_scan_1.Hash(), seq_scan_03.Hash());
  EXPECT_NE(seq_scan_1.Hash(), seq_scan_04.Hash());
  EXPECT_NE(seq_scan_1.Hash(), seq_scan_05.Hash());
  EXPECT_NE(seq_scan_1.Hash(), seq_scan_4.Hash());
  EXPECT_NE(seq_scan_1.Hash(), seq_scan_5.Hash());
  EXPECT_NE(seq_scan_1.Hash(), seq_scan_6.Hash());
  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, IndexScanTest) {
  //===--------------------------------------------------------------------===//
  // IndexScan
  //===--------------------------------------------------------------------===//
  // predicates
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

  // different from index_scan_1 in dbOID
  Operator index_scan_01 =
      IndexScan::Make(catalog::db_oid_t(2), catalog::namespace_oid_t(2), catalog::index_oid_t(3),
                      std::vector<AnnotatedExpression>(), "table", false, std::vector<catalog::col_oid_t>(),
                      std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  // different from index_scan_1 in namespace OID
  Operator index_scan_02 =
      IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(3), catalog::index_oid_t(3),
                      std::vector<AnnotatedExpression>(), "table", false, std::vector<catalog::col_oid_t>(),
                      std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  // different from index_scan_1 in index OID
  Operator index_scan_03 =
      IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(4),
                      std::vector<AnnotatedExpression>(), "table", false, std::vector<catalog::col_oid_t>(),
                      std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  // different from index_scan_1 in table alias
  Operator index_scan_04 =
      IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3),
                      std::vector<AnnotatedExpression>(), "tableTable", false, std::vector<catalog::col_oid_t>(),
                      std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  // different from index_scan_1 in 'is for update'
  Operator index_scan_05 =
      IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3),
                      std::vector<AnnotatedExpression>(), "table", true, std::vector<catalog::col_oid_t>(),
                      std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  // different from index_scan_1 in key column list
  std::vector<catalog::col_oid_t> key_column1 = {catalog::col_oid_t(1), catalog::col_oid_t(2)};
  std::vector<catalog::col_oid_t> key_column2 = {catalog::col_oid_t(1), catalog::col_oid_t(2)};
  Operator index_scan_06 = IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3),
                                           std::vector<AnnotatedExpression>(), "table", false, std::move(key_column1),
                                           std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  // different from index_scan_1 in expr type list
  std::vector<parser::ExpressionType> expr_type1 = {parser::ExpressionType::COMPARE_IN};
  std::vector<parser::ExpressionType> expr_type2 = {parser::ExpressionType::COMPARE_IN};
  Operator index_scan_07 = IndexScan::Make(
      catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3), std::vector<AnnotatedExpression>(),
      "table", false, std::vector<catalog::col_oid_t>(), std::move(expr_type1), std::vector<type::TransientValue>());
  // different from index_scan_1 in value list
  std::vector<type::TransientValue> value1;
  std::vector<type::TransientValue> value2;
  value1.push_back(type::TransientValueFactory::GetInteger(1));
  value2.push_back(type::TransientValueFactory::GetInteger(1));
  Operator index_scan_08 = IndexScan::Make(
      catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3), std::vector<AnnotatedExpression>(),
      "table", false, std::vector<catalog::col_oid_t>(), std::vector<parser::ExpressionType>(), std::move(value1));

  Operator index_scan_1 =
      IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3),
                      std::vector<AnnotatedExpression>(), "table", false, std::vector<catalog::col_oid_t>(),
                      std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  Operator index_scan_2 =
      IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3),
                      std::vector<AnnotatedExpression>(), "table", false, std::vector<catalog::col_oid_t>(),
                      std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  // different from index_scan_1 in predicates
  Operator index_scan_3 = IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3),
                                          std::vector<AnnotatedExpression>{annotated_expr_0}, "table", false,
                                          std::vector<catalog::col_oid_t>(), std::vector<parser::ExpressionType>(),
                                          std::vector<type::TransientValue>());
  Operator index_scan_4 = IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3),
                                          std::vector<AnnotatedExpression>{annotated_expr_1}, "table", false,
                                          std::vector<catalog::col_oid_t>(), std::vector<parser::ExpressionType>(),
                                          std::vector<type::TransientValue>());
  Operator index_scan_5 = IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3),
                                          std::vector<AnnotatedExpression>{annotated_expr_2}, "table", false,
                                          std::vector<catalog::col_oid_t>(), std::vector<parser::ExpressionType>(),
                                          std::vector<type::TransientValue>());
  Operator index_scan_6 = IndexScan::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::index_oid_t(3),
                                          std::vector<AnnotatedExpression>{annotated_expr_3}, "table", false,
                                          std::vector<catalog::col_oid_t>(), std::vector<parser::ExpressionType>(),
                                          std::vector<type::TransientValue>());

  EXPECT_EQ(index_scan_1.GetType(), OpType::INDEXSCAN);
  EXPECT_EQ(index_scan_1.As<IndexScan>()->GetDatabaseOID(), catalog::db_oid_t(1));
  EXPECT_EQ(index_scan_1.As<IndexScan>()->GetNamespaceOID(), catalog::namespace_oid_t(2));
  EXPECT_EQ(index_scan_1.As<IndexScan>()->GetIndexOID(), catalog::index_oid_t(3));
  EXPECT_EQ(index_scan_1.As<IndexScan>()->GetPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(index_scan_3.As<IndexScan>()->GetPredicates(), std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(index_scan_4.As<IndexScan>()->GetPredicates(), std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_EQ(index_scan_1.As<IndexScan>()->GetTableAlias(), "table");
  EXPECT_EQ(index_scan_1.As<IndexScan>()->GetIsForUpdate(), false);
  EXPECT_EQ(index_scan_06.As<IndexScan>()->GetKeyColumnOIDList(), key_column2);
  EXPECT_EQ(index_scan_07.As<IndexScan>()->GetExprTypeList(), expr_type2);
  EXPECT_EQ(index_scan_08.As<IndexScan>()->GetValueList(), value2);
  EXPECT_EQ(index_scan_1.GetName(), "IndexScan");
  EXPECT_TRUE(index_scan_1 == index_scan_2);
  EXPECT_FALSE(index_scan_1 == index_scan_3);
  EXPECT_FALSE(index_scan_1 == index_scan_01);
  EXPECT_FALSE(index_scan_1 == index_scan_02);
  EXPECT_FALSE(index_scan_1 == index_scan_03);
  EXPECT_FALSE(index_scan_1 == index_scan_04);
  EXPECT_FALSE(index_scan_1 == index_scan_05);
  EXPECT_FALSE(index_scan_1 == index_scan_06);
  EXPECT_FALSE(index_scan_1 == index_scan_07);
  EXPECT_FALSE(index_scan_1 == index_scan_08);
  EXPECT_FALSE(index_scan_1 == index_scan_4);
  EXPECT_FALSE(index_scan_4 == index_scan_5);
  EXPECT_FALSE(index_scan_1 == index_scan_6);
  EXPECT_EQ(index_scan_1.Hash(), index_scan_2.Hash());
  EXPECT_NE(index_scan_1.Hash(), index_scan_3.Hash());
  EXPECT_NE(index_scan_1.Hash(), index_scan_01.Hash());
  EXPECT_NE(index_scan_1.Hash(), index_scan_02.Hash());
  EXPECT_NE(index_scan_1.Hash(), index_scan_03.Hash());
  EXPECT_NE(index_scan_1.Hash(), index_scan_04.Hash());
  EXPECT_NE(index_scan_1.Hash(), index_scan_05.Hash());
  EXPECT_NE(index_scan_1.Hash(), index_scan_06.Hash());
  EXPECT_NE(index_scan_1.Hash(), index_scan_07.Hash());
  EXPECT_NE(index_scan_1.Hash(), index_scan_08.Hash());
  EXPECT_NE(index_scan_1.Hash(), index_scan_4.Hash());
  EXPECT_NE(index_scan_1.Hash(), index_scan_5.Hash());
  EXPECT_NE(index_scan_1.Hash(), index_scan_6.Hash());
  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, ExternalFileScanTest) {
  //===--------------------------------------------------------------------===//
  // ExternalFileScan
  //===--------------------------------------------------------------------===//
  Operator ext_file_scan_1 = ExternalFileScan::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\');
  Operator ext_file_scan_2 = ExternalFileScan::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\');
  Operator ext_file_scan_3 = ExternalFileScan::Make(parser::ExternalFileFormat::CSV, "file2.txt", ',', '"', '\\');
  Operator ext_file_scan_4 = ExternalFileScan::Make(parser::ExternalFileFormat::BINARY, "file.txt", ',', '"', '\\');
  Operator ext_file_scan_5 = ExternalFileScan::Make(parser::ExternalFileFormat::CSV, "file.txt", ' ', '"', '\\');
  Operator ext_file_scan_6 = ExternalFileScan::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '\'', '\\');
  Operator ext_file_scan_7 = ExternalFileScan::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '&');

  EXPECT_EQ(ext_file_scan_1.GetType(), OpType::EXTERNALFILESCAN);
  EXPECT_EQ(ext_file_scan_1.GetName(), "ExternalFileScan");
  EXPECT_EQ(ext_file_scan_1.As<ExternalFileScan>()->GetFormat(), parser::ExternalFileFormat::CSV);
  EXPECT_EQ(ext_file_scan_1.As<ExternalFileScan>()->GetFilename(), "file.txt");
  EXPECT_EQ(ext_file_scan_1.As<ExternalFileScan>()->GetDelimiter(), ',');
  EXPECT_EQ(ext_file_scan_1.As<ExternalFileScan>()->GetQuote(), '"');
  EXPECT_EQ(ext_file_scan_1.As<ExternalFileScan>()->GetEscape(), '\\');
  EXPECT_TRUE(ext_file_scan_1 == ext_file_scan_2);
  EXPECT_FALSE(ext_file_scan_1 == ext_file_scan_3);
  EXPECT_FALSE(ext_file_scan_1 == ext_file_scan_4);
  EXPECT_FALSE(ext_file_scan_1 == ext_file_scan_5);
  EXPECT_FALSE(ext_file_scan_1 == ext_file_scan_6);
  EXPECT_FALSE(ext_file_scan_1 == ext_file_scan_7);
  EXPECT_EQ(ext_file_scan_1.Hash(), ext_file_scan_2.Hash());
  EXPECT_NE(ext_file_scan_1.Hash(), ext_file_scan_3.Hash());
  EXPECT_NE(ext_file_scan_1.Hash(), ext_file_scan_4.Hash());
  EXPECT_NE(ext_file_scan_1.Hash(), ext_file_scan_5.Hash());
  EXPECT_NE(ext_file_scan_1.Hash(), ext_file_scan_6.Hash());
  EXPECT_NE(ext_file_scan_1.Hash(), ext_file_scan_7.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, QueryDerivedScanTest) {
  //===--------------------------------------------------------------------===//
  // QueryDerivedScan
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

  Operator query_derived_scan_1 = QueryDerivedScan::Make("alias", std::move(alias_to_expr_map_1));
  Operator query_derived_scan_2 = QueryDerivedScan::Make("alias", std::move(alias_to_expr_map_2));
  Operator query_derived_scan_3 = QueryDerivedScan::Make(
      "alias", std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>());
  Operator query_derived_scan_4 = QueryDerivedScan::Make("alias", std::move(alias_to_expr_map_3));
  Operator query_derived_scan_5 = QueryDerivedScan::Make("alias", std::move(alias_to_expr_map_4));
  Operator query_derived_scan_6 = QueryDerivedScan::Make("alias", std::move(alias_to_expr_map_5));

  EXPECT_EQ(query_derived_scan_1.GetType(), OpType::QUERYDERIVEDSCAN);
  EXPECT_EQ(query_derived_scan_1.GetName(), "QueryDerivedScan");
  EXPECT_EQ(query_derived_scan_1.As<QueryDerivedScan>()->GetTableAlias(), "alias");
  EXPECT_EQ(query_derived_scan_1.As<QueryDerivedScan>()->GetAliasToExprMap(), alias_to_expr_map_1_1);
  EXPECT_TRUE(query_derived_scan_1 == query_derived_scan_2);
  EXPECT_FALSE(query_derived_scan_1 == query_derived_scan_3);
  EXPECT_FALSE(query_derived_scan_1 == query_derived_scan_4);
  EXPECT_FALSE(query_derived_scan_1 == query_derived_scan_5);
  EXPECT_FALSE(query_derived_scan_1 == query_derived_scan_6);
  EXPECT_EQ(query_derived_scan_1.Hash(), query_derived_scan_2.Hash());
  EXPECT_NE(query_derived_scan_1.Hash(), query_derived_scan_3.Hash());
  EXPECT_NE(query_derived_scan_1.Hash(), query_derived_scan_4.Hash());
  EXPECT_NE(query_derived_scan_1.Hash(), query_derived_scan_5.Hash());
  EXPECT_NE(query_derived_scan_1.Hash(), query_derived_scan_6.Hash());

  delete expr_b_1;
  delete expr_b_2;
}

// NOLINTNEXTLINE
TEST(OperatorTests, OrderByTest) {
  // OrderBy operator does not have any data members.
  // So we just need to make sure that all instantiations
  // of the object are equivalent.
  Operator op1 = OrderBy::Make();
  EXPECT_EQ(op1.GetType(), OpType::ORDERBY);

  Operator op2 = OrderBy::Make();
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LimitTest) {
  //===--------------------------------------------------------------------===//
  // Limit
  //===--------------------------------------------------------------------===//
  size_t offset = 90;
  size_t limit = 22;
  auto sort_expr_ori = new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  auto sort_expr = common::ManagedPointer<parser::AbstractExpression>(sort_expr_ori);
  planner::OrderByOrderingType sort_dir = planner::OrderByOrderingType::ASC;

  // Check that all of our GET methods work as expected
  Operator op1 = Limit::Make(offset, limit, {sort_expr}, {sort_dir});
  EXPECT_EQ(op1.GetType(), OpType::LIMIT);
  EXPECT_EQ(op1.As<Limit>()->GetOffset(), offset);
  EXPECT_EQ(op1.As<Limit>()->GetLimit(), limit);
  EXPECT_EQ(op1.As<Limit>()->GetSortExpressions().size(), 1);
  EXPECT_EQ(op1.As<Limit>()->GetSortExpressions()[0], sort_expr);
  EXPECT_EQ(op1.As<Limit>()->GetSortAscending().size(), 1);
  EXPECT_EQ(op1.As<Limit>()->GetSortAscending()[0], sort_dir);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 = Limit::Make(offset, limit, {sort_expr}, {sort_dir});
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  size_t other_offset = 1111;
  Operator op3 = Limit::Make(other_offset, limit, {sort_expr}, {sort_dir});
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  delete sort_expr_ori;
}

// NOLINTNEXTLINE
TEST(OperatorTests, InnerNLJoinTest) {
  //===--------------------------------------------------------------------===//
  // InnerNLJoin
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

  Operator inner_nl_join_1 = InnerNLJoin::Make(std::vector<AnnotatedExpression>(), {x_1}, {x_1});
  Operator inner_nl_join_2 = InnerNLJoin::Make(std::vector<AnnotatedExpression>(), {x_1}, {x_1});
  Operator inner_nl_join_3 = InnerNLJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_0}, {x_1}, {x_1});
  Operator inner_nl_join_4 = InnerNLJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1}, {x_1}, {x_1});
  Operator inner_nl_join_5 = InnerNLJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_2}, {x_2}, {x_1});
  Operator inner_nl_join_6 = InnerNLJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1}, {x_1}, {x_2});
  Operator inner_nl_join_7 = InnerNLJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_3}, {x_1}, {x_1});
  Operator inner_nl_join_8 = InnerNLJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1}, {x_3}, {x_1});
  Operator inner_nl_join_9 = InnerNLJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1}, {x_1}, {x_3});

  EXPECT_EQ(inner_nl_join_1.GetType(), OpType::INNERNLJOIN);
  EXPECT_EQ(inner_nl_join_3.GetType(), OpType::INNERNLJOIN);
  EXPECT_EQ(inner_nl_join_1.GetName(), "InnerNLJoin");
  EXPECT_EQ(inner_nl_join_1.As<InnerNLJoin>()->GetJoinPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(inner_nl_join_3.As<InnerNLJoin>()->GetJoinPredicates(), std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(inner_nl_join_4.As<InnerNLJoin>()->GetJoinPredicates(), std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_EQ(inner_nl_join_1.As<InnerNLJoin>()->GetLeftKeys(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1});
  EXPECT_EQ(inner_nl_join_9.As<InnerNLJoin>()->GetRightKeys(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3});
  EXPECT_TRUE(inner_nl_join_1 == inner_nl_join_2);
  EXPECT_FALSE(inner_nl_join_1 == inner_nl_join_3);
  EXPECT_FALSE(inner_nl_join_4 == inner_nl_join_3);
  EXPECT_TRUE(inner_nl_join_4 == inner_nl_join_5);
  EXPECT_TRUE(inner_nl_join_4 == inner_nl_join_6);
  EXPECT_FALSE(inner_nl_join_4 == inner_nl_join_7);
  EXPECT_FALSE(inner_nl_join_4 == inner_nl_join_8);
  EXPECT_FALSE(inner_nl_join_4 == inner_nl_join_9);
  EXPECT_EQ(inner_nl_join_1.Hash(), inner_nl_join_2.Hash());
  EXPECT_NE(inner_nl_join_1.Hash(), inner_nl_join_3.Hash());
  EXPECT_NE(inner_nl_join_4.Hash(), inner_nl_join_3.Hash());
  EXPECT_EQ(inner_nl_join_4.Hash(), inner_nl_join_5.Hash());
  EXPECT_EQ(inner_nl_join_4.Hash(), inner_nl_join_6.Hash());
  EXPECT_NE(inner_nl_join_4.Hash(), inner_nl_join_7.Hash());
  EXPECT_NE(inner_nl_join_4.Hash(), inner_nl_join_8.Hash());
  EXPECT_NE(inner_nl_join_4.Hash(), inner_nl_join_9.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LeftNLJoinTest) {
  //===--------------------------------------------------------------------===//
  // LeftNLJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  Operator left_nl_join_1 = LeftNLJoin::Make(x_1);
  Operator left_nl_join_2 = LeftNLJoin::Make(x_2);
  Operator left_nl_join_3 = LeftNLJoin::Make(x_3);

  EXPECT_EQ(left_nl_join_1.GetType(), OpType::LEFTNLJOIN);
  EXPECT_EQ(left_nl_join_3.GetType(), OpType::LEFTNLJOIN);
  EXPECT_EQ(left_nl_join_1.GetName(), "LeftNLJoin");
  EXPECT_EQ(*(left_nl_join_1.As<LeftNLJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(left_nl_join_2.As<LeftNLJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(left_nl_join_3.As<LeftNLJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(left_nl_join_1 == left_nl_join_2);
  EXPECT_FALSE(left_nl_join_1 == left_nl_join_3);
  EXPECT_EQ(left_nl_join_1.Hash(), left_nl_join_2.Hash());
  EXPECT_NE(left_nl_join_1.Hash(), left_nl_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, RightNLJoinTest) {
  //===--------------------------------------------------------------------===//
  // RightNLJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  Operator right_nl_join_1 = RightNLJoin::Make(x_1);
  Operator right_nl_join_2 = RightNLJoin::Make(x_2);
  Operator right_nl_join_3 = RightNLJoin::Make(x_3);

  EXPECT_EQ(right_nl_join_1.GetType(), OpType::RIGHTNLJOIN);
  EXPECT_EQ(right_nl_join_3.GetType(), OpType::RIGHTNLJOIN);
  EXPECT_EQ(right_nl_join_1.GetName(), "RightNLJoin");
  EXPECT_EQ(*(right_nl_join_1.As<RightNLJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(right_nl_join_2.As<RightNLJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(right_nl_join_3.As<RightNLJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(right_nl_join_1 == right_nl_join_2);
  EXPECT_FALSE(right_nl_join_1 == right_nl_join_3);
  EXPECT_EQ(right_nl_join_1.Hash(), right_nl_join_2.Hash());
  EXPECT_NE(right_nl_join_1.Hash(), right_nl_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, OuterNLJoin) {
  //===--------------------------------------------------------------------===//
  // OuterNLJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  Operator outer_nl_join_1 = OuterNLJoin::Make(x_1);
  Operator outer_nl_join_2 = OuterNLJoin::Make(x_2);
  Operator outer_nl_join_3 = OuterNLJoin::Make(x_3);

  EXPECT_EQ(outer_nl_join_1.GetType(), OpType::OUTERNLJOIN);
  EXPECT_EQ(outer_nl_join_3.GetType(), OpType::OUTERNLJOIN);
  EXPECT_EQ(outer_nl_join_1.GetName(), "OuterNLJoin");
  EXPECT_EQ(*(outer_nl_join_1.As<OuterNLJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(outer_nl_join_2.As<OuterNLJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(outer_nl_join_3.As<OuterNLJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(outer_nl_join_1 == outer_nl_join_2);
  EXPECT_FALSE(outer_nl_join_1 == outer_nl_join_3);
  EXPECT_EQ(outer_nl_join_1.Hash(), outer_nl_join_2.Hash());
  EXPECT_NE(outer_nl_join_1.Hash(), outer_nl_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, InnerHashJoinTest) {
  //===--------------------------------------------------------------------===//
  // InnerHashJoin
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

  Operator inner_hash_join_1 = InnerHashJoin::Make(std::vector<AnnotatedExpression>(), {x_1}, {x_1});
  Operator inner_hash_join_2 = InnerHashJoin::Make(std::vector<AnnotatedExpression>(), {x_1}, {x_1});
  Operator inner_hash_join_3 = InnerHashJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_0}, {x_1}, {x_1});
  Operator inner_hash_join_4 = InnerHashJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1}, {x_1}, {x_1});
  Operator inner_hash_join_5 = InnerHashJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_2}, {x_2}, {x_1});
  Operator inner_hash_join_6 = InnerHashJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1}, {x_1}, {x_2});
  Operator inner_hash_join_7 = InnerHashJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_3}, {x_1}, {x_1});
  Operator inner_hash_join_8 = InnerHashJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1}, {x_3}, {x_1});
  Operator inner_hash_join_9 = InnerHashJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1}, {x_1}, {x_3});

  EXPECT_EQ(inner_hash_join_1.GetType(), OpType::INNERHASHJOIN);
  EXPECT_EQ(inner_hash_join_3.GetType(), OpType::INNERHASHJOIN);
  EXPECT_EQ(inner_hash_join_1.GetName(), "InnerHashJoin");
  EXPECT_EQ(inner_hash_join_1.As<InnerHashJoin>()->GetJoinPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(inner_hash_join_3.As<InnerHashJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(inner_hash_join_4.As<InnerHashJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_EQ(inner_hash_join_1.As<InnerHashJoin>()->GetLeftKeys(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1});
  EXPECT_EQ(inner_hash_join_9.As<InnerHashJoin>()->GetRightKeys(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3});
  EXPECT_TRUE(inner_hash_join_1 == inner_hash_join_2);
  EXPECT_FALSE(inner_hash_join_1 == inner_hash_join_3);
  EXPECT_FALSE(inner_hash_join_4 == inner_hash_join_3);
  EXPECT_TRUE(inner_hash_join_4 == inner_hash_join_5);
  EXPECT_TRUE(inner_hash_join_4 == inner_hash_join_6);
  EXPECT_FALSE(inner_hash_join_4 == inner_hash_join_7);
  EXPECT_FALSE(inner_hash_join_4 == inner_hash_join_8);
  EXPECT_FALSE(inner_hash_join_4 == inner_hash_join_9);
  EXPECT_EQ(inner_hash_join_1.Hash(), inner_hash_join_2.Hash());
  EXPECT_NE(inner_hash_join_1.Hash(), inner_hash_join_3.Hash());
  EXPECT_NE(inner_hash_join_4.Hash(), inner_hash_join_3.Hash());
  EXPECT_EQ(inner_hash_join_4.Hash(), inner_hash_join_5.Hash());
  EXPECT_EQ(inner_hash_join_4.Hash(), inner_hash_join_6.Hash());
  EXPECT_NE(inner_hash_join_4.Hash(), inner_hash_join_7.Hash());
  EXPECT_NE(inner_hash_join_4.Hash(), inner_hash_join_8.Hash());
  EXPECT_NE(inner_hash_join_4.Hash(), inner_hash_join_9.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LeftHashJoinTest) {
  //===--------------------------------------------------------------------===//
  // LeftHashJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  Operator left_hash_join_1 = LeftHashJoin::Make(x_1);
  Operator left_hash_join_2 = LeftHashJoin::Make(x_2);
  Operator left_hash_join_3 = LeftHashJoin::Make(x_3);

  EXPECT_EQ(left_hash_join_1.GetType(), OpType::LEFTHASHJOIN);
  EXPECT_EQ(left_hash_join_3.GetType(), OpType::LEFTHASHJOIN);
  EXPECT_EQ(left_hash_join_1.GetName(), "LeftHashJoin");
  EXPECT_EQ(*(left_hash_join_1.As<LeftHashJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(left_hash_join_2.As<LeftHashJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(left_hash_join_3.As<LeftHashJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(left_hash_join_1 == left_hash_join_2);
  EXPECT_FALSE(left_hash_join_1 == left_hash_join_3);
  EXPECT_EQ(left_hash_join_1.Hash(), left_hash_join_2.Hash());
  EXPECT_NE(left_hash_join_1.Hash(), left_hash_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, RightHashJoinTest) {
  //===--------------------------------------------------------------------===//
  // RightHashJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  Operator right_hash_join_1 = RightHashJoin::Make(x_1);
  Operator right_hash_join_2 = RightHashJoin::Make(x_2);
  Operator right_hash_join_3 = RightHashJoin::Make(x_3);

  EXPECT_EQ(right_hash_join_1.GetType(), OpType::RIGHTHASHJOIN);
  EXPECT_EQ(right_hash_join_3.GetType(), OpType::RIGHTHASHJOIN);
  EXPECT_EQ(right_hash_join_1.GetName(), "RightHashJoin");
  EXPECT_EQ(*(right_hash_join_1.As<RightHashJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(right_hash_join_2.As<RightHashJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(right_hash_join_3.As<RightHashJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(right_hash_join_1 == right_hash_join_2);
  EXPECT_FALSE(right_hash_join_1 == right_hash_join_3);
  EXPECT_EQ(right_hash_join_1.Hash(), right_hash_join_2.Hash());
  EXPECT_NE(right_hash_join_1.Hash(), right_hash_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, OuterHashJoinTest) {
  //===--------------------------------------------------------------------===//
  // OuterHashJoin
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  Operator outer_hash_join_1 = OuterHashJoin::Make(x_1);
  Operator outer_hash_join_2 = OuterHashJoin::Make(x_2);
  Operator outer_hash_join_3 = OuterHashJoin::Make(x_3);

  EXPECT_EQ(outer_hash_join_1.GetType(), OpType::OUTERHASHJOIN);
  EXPECT_EQ(outer_hash_join_3.GetType(), OpType::OUTERHASHJOIN);
  EXPECT_EQ(outer_hash_join_1.GetName(), "OuterHashJoin");
  EXPECT_EQ(*(outer_hash_join_1.As<OuterHashJoin>()->GetJoinPredicate()), *x_1);
  EXPECT_EQ(*(outer_hash_join_2.As<OuterHashJoin>()->GetJoinPredicate()), *x_2);
  EXPECT_EQ(*(outer_hash_join_3.As<OuterHashJoin>()->GetJoinPredicate()), *x_3);
  EXPECT_TRUE(outer_hash_join_1 == outer_hash_join_2);
  EXPECT_FALSE(outer_hash_join_1 == outer_hash_join_3);
  EXPECT_EQ(outer_hash_join_1.Hash(), outer_hash_join_2.Hash());
  EXPECT_NE(outer_hash_join_1.Hash(), outer_hash_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, InsertTest) {
  //===--------------------------------------------------------------------===//
  // Insert
  //===--------------------------------------------------------------------===//
  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);
  catalog::col_oid_t columns[] = {catalog::col_oid_t(1), catalog::col_oid_t(2)};
  std::vector<catalog::index_oid_t> indexes = {catalog::index_oid_t(4), catalog::index_oid_t(5)};
  parser::AbstractExpression *raw_values[] = {
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1)),
      new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(9))};
  std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> values = {
      std::vector<common::ManagedPointer<parser::AbstractExpression>>(raw_values, std::end(raw_values))};

  // Check that all of our GET methods work as expected
  Operator op1 =
      Insert::Make(database_oid, namespace_oid, table_oid, std::vector<catalog::col_oid_t>(columns, std::end(columns)),
                   std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>(values),
                   std::vector<catalog::index_oid_t>(indexes));
  EXPECT_EQ(op1.GetType(), OpType::INSERT);
  EXPECT_EQ(op1.As<Insert>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.As<Insert>()->GetNamespaceOid(), namespace_oid);
  EXPECT_EQ(op1.As<Insert>()->GetTableOid(), table_oid);
  EXPECT_EQ(op1.As<Insert>()->GetValues(), values);
  EXPECT_EQ(op1.As<Insert>()->GetColumns(), (std::vector<catalog::col_oid_t>(columns, std::end(columns))));
  EXPECT_EQ(op1.As<Insert>()->GetIndexes(), indexes);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 =
      Insert::Make(database_oid, namespace_oid, table_oid, std::vector<catalog::col_oid_t>(columns, std::end(columns)),
                   std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>(values),
                   std::vector<catalog::index_oid_t>(indexes));
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // For this last check, we are going to give it more rows to insert
  // This will make sure that our hash is going deep into the vectors
  std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>> other_values = {
      std::vector<common::ManagedPointer<parser::AbstractExpression>>(raw_values, std::end(raw_values)),
      std::vector<common::ManagedPointer<parser::AbstractExpression>>(raw_values, std::end(raw_values))};
  Operator op3 =
      Insert::Make(database_oid, namespace_oid, table_oid, std::vector<catalog::col_oid_t>(columns, std::end(columns)),
                   std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>(other_values),
                   std::vector<catalog::index_oid_t>(indexes));
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
  EXPECT_DEATH(
      Insert::Make(database_oid, namespace_oid, table_oid, std::vector<catalog::col_oid_t>(columns, std::end(columns)),
                   std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>(bad_values),
                   std::vector<catalog::index_oid_t>(indexes)),
      "Mismatched");
  for (auto entry : bad_raw_values) delete entry;
#endif

  for (auto entry : raw_values) delete entry;
}

// NOLINTNEXTLINE
TEST(OperatorTests, InsertSelectTest) {
  //===--------------------------------------------------------------------===//
  // InsertSelect
  //===--------------------------------------------------------------------===//
  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);
  std::vector<catalog::index_oid_t> index_oids{721};

  // Check that all of our GET methods work as expected
  Operator op1 =
      InsertSelect::Make(database_oid, namespace_oid, table_oid, std::vector<catalog::index_oid_t>(index_oids));
  EXPECT_EQ(op1.GetType(), OpType::INSERTSELECT);
  EXPECT_EQ(op1.As<InsertSelect>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.As<InsertSelect>()->GetNamespaceOid(), namespace_oid);
  EXPECT_EQ(op1.As<InsertSelect>()->GetTableOid(), table_oid);
  EXPECT_EQ(op1.As<InsertSelect>()->GetIndexes(), index_oids);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 =
      InsertSelect::Make(database_oid, namespace_oid, table_oid, std::vector<catalog::index_oid_t>(index_oids));
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  catalog::db_oid_t other_database_oid(999);
  Operator op3 =
      InsertSelect::Make(other_database_oid, namespace_oid, table_oid, std::vector<catalog::index_oid_t>(index_oids));
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, DeleteTest) {
  //===--------------------------------------------------------------------===//
  // Delete
  //===--------------------------------------------------------------------===//
  auto expr_b_1 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_2 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(true));
  auto expr_b_3 = new parser::ConstantValueExpression(type::TransientValueFactory::GetBoolean(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);

  // Check that all of our GET methods work as expected
  Operator op1 = Delete::Make(database_oid, namespace_oid, table_oid, x_1);
  EXPECT_EQ(op1.GetType(), OpType::DELETE);
  EXPECT_EQ(op1.As<Delete>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.As<Delete>()->GetNamespaceOid(), namespace_oid);
  EXPECT_EQ(op1.As<Delete>()->GetTableOid(), table_oid);
  EXPECT_EQ(op1.As<Delete>()->GetDeleteCondition(), x_1);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 = Delete::Make(database_oid, namespace_oid, table_oid, x_2);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  catalog::db_oid_t other_database_oid(999);
  Operator op3 = Delete::Make(other_database_oid, namespace_oid, table_oid, x_1);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  // Lastly, make another different object and make sure that it is not equal
  // and that it's hash is not the same!
  Operator op4 = Delete::Make(database_oid, namespace_oid, table_oid, x_3);
  EXPECT_FALSE(op1 == op4);
  EXPECT_NE(op1.Hash(), op4.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
}

// NOLINTNEXTLINE
TEST(OperatorTests, ExportExternalFileTest) {
  //===--------------------------------------------------------------------===//
  // ExportExternalFile
  //===--------------------------------------------------------------------===//
  std::string file_name = "fakefile.txt";
  char delimiter = 'X';
  char quote = 'Y';
  char escape = 'Z';

  // Check that all of our GET methods work as expected
  Operator op1 = ExportExternalFile::Make(parser::ExternalFileFormat::BINARY, file_name, delimiter, quote, escape);
  EXPECT_EQ(op1.GetType(), OpType::EXPORTEXTERNALFILE);
  EXPECT_EQ(op1.As<ExportExternalFile>()->GetFilename(), file_name);
  EXPECT_EQ(op1.As<ExportExternalFile>()->GetFormat(), parser::ExternalFileFormat::BINARY);
  EXPECT_EQ(op1.As<ExportExternalFile>()->GetDelimiter(), delimiter);
  EXPECT_EQ(op1.As<ExportExternalFile>()->GetQuote(), quote);
  EXPECT_EQ(op1.As<ExportExternalFile>()->GetEscape(), escape);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  std::string file_name_copy = file_name;  // NOLINT
  Operator op2 = ExportExternalFile::Make(parser::ExternalFileFormat::BINARY, file_name_copy, delimiter, quote, escape);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  Operator op3 = ExportExternalFile::Make(parser::ExternalFileFormat::CSV, file_name, delimiter, quote, escape);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, UpdateTest) {
  //===--------------------------------------------------------------------===//
  // Update
  //===--------------------------------------------------------------------===//
  std::string column = "abc";
  parser::AbstractExpression *value = new parser::ConstantValueExpression(type::TransientValueFactory::GetTinyInt(1));
  auto raw_update_clause = new parser::UpdateClause(column, common::ManagedPointer<parser::AbstractExpression>(value));
  auto update_clause = common::ManagedPointer(raw_update_clause);
  catalog::db_oid_t database_oid(123);
  catalog::namespace_oid_t namespace_oid(456);
  catalog::table_oid_t table_oid(789);

  // Check that all of our GET methods work as expected
  Operator op1 = Update::Make(database_oid, namespace_oid, table_oid, {update_clause});
  EXPECT_EQ(op1.GetType(), OpType::UPDATE);
  EXPECT_EQ(op1.As<Update>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.As<Update>()->GetNamespaceOid(), namespace_oid);
  EXPECT_EQ(op1.As<Update>()->GetTableOid(), table_oid);
  EXPECT_EQ(op1.As<Update>()->GetUpdateClauses().size(), 1);
  EXPECT_EQ(op1.As<Update>()->GetUpdateClauses()[0], update_clause);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 = Update::Make(database_oid, namespace_oid, table_oid, {update_clause});
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  Operator op3 = Update::Make(database_oid, namespace_oid, table_oid, {});
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  delete value;
  delete raw_update_clause;
}

// NOLINTNEXTLINE
TEST(OperatorTests, HashGroupByTest) {
  //===--------------------------------------------------------------------===//
  // HashGroupBy
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

  Operator group_by_1_0 = HashGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1},
                                            std::vector<AnnotatedExpression>{annotated_expr_0});
  Operator group_by_1_1 = HashGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1},
                                            std::vector<AnnotatedExpression>{annotated_expr_1});
  Operator group_by_2_2 = HashGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_2},
                                            std::vector<AnnotatedExpression>{annotated_expr_2});
  Operator group_by_3 = HashGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3},
                                          std::vector<AnnotatedExpression>());
  Operator group_by_7_4 = HashGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_7},
                                            std::vector<AnnotatedExpression>{annotated_expr_4});
  Operator group_by_4 = HashGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>(),
                                          std::vector<AnnotatedExpression>{annotated_expr_1});

  EXPECT_EQ(group_by_1_1.GetType(), OpType::HASHGROUPBY);
  EXPECT_EQ(group_by_3.GetType(), OpType::HASHGROUPBY);
  EXPECT_EQ(group_by_7_4.GetName(), "HashGroupBy");
  EXPECT_EQ(group_by_1_1.As<HashGroupBy>()->GetColumns(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1});
  EXPECT_EQ(group_by_3.As<HashGroupBy>()->GetColumns(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3});
  EXPECT_EQ(group_by_1_1.As<HashGroupBy>()->GetHaving(), std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_EQ(group_by_7_4.As<HashGroupBy>()->GetHaving(), std::vector<AnnotatedExpression>{annotated_expr_4});
  EXPECT_TRUE(group_by_1_1 == group_by_2_2);
  EXPECT_FALSE(group_by_1_1 == group_by_7_4);
  EXPECT_FALSE(group_by_1_0 == group_by_1_1);
  EXPECT_FALSE(group_by_3 == group_by_7_4);
  EXPECT_FALSE(group_by_4 == group_by_1_1);

  EXPECT_EQ(group_by_1_1.Hash(), group_by_2_2.Hash());
  EXPECT_NE(group_by_1_1.Hash(), group_by_7_4.Hash());
  EXPECT_NE(group_by_1_0.Hash(), group_by_1_1.Hash());
  EXPECT_NE(group_by_3.Hash(), group_by_7_4.Hash());
  EXPECT_NE(group_by_4.Hash(), group_by_1_1.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
  delete expr_b_4;
  delete expr_b_5;
  delete expr_b_6;
  delete expr_b_7;
  delete expr_b_8;
}

// NOLINTNEXTLINE
TEST(OperatorTests, SortGroupByTest) {
  //===--------------------------------------------------------------------===//
  // SortGroupBy
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

  Operator group_by_1_0 = SortGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1},
                                            std::vector<AnnotatedExpression>{annotated_expr_0});
  Operator group_by_1_1 = SortGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1},
                                            std::vector<AnnotatedExpression>{annotated_expr_1});
  Operator group_by_2_2 = SortGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_2},
                                            std::vector<AnnotatedExpression>{annotated_expr_2});
  Operator group_by_3 = SortGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3},
                                          std::vector<AnnotatedExpression>());
  Operator group_by_7_4 = SortGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_7},
                                            std::vector<AnnotatedExpression>{annotated_expr_4});
  Operator group_by_4 = SortGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>(),
                                          std::vector<AnnotatedExpression>{annotated_expr_1});

  EXPECT_EQ(group_by_1_1.GetType(), OpType::SORTGROUPBY);
  EXPECT_EQ(group_by_3.GetType(), OpType::SORTGROUPBY);
  EXPECT_EQ(group_by_7_4.GetName(), "SortGroupBy");
  EXPECT_EQ(group_by_1_1.As<SortGroupBy>()->GetColumns(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1});
  EXPECT_EQ(group_by_3.As<SortGroupBy>()->GetColumns(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3});
  EXPECT_EQ(group_by_1_1.As<SortGroupBy>()->GetHaving(), std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_EQ(group_by_7_4.As<SortGroupBy>()->GetHaving(), std::vector<AnnotatedExpression>{annotated_expr_4});
  EXPECT_TRUE(group_by_1_1 == group_by_2_2);
  EXPECT_FALSE(group_by_1_1 == group_by_7_4);
  EXPECT_FALSE(group_by_1_0 == group_by_1_1);
  EXPECT_FALSE(group_by_3 == group_by_7_4);
  EXPECT_FALSE(group_by_4 == group_by_1_1);

  EXPECT_EQ(group_by_1_1.Hash(), group_by_2_2.Hash());
  EXPECT_NE(group_by_1_1.Hash(), group_by_7_4.Hash());
  EXPECT_NE(group_by_1_0.Hash(), group_by_1_1.Hash());
  EXPECT_NE(group_by_3.Hash(), group_by_7_4.Hash());
  EXPECT_NE(group_by_4.Hash(), group_by_1_1.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;
  delete expr_b_4;
  delete expr_b_5;
  delete expr_b_6;
  delete expr_b_7;
  delete expr_b_8;
}

// NOLINTNEXTLINE
TEST(OperatorTests, AggregateTest) {
  //===--------------------------------------------------------------------===//
  // Aggregate
  //===--------------------------------------------------------------------===//
  // Aggregate operator does not have any data members.
  // So we just need to make sure that all instantiations
  // of the object are equivalent.
  Operator op1 = Aggregate::Make();
  EXPECT_EQ(op1.GetType(), OpType::AGGREGATE);

  Operator op2 = Aggregate::Make();
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, DistinctTest) {
  //===--------------------------------------------------------------------===//
  // Distinct
  //===--------------------------------------------------------------------===//
  // Distinct operator does not have any data members.
  // So we just need to make sure that all instantiations
  // of the object are equivalent.
  Operator op1 = Distinct::Make();
  EXPECT_EQ(op1.GetType(), OpType::DISTINCT);

  Operator op2 = Distinct::Make();
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());
}

}  // namespace terrier::optimizer
