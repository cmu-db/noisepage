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
//  std::unique_ptr<parser::UpdateClause> clause =
//      std::make_unique<parser::UpdateClause>(column, value);
  std::vector<std::unique_ptr<parser::UpdateClause>> updates;

  // Check that all of our GET methods work as expected
//  Operator op1 = LogicalUpdate::make(database_oid, namespace_oid, table_oid, updates);
//  EXPECT_EQ(op1.GetType(), OpType::LOGICALUPDATE);
//  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetFilename(), file_name);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
//  Operator op2 = LogicalExportExternalFile::make(parser::ExternalFileFormat::BINARY, file_name_copy, delimiter, quote, escape);
//  EXPECT_TRUE(op1 == op2);
//  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
//  Operator op3 = LogicalExportExternalFile::make(parser::ExternalFileFormat::CSV, file_name, delimiter, quote, escape);
//  EXPECT_FALSE(op1 == op3);
//  EXPECT_NE(op1.Hash(), op3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalExportExternalFileTest) {
  std::string file_name = "fakefile.txt";
  char delimiter = 'X';
  char quote = 'Y';
  char escape = 'Z';

  // Check that all of our GET methods work as expected
  Operator op1 = LogicalExportExternalFile::make(parser::ExternalFileFormat::BINARY, file_name, delimiter, quote, escape);
  EXPECT_EQ(op1.GetType(), OpType::LOGICALEXPORTEXTERNALFILE);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetFilename(), file_name);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetDelimiter(), delimiter);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetQuote(), quote);
  EXPECT_EQ(op1.As<LogicalExportExternalFile>()->GetEscape(), escape);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  std::string file_name_copy = file_name;
  Operator op2 = LogicalExportExternalFile::make(parser::ExternalFileFormat::BINARY, file_name_copy, delimiter, quote, escape);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  Operator op3 = LogicalExportExternalFile::make(parser::ExternalFileFormat::CSV, file_name, delimiter, quote, escape);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicSeqScanTest) {
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
TEST(OperatorTests, BasicIndexScanTest) {
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
TEST(OperatorTests, BasicExternalFileScanTest) {
  //===--------------------------------------------------------------------===//
  // ExternalFileScan
  //===--------------------------------------------------------------------===//
  Operator ext_file_scan_1 = ExternalFileScan::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\'');
  Operator ext_file_scan_2 = ExternalFileScan::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\'');
  Operator ext_file_scan_3 = ExternalFileScan::make(parser::ExternalFileFormat::CSV, "file2.txt", ',', '"', '\'');

  EXPECT_EQ(ext_file_scan_1.GetType(), OpType::EXTERNALFILESCAN);
  EXPECT_EQ(ext_file_scan_1.GetName(), "ExternalFileScan");
  EXPECT_TRUE(ext_file_scan_1 == ext_file_scan_2);
  EXPECT_FALSE(ext_file_scan_1 == ext_file_scan_3);
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicQueryDerivedScanTest) {
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
TEST(OperatorTests, BasicOrderByTest) {
  //===--------------------------------------------------------------------===//
  // OrderBy
  //===--------------------------------------------------------------------===//
  Operator order_by = OrderBy::make();

  EXPECT_EQ(order_by.GetType(), OpType::ORDERBY);
  EXPECT_EQ(order_by.GetName(), "OrderBy");
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicLimitTest) {
  //===--------------------------------------------------------------------===//
  // Limit
  //===--------------------------------------------------------------------===//
  Operator limit = Limit::make(0, 0, std::vector<parser::AbstractExpression *>(), std::vector<bool>());

  EXPECT_EQ(limit.GetType(), OpType::LIMIT);
  EXPECT_EQ(limit.GetName(), "Limit");
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicInnerNLJoinTest) {
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
TEST(OperatorTests, BasicLeftNLJoinTest) {
  //===--------------------------------------------------------------------===//
  // LeftNLJoin
  //===--------------------------------------------------------------------===//
  Operator left_nl_join = LeftNLJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(left_nl_join.GetType(), OpType::LEFTNLJOIN);
  EXPECT_EQ(left_nl_join.GetName(), "LeftNLJoin");
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicRightNLJoinTest) {
  //===--------------------------------------------------------------------===//
  // RightNLJoin
  //===--------------------------------------------------------------------===//
  Operator right_nl_join = RightNLJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(right_nl_join.GetType(), OpType::RIGHTNLJOIN);
  EXPECT_EQ(right_nl_join.GetName(), "RightNLJoin");
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicOuterNLJoin) {
  //===--------------------------------------------------------------------===//
  // OuterNLJoin
  //===--------------------------------------------------------------------===//
  Operator outer_nl_join = OuterNLJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(outer_nl_join.GetType(), OpType::OUTERNLJOIN);
  EXPECT_EQ(outer_nl_join.GetName(), "OuterNLJoin");
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicInnerHashJoinTest) {
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
TEST(OperatorTests, BasicLeftHashJoinTest) {
  //===--------------------------------------------------------------------===//
  // LeftHashJoin
  //===--------------------------------------------------------------------===//
  Operator left_hash_join = LeftHashJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(left_hash_join.GetType(), OpType::LEFTHASHJOIN);
  EXPECT_EQ(left_hash_join.GetName(), "LeftHashJoin");
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicRightHashJoinTest) {
  //===--------------------------------------------------------------------===//
  // RightHashJoin
  //===--------------------------------------------------------------------===//
  Operator right_hash_join = RightHashJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(right_hash_join.GetType(), OpType::RIGHTHASHJOIN);
  EXPECT_EQ(right_hash_join.GetName(), "RightHashJoin");
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicOuterHashJoinTest) {
  //===--------------------------------------------------------------------===//
  // OuterHashJoin
  //===--------------------------------------------------------------------===//
  Operator outer_hash_join = OuterHashJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(outer_hash_join.GetType(), OpType::OUTERHASHJOIN);
  EXPECT_EQ(outer_hash_join.GetName(), "OuterHashJoin");
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicInsertTest) {
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
TEST(OperatorTests, BasicInsertSelectTest) {
  //===--------------------------------------------------------------------===//
  // InsertSelect
  //===--------------------------------------------------------------------===//
  Operator insert_select =
      InsertSelect::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3));

  EXPECT_EQ(insert_select.GetType(), OpType::INSERTSELECT);
  EXPECT_EQ(insert_select.GetName(), "InsertSelect");
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicDeleteTest) {
  //===--------------------------------------------------------------------===//
  // Delete
  //===--------------------------------------------------------------------===//
  Operator del = Delete::make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(3), nullptr);

  EXPECT_EQ(del.GetType(), OpType::DELETE);
  EXPECT_EQ(del.GetName(), "Delete");
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicExportExternalFileTest) {
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
TEST(OperatorTests, BasicUpdateTest) {
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
TEST(OperatorTests, BasicHashGroupByTest) {
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
TEST(OperatorTests, BasicSortGroupByTest) {
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
TEST(OperatorTests, BasicAggregateTest) {
  //===--------------------------------------------------------------------===//
  // Aggregate
  //===--------------------------------------------------------------------===//
  Operator aggr = Aggregate::make();

  EXPECT_EQ(aggr.GetType(), OpType::AGGREGATE);
  EXPECT_EQ(aggr.GetName(), "Aggregate");
}

// NOLINTNEXTLINE
TEST(OperatorTests, BasicDistinctTest) {
  //===--------------------------------------------------------------------===//
  // Distinct
  //===--------------------------------------------------------------------===//
  Operator distinct = Distinct::make();

  EXPECT_EQ(distinct.GetType(), OpType::DISTINCT);
  EXPECT_EQ(distinct.GetName(), "Distinct");
}

}  // namespace terrier::optimizer
