#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "catalog/catalog_defs.h"
#include "optimizer/operator_expression.h"
#include "optimizer/physical_operator.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/update_statement.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"

#include "gtest/gtest.h"

namespace terrier::optimizer {

// Test the creation of operator objects
// NOLINTNEXTLINE
TEST(OperatorTests, BasicTest) {
  //===--------------------------------------------------------------------===//
  // SeqScan
  //===--------------------------------------------------------------------===//
  Operator seq_scan_1 = SeqScan::make(nullptr, "table", std::vector<AnnotatedExpression>(), false);
  Operator seq_scan_2 = SeqScan::make(nullptr, "table", std::vector<AnnotatedExpression>(), false);

  auto annotated_expr = AnnotatedExpression(nullptr, std::unordered_set<std::string>());
  Operator seq_scan_3 = SeqScan::make(nullptr, "table", std::vector<AnnotatedExpression>{annotated_expr}, false);

  EXPECT_EQ(seq_scan_1.GetType(), OpType::SeqScan);
  EXPECT_EQ(seq_scan_1.GetName(), "SeqScan");
  EXPECT_EQ(seq_scan_1.As<IndexScan>(), nullptr);
  EXPECT_TRUE(seq_scan_1 == seq_scan_2);
  EXPECT_FALSE(seq_scan_1 == seq_scan_3);

  //===--------------------------------------------------------------------===//
  // IndexScan
  //===--------------------------------------------------------------------===//
  Operator index_scan_1 = IndexScan::make(nullptr, "table", std::vector<AnnotatedExpression>(), false,
                                          catalog::index_oid_t(0), std::vector<catalog::col_oid_t>(),
                                          std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  Operator index_scan_2 = IndexScan::make(nullptr, "table", std::vector<AnnotatedExpression>(), false,
                                          catalog::index_oid_t(0), std::vector<catalog::col_oid_t>(),
                                          std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());
  Operator index_scan_3 = IndexScan::make(nullptr, "table", std::vector<AnnotatedExpression>(), false,
                                          catalog::index_oid_t(1), std::vector<catalog::col_oid_t>(),
                                          std::vector<parser::ExpressionType>(), std::vector<type::TransientValue>());

  EXPECT_EQ(index_scan_1.GetType(), OpType::IndexScan);
  EXPECT_EQ(index_scan_1.GetName(), "IndexScan");
  EXPECT_TRUE(index_scan_1 == index_scan_2);
  EXPECT_FALSE(index_scan_1 == index_scan_3);

  //===--------------------------------------------------------------------===//
  // ExternalFileScan
  //===--------------------------------------------------------------------===//
  Operator ext_file_scan_1 = ExternalFileScan::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\'');
  Operator ext_file_scan_2 = ExternalFileScan::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\'');
  Operator ext_file_scan_3 = ExternalFileScan::make(parser::ExternalFileFormat::CSV, "file2.txt", ',', '"', '\'');

  EXPECT_EQ(ext_file_scan_1.GetType(), OpType::ExternalFileScan);
  EXPECT_EQ(ext_file_scan_1.GetName(), "ExternalFileScan");
  EXPECT_TRUE(ext_file_scan_1 == ext_file_scan_2);
  EXPECT_FALSE(ext_file_scan_1 == ext_file_scan_3);

  //===--------------------------------------------------------------------===//
  // QueryDerivedScan
  //===--------------------------------------------------------------------===//
  auto alias_to_expr_map_1 = std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>();
  auto alias_to_expr_map_2 = std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>();
  auto expr_query_derived_scan =
      std::make_shared<parser::ConstantValueExpression>(type::TransientValueFactory::GetTinyInt(1));
  alias_to_expr_map_1["constant expr"] = expr_query_derived_scan;
  alias_to_expr_map_2["constant expr"] = expr_query_derived_scan;
  Operator query_derived_scan_1 = QueryDerivedScan::make("alias", alias_to_expr_map_1);
  Operator query_derived_scan_2 = QueryDerivedScan::make("alias", alias_to_expr_map_2);
  Operator query_derived_scan_3 =
      QueryDerivedScan::make("alias", std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>());

  EXPECT_EQ(query_derived_scan_1.GetType(), OpType::QueryDerivedScan);
  EXPECT_EQ(query_derived_scan_1.GetName(), "QueryDerivedScan");
  EXPECT_TRUE(query_derived_scan_1 == query_derived_scan_2);
  EXPECT_FALSE(query_derived_scan_1 == query_derived_scan_3);

  //===--------------------------------------------------------------------===//
  // OrderBy
  //===--------------------------------------------------------------------===//
  Operator order_by = OrderBy::make();

  EXPECT_EQ(order_by.GetType(), OpType::OrderBy);
  EXPECT_EQ(order_by.GetName(), "OrderBy");

  //===--------------------------------------------------------------------===//
  // Limit
  //===--------------------------------------------------------------------===//
  Operator limit = Limit::make(0, 0, std::vector<parser::AbstractExpression *>(), std::vector<bool>());

  EXPECT_EQ(limit.GetType(), OpType::Limit);
  EXPECT_EQ(limit.GetName(), "Limit");

  //===--------------------------------------------------------------------===//
  // InnerNLJoin
  //===--------------------------------------------------------------------===//
  Operator inner_nl_join_1 =
      InnerNLJoin::make(std::vector<AnnotatedExpression>(), std::vector<std::unique_ptr<parser::AbstractExpression>>(),
                        std::vector<std::unique_ptr<parser::AbstractExpression>>());
  Operator inner_nl_join_2 =
      InnerNLJoin::make(std::vector<AnnotatedExpression>(), std::vector<std::unique_ptr<parser::AbstractExpression>>(),
                        std::vector<std::unique_ptr<parser::AbstractExpression>>());

  EXPECT_EQ(inner_nl_join_1.GetType(), OpType::InnerNLJoin);
  EXPECT_EQ(inner_nl_join_1.GetName(), "InnerNLJoin");
  EXPECT_TRUE(inner_nl_join_1 == inner_nl_join_2);

  //===--------------------------------------------------------------------===//
  // LeftNLJoin
  //===--------------------------------------------------------------------===//
  Operator left_nl_join = LeftNLJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(left_nl_join.GetType(), OpType::LeftNLJoin);
  EXPECT_EQ(left_nl_join.GetName(), "LeftNLJoin");

  //===--------------------------------------------------------------------===//
  // RightNLJoin
  //===--------------------------------------------------------------------===//
  Operator right_nl_join = RightNLJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(right_nl_join.GetType(), OpType::RightNLJoin);
  EXPECT_EQ(right_nl_join.GetName(), "RightNLJoin");

  //===--------------------------------------------------------------------===//
  // OuterNLJoin
  //===--------------------------------------------------------------------===//
  Operator outer_nl_join = OuterNLJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(outer_nl_join.GetType(), OpType::OuterNLJoin);
  EXPECT_EQ(outer_nl_join.GetName(), "OuterNLJoin");

  //===--------------------------------------------------------------------===//
  // InnerHashJoin
  //===--------------------------------------------------------------------===//
  Operator inner_hash_join_1 = InnerHashJoin::make(std::vector<AnnotatedExpression>(),
                                                   std::vector<std::unique_ptr<parser::AbstractExpression>>(),
                                                   std::vector<std::unique_ptr<parser::AbstractExpression>>());
  Operator inner_hash_join_2 = InnerHashJoin::make(std::vector<AnnotatedExpression>(),
                                                   std::vector<std::unique_ptr<parser::AbstractExpression>>(),
                                                   std::vector<std::unique_ptr<parser::AbstractExpression>>());

  EXPECT_EQ(inner_hash_join_1.GetType(), OpType::InnerHashJoin);
  EXPECT_EQ(inner_hash_join_1.GetName(), "InnerHashJoin");
  EXPECT_TRUE(inner_hash_join_1 == inner_hash_join_2);

  //===--------------------------------------------------------------------===//
  // LeftHashJoin
  //===--------------------------------------------------------------------===//
  Operator left_hash_join = LeftHashJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(left_hash_join.GetType(), OpType::LeftHashJoin);
  EXPECT_EQ(left_hash_join.GetName(), "LeftHashJoin");

  //===--------------------------------------------------------------------===//
  // RightHashJoin
  //===--------------------------------------------------------------------===//
  Operator right_hash_join = RightHashJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(right_hash_join.GetType(), OpType::RightHashJoin);
  EXPECT_EQ(right_hash_join.GetName(), "RightHashJoin");

  //===--------------------------------------------------------------------===//
  // OuterHashJoin
  //===--------------------------------------------------------------------===//
  Operator outer_hash_join = OuterHashJoin::make(std::shared_ptr<parser::AbstractExpression>());

  EXPECT_EQ(outer_hash_join.GetType(), OpType::OuterHashJoin);
  EXPECT_EQ(outer_hash_join.GetName(), "OuterHashJoin");

  //===--------------------------------------------------------------------===//
  // Insert
  //===--------------------------------------------------------------------===//
  auto columns = new std::vector<std::string>;
  auto values = new std::vector<std::vector<std::unique_ptr<parser::AbstractExpression>>>;
  Operator insert = Insert::make(nullptr, std::vector<catalog::index_oid_t>(), columns, values);

  EXPECT_EQ(insert.GetType(), OpType::Insert);
  EXPECT_EQ(insert.GetName(), "Insert");

  delete columns;
  delete values;

  //===--------------------------------------------------------------------===//
  // InsertSelect
  //===--------------------------------------------------------------------===//
  Operator insert_select = InsertSelect::make(nullptr, std::vector<catalog::index_oid_t>());

  EXPECT_EQ(insert_select.GetType(), OpType::InsertSelect);
  EXPECT_EQ(insert_select.GetName(), "InsertSelect");

  //===--------------------------------------------------------------------===//
  // Delete
  //===--------------------------------------------------------------------===//
  Operator del = Delete::make(nullptr, std::vector<catalog::index_oid_t>());

  EXPECT_EQ(del.GetType(), OpType::Delete);
  EXPECT_EQ(del.GetName(), "Delete");

  //===--------------------------------------------------------------------===//
  // ExportExternalFile
  //===--------------------------------------------------------------------===//
  Operator export_ext_file_1 = ExportExternalFile::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\'');
  Operator export_ext_file_2 = ExportExternalFile::make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\'');
  Operator export_ext_file_3 = ExportExternalFile::make(parser::ExternalFileFormat::CSV, "file2.txt", ',', '"', '\'');

  EXPECT_EQ(export_ext_file_1.GetType(), OpType::ExportExternalFile);
  EXPECT_EQ(export_ext_file_1.GetName(), "ExportExternalFile");
  EXPECT_TRUE(export_ext_file_1 == export_ext_file_2);
  EXPECT_FALSE(export_ext_file_1 == export_ext_file_3);

  //===--------------------------------------------------------------------===//
  // Update
  //===--------------------------------------------------------------------===//
  auto updates = new std::vector<std::unique_ptr<parser::UpdateClause>>;
  Operator update = Update::make(nullptr, std::vector<catalog::index_oid_t>(), updates);

  EXPECT_EQ(update.GetType(), OpType::Update);
  EXPECT_EQ(update.GetName(), "Update");

  delete updates;

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

  EXPECT_EQ(hash_group_by_1.GetType(), OpType::HashGroupBy);
  EXPECT_EQ(hash_group_by_2.GetName(), "HashGroupBy");
  EXPECT_TRUE(hash_group_by_1 == hash_group_by_2);
  EXPECT_FALSE(hash_group_by_1 == hash_group_by_3);

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

  EXPECT_EQ(sort_group_by_1.GetType(), OpType::SortGroupBy);
  EXPECT_EQ(sort_group_by_1.GetName(), "SortGroupBy");
  EXPECT_TRUE(sort_group_by_1 == sort_group_by_2);
  EXPECT_FALSE(sort_group_by_1 == sort_group_by_3);

  //===--------------------------------------------------------------------===//
  // Aggregate
  //===--------------------------------------------------------------------===//
  Operator aggr = Aggregate::make();

  EXPECT_EQ(aggr.GetType(), OpType::Aggregate);
  EXPECT_EQ(aggr.GetName(), "Aggregate");

  //===--------------------------------------------------------------------===//
  // Distinct
  //===--------------------------------------------------------------------===//
  Operator distinct = Distinct::make();

  EXPECT_EQ(distinct.GetType(), OpType::Distinct);
  EXPECT_EQ(distinct.GetName(), "Distinct");
}

}  // namespace terrier::optimizer
