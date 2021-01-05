#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "catalog/catalog_defs.h"
#include "common/managed_pointer.h"
#include "execution/sql/value_util.h"
#include "gtest/gtest.h"
#include "optimizer/logical_operators.h"
#include "optimizer/operator_node.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression/constant_value_expression.h"
#include "parser/update_statement.h"
#include "transaction/deferred_action_manager.h"
#include "transaction/transaction_manager.h"

namespace noisepage::optimizer {

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalInsertTest) {
  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  catalog::db_oid_t database_oid(123);
  catalog::table_oid_t table_oid(789);
  catalog::col_oid_t columns[] = {catalog::col_oid_t(1), catalog::col_oid_t(2)};
  parser::AbstractExpression *raw_values[] = {
      new parser::ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(1)),
      new parser::ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(9))};
  auto *values = new std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>(
      {std::vector<common::ManagedPointer<parser::AbstractExpression>>(raw_values, std::end(raw_values))});

  // Check that all of our GET methods work as expected
  Operator op1 =
      LogicalInsert::Make(
          database_oid, table_oid, std::vector<catalog::col_oid_t>(columns, std::end(columns)),
          common::ManagedPointer<std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>>(values))
          .RegisterWithTxnContext(txn_context);
  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALINSERT);
  EXPECT_EQ(op1.GetContentsAs<LogicalInsert>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.GetContentsAs<LogicalInsert>()->GetTableOid(), table_oid);
  EXPECT_EQ(op1.GetContentsAs<LogicalInsert>()->GetColumns(),
            (std::vector<catalog::col_oid_t>{catalog::col_oid_t(1), catalog::col_oid_t(2)}));
  EXPECT_EQ(op1.GetContentsAs<LogicalInsert>()->GetValues(), values);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 =
      LogicalInsert::Make(
          database_oid, table_oid, std::vector<catalog::col_oid_t>(columns, std::end(columns)),
          common::ManagedPointer<std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>>(values))
          .RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // For this last check, we are going to give it more rows to insert
  // This will make sure that our hash is going deep into the vectors
  auto *other_values = new std::vector{
      std::vector<common::ManagedPointer<parser::AbstractExpression>>(raw_values, std::end(raw_values)),
      std::vector<common::ManagedPointer<parser::AbstractExpression>>(raw_values, std::end(raw_values))};
  Operator op3 =
      LogicalInsert::Make(
          database_oid, table_oid, std::vector<catalog::col_oid_t>(columns, std::end(columns)),
          common::ManagedPointer<std::vector<std::vector<common::ManagedPointer<parser::AbstractExpression>>>>(
              other_values))
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  // All operators created should be cleaned up on abort
  txn_manager.Abort(txn_context);
  delete txn_context;

  for (auto entry : raw_values) delete entry;
  delete values;
  delete other_values;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalInsertSelectTest) {
  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  catalog::db_oid_t database_oid(123);
  catalog::table_oid_t table_oid(789);

  // Check that all of our GET methods work as expected
  Operator op1 = LogicalInsertSelect::Make(database_oid, table_oid).RegisterWithTxnContext(txn_context);
  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALINSERTSELECT);
  EXPECT_EQ(op1.GetContentsAs<LogicalInsertSelect>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.GetContentsAs<LogicalInsertSelect>()->GetTableOid(), table_oid);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 = LogicalInsertSelect::Make(database_oid, table_oid).RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  catalog::db_oid_t other_database_oid(999);
  Operator op3 = LogicalInsertSelect::Make(other_database_oid, table_oid).RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalLimitTest) {
  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  size_t offset = 90;
  size_t limit = 22;
  parser::AbstractExpression *sort_expr_ori =
      new parser::ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(1));
  auto sort_expr = common::ManagedPointer<parser::AbstractExpression>(sort_expr_ori);
  OrderByOrderingType sort_dir = OrderByOrderingType::ASC;

  // Check that all of our GET methods work as expected
  Operator op1 = LogicalLimit::Make(offset, limit, {sort_expr}, {sort_dir}).RegisterWithTxnContext(txn_context);
  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALLIMIT);
  EXPECT_EQ(op1.GetContentsAs<LogicalLimit>()->GetOffset(), offset);
  EXPECT_EQ(op1.GetContentsAs<LogicalLimit>()->GetLimit(), limit);
  EXPECT_EQ(op1.GetContentsAs<LogicalLimit>()->GetSortExpressions().size(), 1);
  EXPECT_EQ(op1.GetContentsAs<LogicalLimit>()->GetSortExpressions()[0], sort_expr);
  EXPECT_EQ(op1.GetContentsAs<LogicalLimit>()->GetSortDirections().size(), 1);
  EXPECT_EQ(op1.GetContentsAs<LogicalLimit>()->GetSortDirections()[0], sort_dir);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 = LogicalLimit::Make(offset, limit, {sort_expr}, {sort_dir}).RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  size_t other_offset = 1111;
  Operator op3 = LogicalLimit::Make(other_offset, limit, {sort_expr}, {sort_dir}).RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
  delete sort_expr_ori;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalDeleteTest) {
  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();
  catalog::db_oid_t database_oid(123);
  catalog::table_oid_t table_oid(789);

  // Check that all of our GET methods work as expected
  Operator op1 = LogicalDelete::Make(database_oid, "tbl", table_oid).RegisterWithTxnContext(txn_context);
  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALDELETE);
  EXPECT_EQ(op1.GetContentsAs<LogicalDelete>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.GetContentsAs<LogicalDelete>()->GetTableOid(), table_oid);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  Operator op2 = LogicalDelete::Make(database_oid, "tbl", table_oid).RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  catalog::db_oid_t other_database_oid(999);
  Operator op3 = LogicalDelete::Make(other_database_oid, "tbl", table_oid).RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalUpdateTest) {
  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  std::string column = "abc";
  parser::AbstractExpression *value =
      new parser::ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(1));
  auto update_clause =
      std::make_unique<parser::UpdateClause>(column, common::ManagedPointer<parser::AbstractExpression>(value));
  auto update_clause2 =
      std::make_unique<parser::UpdateClause>(column, common::ManagedPointer<parser::AbstractExpression>(value));
  std::vector<common::ManagedPointer<parser::UpdateClause>> update_clause_v;
  update_clause_v.emplace_back(common::ManagedPointer<parser::UpdateClause>(update_clause));
  catalog::db_oid_t database_oid(123);
  catalog::table_oid_t table_oid(789);

  // Check that all of our GET methods work as expected
  Operator op1 = LogicalUpdate::Make(database_oid, "tbl", table_oid, std::move(update_clause_v))
                     .RegisterWithTxnContext(txn_context);
  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALUPDATE);
  EXPECT_EQ(op1.GetContentsAs<LogicalUpdate>()->GetDatabaseOid(), database_oid);
  EXPECT_EQ(op1.GetContentsAs<LogicalUpdate>()->GetTableOid(), table_oid);
  EXPECT_EQ(op1.GetContentsAs<LogicalUpdate>()->GetUpdateClauses().size(), 1);
  EXPECT_EQ(update_clause2->GetColumnName(),
            op1.GetContentsAs<LogicalUpdate>()->GetUpdateClauses()[0]->GetColumnName());
  EXPECT_EQ(update_clause2->GetUpdateValue(),
            op1.GetContentsAs<LogicalUpdate>()->GetUpdateClauses()[0]->GetUpdateValue());

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  std::vector<common::ManagedPointer<parser::UpdateClause>> update_clause_v2;
  update_clause_v2.emplace_back(common::ManagedPointer<parser::UpdateClause>(update_clause2));
  Operator op2 = LogicalUpdate::Make(database_oid, "tbl", table_oid, std::move(update_clause_v2))
                     .RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  Operator op3 = LogicalUpdate::Make(database_oid, "tbl", table_oid, {}).RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
  delete value;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalExportExternalFileTest) {
  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  std::string file_name = "fakefile.txt";
  char delimiter = 'X';
  char quote = 'Y';
  char escape = 'Z';

  // Check that all of our GET methods work as expected
  Operator op1 =
      LogicalExportExternalFile::Make(parser::ExternalFileFormat::BINARY, file_name, delimiter, quote, escape)
          .RegisterWithTxnContext(txn_context);
  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALEXPORTEXTERNALFILE);
  EXPECT_EQ(op1.GetContentsAs<LogicalExportExternalFile>()->GetFormat(), parser::ExternalFileFormat::BINARY);
  EXPECT_EQ(op1.GetContentsAs<LogicalExportExternalFile>()->GetFilename(), file_name);
  EXPECT_EQ(op1.GetContentsAs<LogicalExportExternalFile>()->GetDelimiter(), delimiter);
  EXPECT_EQ(op1.GetContentsAs<LogicalExportExternalFile>()->GetQuote(), quote);
  EXPECT_EQ(op1.GetContentsAs<LogicalExportExternalFile>()->GetEscape(), escape);

  // Check that if we make a new object with the same values, then it will
  // be equal to our first object and have the same hash
  std::string file_name_copy = file_name;  // NOLINT
  Operator op2 =
      LogicalExportExternalFile::Make(parser::ExternalFileFormat::BINARY, file_name_copy, delimiter, quote, escape)
          .RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  // Lastly, make a different object and make sure that it is not equal
  // and that it's hash is not the same!
  Operator op3 = LogicalExportExternalFile::Make(parser::ExternalFileFormat::CSV, file_name, delimiter, quote, escape)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalGetTest) {
  //===--------------------------------------------------------------------===//
  // LogicalGet
  //===--------------------------------------------------------------------===//
  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_3 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_get_01 = LogicalGet::Make(catalog::db_oid_t(2), catalog::table_oid_t(3),
                                             std::vector<AnnotatedExpression>(), "table", false)
                                .RegisterWithTxnContext(txn_context);
  Operator logical_get_03 = LogicalGet::Make(catalog::db_oid_t(1), catalog::table_oid_t(4),
                                             std::vector<AnnotatedExpression>(), "table", false)
                                .RegisterWithTxnContext(txn_context);
  Operator logical_get_04 = LogicalGet::Make(catalog::db_oid_t(1), catalog::table_oid_t(3),
                                             std::vector<AnnotatedExpression>(), "tableTable", false)
                                .RegisterWithTxnContext(txn_context);
  Operator logical_get_05 =
      LogicalGet::Make(catalog::db_oid_t(1), catalog::table_oid_t(3), std::vector<AnnotatedExpression>(), "table", true)
          .RegisterWithTxnContext(txn_context);
  Operator logical_get_1 = LogicalGet::Make(catalog::db_oid_t(1), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>(), "table", false)
                               .RegisterWithTxnContext(txn_context);
  Operator logical_get_3 = LogicalGet::Make(catalog::db_oid_t(1), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>{annotated_expr_0}, "table", false)
                               .RegisterWithTxnContext(txn_context);
  Operator logical_get_4 = LogicalGet::Make(catalog::db_oid_t(1), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>{annotated_expr_1}, "table", false)
                               .RegisterWithTxnContext(txn_context);
  Operator logical_get_5 = LogicalGet::Make(catalog::db_oid_t(1), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>{annotated_expr_2}, "table", false)
                               .RegisterWithTxnContext(txn_context);
  Operator logical_get_6 = LogicalGet::Make(catalog::db_oid_t(1), catalog::table_oid_t(3),
                                            std::vector<AnnotatedExpression>{annotated_expr_3}, "table", false)
                               .RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_get_1.GetOpType(), OpType::LOGICALGET);
  EXPECT_EQ(logical_get_1.GetContentsAs<LogicalGet>()->GetDatabaseOid(), catalog::db_oid_t(1));
  EXPECT_EQ(logical_get_1.GetContentsAs<LogicalGet>()->GetTableOid(), catalog::table_oid_t(3));
  EXPECT_EQ(logical_get_1.GetContentsAs<LogicalGet>()->GetPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_get_3.GetContentsAs<LogicalGet>()->GetPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(logical_get_4.GetContentsAs<LogicalGet>()->GetPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_EQ(logical_get_1.GetContentsAs<LogicalGet>()->GetTableAlias(), "table");
  EXPECT_EQ(logical_get_1.GetContentsAs<LogicalGet>()->GetIsForUpdate(), false);
  EXPECT_EQ(logical_get_1.GetName(), "LogicalGet");
  EXPECT_FALSE(logical_get_1 == logical_get_3);
  EXPECT_FALSE(logical_get_1 == logical_get_01);
  EXPECT_FALSE(logical_get_1 == logical_get_03);
  EXPECT_FALSE(logical_get_1 == logical_get_04);
  EXPECT_FALSE(logical_get_1 == logical_get_05);
  EXPECT_FALSE(logical_get_1 == logical_get_4);
  EXPECT_FALSE(logical_get_4 == logical_get_5);
  EXPECT_FALSE(logical_get_1 == logical_get_6);
  EXPECT_NE(logical_get_1.Hash(), logical_get_3.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_01.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_03.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_04.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_05.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_4.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_5.Hash());
  EXPECT_NE(logical_get_1.Hash(), logical_get_6.Hash());
  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalExternalFileGetTest) {
  //===--------------------------------------------------------------------===//
  // LogicalExternalFileGet
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  Operator logical_ext_file_get_1 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\')
          .RegisterWithTxnContext(txn_context);
  Operator logical_ext_file_get_2 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '\\')
          .RegisterWithTxnContext(txn_context);
  Operator logical_ext_file_get_3 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file2.txt", ',', '"', '\\')
          .RegisterWithTxnContext(txn_context);
  Operator logical_ext_file_get_4 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::BINARY, "file.txt", ',', '"', '\\')
          .RegisterWithTxnContext(txn_context);
  Operator logical_ext_file_get_5 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ' ', '"', '\\')
          .RegisterWithTxnContext(txn_context);
  Operator logical_ext_file_get_6 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '\'', '\\')
          .RegisterWithTxnContext(txn_context);
  Operator logical_ext_file_get_7 =
      LogicalExternalFileGet::Make(parser::ExternalFileFormat::CSV, "file.txt", ',', '"', '&')
          .RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_ext_file_get_1.GetOpType(), OpType::LOGICALEXTERNALFILEGET);
  EXPECT_EQ(logical_ext_file_get_1.GetName(), "LogicalExternalFileGet");
  EXPECT_EQ(logical_ext_file_get_1.GetContentsAs<LogicalExternalFileGet>()->GetFormat(),
            parser::ExternalFileFormat::CSV);
  EXPECT_EQ(logical_ext_file_get_1.GetContentsAs<LogicalExternalFileGet>()->GetFilename(), "file.txt");
  EXPECT_EQ(logical_ext_file_get_1.GetContentsAs<LogicalExternalFileGet>()->GetDelimiter(), ',');
  EXPECT_EQ(logical_ext_file_get_1.GetContentsAs<LogicalExternalFileGet>()->GetQuote(), '"');
  EXPECT_EQ(logical_ext_file_get_1.GetContentsAs<LogicalExternalFileGet>()->GetEscape(), '\\');
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

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalQueryDerivedGetTest) {
  //===--------------------------------------------------------------------===//
  // LogicalQueryDerivedGet
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  auto alias_to_expr_map_1 = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();
  auto alias_to_expr_map_1_1 = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();
  auto alias_to_expr_map_2 = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();
  auto alias_to_expr_map_3 = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();
  auto alias_to_expr_map_4 = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();
  auto alias_to_expr_map_5 = std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(1));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(1));
  auto expr1 = common::ManagedPointer(expr_b_1);
  auto expr2 = common::ManagedPointer(expr_b_2);

  alias_to_expr_map_1["constant expr"] = expr1;
  alias_to_expr_map_1_1["constant expr"] = expr1;
  alias_to_expr_map_2["constant expr"] = expr1;
  alias_to_expr_map_3["constant expr"] = expr2;
  alias_to_expr_map_4["constant expr2"] = expr1;
  alias_to_expr_map_5["constant expr"] = expr1;
  alias_to_expr_map_5["constant expr2"] = expr2;

  Operator logical_query_derived_get_1 =
      LogicalQueryDerivedGet::Make("alias", std::move(alias_to_expr_map_1)).RegisterWithTxnContext(txn_context);
  Operator logical_query_derived_get_2 =
      LogicalQueryDerivedGet::Make("alias", std::move(alias_to_expr_map_2)).RegisterWithTxnContext(txn_context);
  Operator logical_query_derived_get_3 =
      LogicalQueryDerivedGet::Make(
          "alias", std::unordered_map<std::string, common::ManagedPointer<parser::AbstractExpression>>())
          .RegisterWithTxnContext(txn_context);
  Operator logical_query_derived_get_4 =
      LogicalQueryDerivedGet::Make("alias", std::move(alias_to_expr_map_3)).RegisterWithTxnContext(txn_context);
  Operator logical_query_derived_get_5 =
      LogicalQueryDerivedGet::Make("alias", std::move(alias_to_expr_map_4)).RegisterWithTxnContext(txn_context);
  Operator logical_query_derived_get_6 =
      LogicalQueryDerivedGet::Make("alias", std::move(alias_to_expr_map_5)).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_query_derived_get_1.GetOpType(), OpType::LOGICALQUERYDERIVEDGET);
  EXPECT_EQ(logical_query_derived_get_1.GetName(), "LogicalQueryDerivedGet");
  EXPECT_EQ(logical_query_derived_get_1.GetContentsAs<LogicalQueryDerivedGet>()->GetTableAlias(), "alias");
  EXPECT_EQ(logical_query_derived_get_1.GetContentsAs<LogicalQueryDerivedGet>()->GetAliasToExprMap(),
            alias_to_expr_map_1_1);
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

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalFilterTest) {
  //===--------------------------------------------------------------------===//
  // LogicalFilter
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_3 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_filter_1 =
      LogicalFilter::Make(std::vector<AnnotatedExpression>()).RegisterWithTxnContext(txn_context);
  Operator logical_filter_2 =
      LogicalFilter::Make(std::vector<AnnotatedExpression>()).RegisterWithTxnContext(txn_context);
  Operator logical_filter_3 =
      LogicalFilter::Make(std::vector<AnnotatedExpression>{annotated_expr_0}).RegisterWithTxnContext(txn_context);
  Operator logical_filter_4 =
      LogicalFilter::Make(std::vector<AnnotatedExpression>{annotated_expr_1}).RegisterWithTxnContext(txn_context);
  Operator logical_filter_5 =
      LogicalFilter::Make(std::vector<AnnotatedExpression>{annotated_expr_2}).RegisterWithTxnContext(txn_context);
  Operator logical_filter_6 =
      LogicalFilter::Make(std::vector<AnnotatedExpression>{annotated_expr_3}).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_filter_1.GetOpType(), OpType::LOGICALFILTER);
  EXPECT_EQ(logical_filter_3.GetOpType(), OpType::LOGICALFILTER);
  EXPECT_EQ(logical_filter_1.GetName(), "LogicalFilter");
  EXPECT_EQ(logical_filter_1.GetContentsAs<LogicalFilter>()->GetPredicates(), std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_filter_3.GetContentsAs<LogicalFilter>()->GetPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(logical_filter_4.GetContentsAs<LogicalFilter>()->GetPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_1});
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

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalProjectionTest) {
  //===--------------------------------------------------------------------===//
  // LogicalProjection
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_3 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  Operator logical_projection_1 =
      LogicalProjection::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1})
          .RegisterWithTxnContext(txn_context);
  Operator logical_projection_2 =
      LogicalProjection::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_2})
          .RegisterWithTxnContext(txn_context);
  Operator logical_projection_3 =
      LogicalProjection::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3})
          .RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_projection_1.GetOpType(), OpType::LOGICALPROJECTION);
  EXPECT_EQ(logical_projection_3.GetOpType(), OpType::LOGICALPROJECTION);
  EXPECT_EQ(logical_projection_1.GetName(), "LogicalProjection");
  EXPECT_EQ(logical_projection_1.GetContentsAs<LogicalProjection>()->GetExpressions(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1});
  EXPECT_EQ(logical_projection_3.GetContentsAs<LogicalProjection>()->GetExpressions(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3});
  EXPECT_TRUE(logical_projection_1 == logical_projection_2);
  EXPECT_FALSE(logical_projection_1 == logical_projection_3);
  EXPECT_EQ(logical_projection_1.Hash(), logical_projection_2.Hash());
  EXPECT_NE(logical_projection_1.Hash(), logical_projection_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalDependentJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalDependentJoin
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_3 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_dep_join_0 = LogicalDependentJoin::Make().RegisterWithTxnContext(txn_context);
  Operator logical_dep_join_1 =
      LogicalDependentJoin::Make(std::vector<AnnotatedExpression>()).RegisterWithTxnContext(txn_context);
  Operator logical_dep_join_2 =
      LogicalDependentJoin::Make(std::vector<AnnotatedExpression>()).RegisterWithTxnContext(txn_context);
  Operator logical_dep_join_3 = LogicalDependentJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_0})
                                    .RegisterWithTxnContext(txn_context);
  Operator logical_dep_join_4 = LogicalDependentJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1})
                                    .RegisterWithTxnContext(txn_context);
  Operator logical_dep_join_5 = LogicalDependentJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_2})
                                    .RegisterWithTxnContext(txn_context);
  Operator logical_dep_join_6 = LogicalDependentJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_3})
                                    .RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_dep_join_1.GetOpType(), OpType::LOGICALDEPENDENTJOIN);
  EXPECT_EQ(logical_dep_join_3.GetOpType(), OpType::LOGICALDEPENDENTJOIN);
  EXPECT_EQ(logical_dep_join_1.GetName(), "LogicalDependentJoin");
  EXPECT_EQ(logical_dep_join_1.GetContentsAs<LogicalDependentJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_dep_join_3.GetContentsAs<LogicalDependentJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(logical_dep_join_4.GetContentsAs<LogicalDependentJoin>()->GetJoinPredicates(),
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

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalMarkJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalMarkJoin
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_3 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_mark_join_0 = LogicalMarkJoin::Make().RegisterWithTxnContext(txn_context);
  Operator logical_mark_join_1 =
      LogicalMarkJoin::Make(std::vector<AnnotatedExpression>()).RegisterWithTxnContext(txn_context);
  Operator logical_mark_join_2 =
      LogicalMarkJoin::Make(std::vector<AnnotatedExpression>()).RegisterWithTxnContext(txn_context);
  Operator logical_mark_join_3 =
      LogicalMarkJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_0}).RegisterWithTxnContext(txn_context);
  Operator logical_mark_join_4 =
      LogicalMarkJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1}).RegisterWithTxnContext(txn_context);
  Operator logical_mark_join_5 =
      LogicalMarkJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_2}).RegisterWithTxnContext(txn_context);
  Operator logical_mark_join_6 =
      LogicalMarkJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_3}).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_mark_join_1.GetOpType(), OpType::LOGICALMARKJOIN);
  EXPECT_EQ(logical_mark_join_3.GetOpType(), OpType::LOGICALMARKJOIN);
  EXPECT_EQ(logical_mark_join_1.GetName(), "LogicalMarkJoin");
  EXPECT_EQ(logical_mark_join_1.GetContentsAs<LogicalMarkJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_mark_join_3.GetContentsAs<LogicalMarkJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(logical_mark_join_4.GetContentsAs<LogicalMarkJoin>()->GetJoinPredicates(),
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

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalSingleJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalSingleJoin
  //===--------------------------------------------------------------------===//
  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_3 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_single_join_0 = LogicalSingleJoin::Make().RegisterWithTxnContext(txn_context);
  Operator logical_single_join_1 =
      LogicalSingleJoin::Make(std::vector<AnnotatedExpression>()).RegisterWithTxnContext(txn_context);
  Operator logical_single_join_2 =
      LogicalSingleJoin::Make(std::vector<AnnotatedExpression>()).RegisterWithTxnContext(txn_context);
  Operator logical_single_join_3 =
      LogicalSingleJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_0}).RegisterWithTxnContext(txn_context);
  Operator logical_single_join_4 =
      LogicalSingleJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1}).RegisterWithTxnContext(txn_context);
  Operator logical_single_join_5 =
      LogicalSingleJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_2}).RegisterWithTxnContext(txn_context);
  Operator logical_single_join_6 =
      LogicalSingleJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_3}).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_single_join_1.GetOpType(), OpType::LOGICALSINGLEJOIN);
  EXPECT_EQ(logical_single_join_3.GetOpType(), OpType::LOGICALSINGLEJOIN);
  EXPECT_EQ(logical_single_join_1.GetName(), "LogicalSingleJoin");
  EXPECT_EQ(logical_single_join_1.GetContentsAs<LogicalSingleJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_single_join_3.GetContentsAs<LogicalSingleJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(logical_single_join_4.GetContentsAs<LogicalSingleJoin>()->GetJoinPredicates(),
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

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalInnerJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalInnerJoin
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_3 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_0 =
      AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression>(), std::unordered_set<std::string>());
  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_inner_join_0 = LogicalInnerJoin::Make().RegisterWithTxnContext(txn_context);
  Operator logical_inner_join_1 =
      LogicalInnerJoin::Make(std::vector<AnnotatedExpression>()).RegisterWithTxnContext(txn_context);
  Operator logical_inner_join_2 =
      LogicalInnerJoin::Make(std::vector<AnnotatedExpression>()).RegisterWithTxnContext(txn_context);
  Operator logical_inner_join_3 =
      LogicalInnerJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_0}).RegisterWithTxnContext(txn_context);
  Operator logical_inner_join_4 =
      LogicalInnerJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_1}).RegisterWithTxnContext(txn_context);
  Operator logical_inner_join_5 =
      LogicalInnerJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_2}).RegisterWithTxnContext(txn_context);
  Operator logical_inner_join_6 =
      LogicalInnerJoin::Make(std::vector<AnnotatedExpression>{annotated_expr_3}).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_inner_join_1.GetOpType(), OpType::LOGICALINNERJOIN);
  EXPECT_EQ(logical_inner_join_3.GetOpType(), OpType::LOGICALINNERJOIN);
  EXPECT_EQ(logical_inner_join_1.GetName(), "LogicalInnerJoin");
  EXPECT_EQ(logical_inner_join_1.GetContentsAs<LogicalInnerJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>());
  EXPECT_EQ(logical_inner_join_3.GetContentsAs<LogicalInnerJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>{annotated_expr_0});
  EXPECT_EQ(logical_inner_join_4.GetContentsAs<LogicalInnerJoin>()->GetJoinPredicates(),
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

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalLeftJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalLeftJoin
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_3 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_left_join_1 =
      LogicalLeftJoin::Make(std::vector<AnnotatedExpression>({annotated_expr_1})).RegisterWithTxnContext(txn_context);
  Operator logical_left_join_2 =
      LogicalLeftJoin::Make(std::vector<AnnotatedExpression>({annotated_expr_2})).RegisterWithTxnContext(txn_context);
  Operator logical_left_join_3 =
      LogicalLeftJoin::Make(std::vector<AnnotatedExpression>({annotated_expr_3})).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_left_join_1.GetOpType(), OpType::LOGICALLEFTJOIN);
  EXPECT_EQ(logical_left_join_3.GetOpType(), OpType::LOGICALLEFTJOIN);
  EXPECT_EQ(logical_left_join_1.GetName(), "LogicalLeftJoin");
  EXPECT_EQ(logical_left_join_1.GetContentsAs<LogicalLeftJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>({annotated_expr_1}));
  EXPECT_EQ(logical_left_join_2.GetContentsAs<LogicalLeftJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>({annotated_expr_2}));
  EXPECT_EQ(logical_left_join_3.GetContentsAs<LogicalLeftJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>({annotated_expr_3}));
  EXPECT_TRUE(logical_left_join_1 == logical_left_join_2);
  EXPECT_FALSE(logical_left_join_1 == logical_left_join_3);
  EXPECT_EQ(logical_left_join_1.Hash(), logical_left_join_2.Hash());
  EXPECT_NE(logical_left_join_1.Hash(), logical_left_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalRightJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalRightJoin
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_3 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_right_join_1 =
      LogicalRightJoin::Make(std::vector<AnnotatedExpression>({annotated_expr_1})).RegisterWithTxnContext(txn_context);
  Operator logical_right_join_2 =
      LogicalRightJoin::Make(std::vector<AnnotatedExpression>({annotated_expr_2})).RegisterWithTxnContext(txn_context);
  Operator logical_right_join_3 =
      LogicalRightJoin::Make(std::vector<AnnotatedExpression>({annotated_expr_3})).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_right_join_1.GetOpType(), OpType::LOGICALRIGHTJOIN);
  EXPECT_EQ(logical_right_join_3.GetOpType(), OpType::LOGICALRIGHTJOIN);
  EXPECT_EQ(logical_right_join_1.GetName(), "LogicalRightJoin");
  EXPECT_EQ(logical_right_join_1.GetContentsAs<LogicalRightJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>({annotated_expr_1}));
  EXPECT_EQ(logical_right_join_2.GetContentsAs<LogicalRightJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>({annotated_expr_2}));
  EXPECT_EQ(logical_right_join_3.GetContentsAs<LogicalRightJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>({annotated_expr_3}));

  EXPECT_TRUE(logical_right_join_1 == logical_right_join_2);
  EXPECT_FALSE(logical_right_join_1 == logical_right_join_3);
  EXPECT_EQ(logical_right_join_1.Hash(), logical_right_join_2.Hash());
  EXPECT_NE(logical_right_join_1.Hash(), logical_right_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalOuterJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalOuterJoin
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_3 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_outer_join_1 =
      LogicalOuterJoin::Make(std::vector<AnnotatedExpression>({annotated_expr_1})).RegisterWithTxnContext(txn_context);
  Operator logical_outer_join_2 =
      LogicalOuterJoin::Make(std::vector<AnnotatedExpression>({annotated_expr_2})).RegisterWithTxnContext(txn_context);
  Operator logical_outer_join_3 =
      LogicalOuterJoin::Make(std::vector<AnnotatedExpression>({annotated_expr_3})).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_outer_join_1.GetOpType(), OpType::LOGICALOUTERJOIN);
  EXPECT_EQ(logical_outer_join_3.GetOpType(), OpType::LOGICALOUTERJOIN);
  EXPECT_EQ(logical_outer_join_1.GetName(), "LogicalOuterJoin");
  EXPECT_EQ(logical_outer_join_1.GetContentsAs<LogicalOuterJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>({annotated_expr_1}));
  EXPECT_EQ(logical_outer_join_2.GetContentsAs<LogicalOuterJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>({annotated_expr_2}));
  EXPECT_EQ(logical_outer_join_3.GetContentsAs<LogicalOuterJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>({annotated_expr_3}));
  EXPECT_TRUE(logical_outer_join_1 == logical_outer_join_2);
  EXPECT_FALSE(logical_outer_join_1 == logical_outer_join_3);
  EXPECT_EQ(logical_outer_join_1.Hash(), logical_outer_join_2.Hash());
  EXPECT_NE(logical_outer_join_1.Hash(), logical_outer_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalSemiJoinTest) {
  //===--------------------------------------------------------------------===//
  // LogicalSemiJoin
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_3 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));

  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);

  auto annotated_expr_1 = AnnotatedExpression(x_1, std::unordered_set<std::string>());
  auto annotated_expr_2 = AnnotatedExpression(x_2, std::unordered_set<std::string>());
  auto annotated_expr_3 = AnnotatedExpression(x_3, std::unordered_set<std::string>());

  Operator logical_semi_join_1 =
      LogicalSemiJoin::Make(std::vector<AnnotatedExpression>({annotated_expr_1})).RegisterWithTxnContext(txn_context);
  Operator logical_semi_join_2 =
      LogicalSemiJoin::Make(std::vector<AnnotatedExpression>({annotated_expr_2})).RegisterWithTxnContext(txn_context);
  Operator logical_semi_join_3 =
      LogicalSemiJoin::Make(std::vector<AnnotatedExpression>({annotated_expr_3})).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_semi_join_1.GetOpType(), OpType::LOGICALSEMIJOIN);
  EXPECT_EQ(logical_semi_join_3.GetOpType(), OpType::LOGICALSEMIJOIN);
  EXPECT_EQ(logical_semi_join_1.GetName(), "LogicalSemiJoin");
  EXPECT_EQ(logical_semi_join_1.GetContentsAs<LogicalSemiJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>({annotated_expr_1}));
  EXPECT_EQ(logical_semi_join_2.GetContentsAs<LogicalSemiJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>({annotated_expr_2}));
  EXPECT_EQ(logical_semi_join_3.GetContentsAs<LogicalSemiJoin>()->GetJoinPredicates(),
            std::vector<AnnotatedExpression>({annotated_expr_3}));
  EXPECT_TRUE(logical_semi_join_1 == logical_semi_join_2);
  EXPECT_FALSE(logical_semi_join_1 == logical_semi_join_3);
  EXPECT_EQ(logical_semi_join_1.Hash(), logical_semi_join_2.Hash());
  EXPECT_NE(logical_semi_join_1.Hash(), logical_semi_join_3.Hash());

  delete expr_b_1;
  delete expr_b_2;
  delete expr_b_3;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalAggregateAndGroupByTest) {
  //===--------------------------------------------------------------------===//
  // LogicalAggregateAndGroupBy
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  // ConstValueExpression subclass AbstractExpression
  parser::AbstractExpression *expr_b_1 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_2 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_3 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));
  parser::AbstractExpression *expr_b_7 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));

  // columns: vector of ManagedPointer of AbstractExpression
  auto x_1 = common::ManagedPointer<parser::AbstractExpression>(expr_b_1);
  auto x_2 = common::ManagedPointer<parser::AbstractExpression>(expr_b_2);
  auto x_3 = common::ManagedPointer<parser::AbstractExpression>(expr_b_3);
  auto x_7 = common::ManagedPointer<parser::AbstractExpression>(expr_b_7);

  // ConstValueExpression subclass AbstractExpression
  parser::AbstractExpression *expr_b_4 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_5 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_8 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true));
  parser::AbstractExpression *expr_b_6 =
      new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(false));
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
                                       std::vector<AnnotatedExpression>{annotated_expr_0})
          .RegisterWithTxnContext(txn_context);
  Operator logical_group_1_1 =
      LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1},
                                       std::vector<AnnotatedExpression>{annotated_expr_1})
          .RegisterWithTxnContext(txn_context);
  Operator logical_group_2_2 =
      LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_2},
                                       std::vector<AnnotatedExpression>{annotated_expr_2})
          .RegisterWithTxnContext(txn_context);
  Operator logical_group_3 =
      LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3})
          .RegisterWithTxnContext(txn_context);
  Operator logical_group_7_4 =
      LogicalAggregateAndGroupBy::Make(std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_7},
                                       std::vector<AnnotatedExpression>{annotated_expr_4})
          .RegisterWithTxnContext(txn_context);

  EXPECT_EQ(logical_group_1_1.GetOpType(), OpType::LOGICALAGGREGATEANDGROUPBY);
  EXPECT_EQ(logical_group_3.GetOpType(), OpType::LOGICALAGGREGATEANDGROUPBY);
  EXPECT_EQ(logical_group_7_4.GetName(), "LogicalAggregateAndGroupBy");
  EXPECT_EQ(logical_group_1_1.GetContentsAs<LogicalAggregateAndGroupBy>()->GetColumns(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_1});
  EXPECT_EQ(logical_group_3.GetContentsAs<LogicalAggregateAndGroupBy>()->GetColumns(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{x_3});
  EXPECT_EQ(logical_group_1_1.GetContentsAs<LogicalAggregateAndGroupBy>()->GetHaving(),
            std::vector<AnnotatedExpression>{annotated_expr_1});
  EXPECT_EQ(logical_group_7_4.GetContentsAs<LogicalAggregateAndGroupBy>()->GetHaving(),
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

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalCreateDatabaseTest) {
  //===--------------------------------------------------------------------===//
  // LogicalCreateDatabase
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  Operator op1 = LogicalCreateDatabase::Make("testdb").RegisterWithTxnContext(txn_context);
  Operator op2 = LogicalCreateDatabase::Make("testdb").RegisterWithTxnContext(txn_context);
  Operator op3 = LogicalCreateDatabase::Make("another_testdb").RegisterWithTxnContext(txn_context);

  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALCREATEDATABASE);
  EXPECT_EQ(op3.GetOpType(), OpType::LOGICALCREATEDATABASE);
  EXPECT_EQ(op1.GetName(), "LogicalCreateDatabase");
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateDatabase>()->GetDatabaseName(), "testdb");
  EXPECT_EQ(op3.GetContentsAs<LogicalCreateDatabase>()->GetDatabaseName(), "another_testdb");
  EXPECT_TRUE(op1 == op2);
  EXPECT_FALSE(op1 == op3);
  EXPECT_EQ(op1.Hash(), op2.Hash());
  EXPECT_NE(op1.Hash(), op3.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalCreateFunctionTest) {
  //===--------------------------------------------------------------------===//
  // LogicalCreateFunction
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  Operator op1 =
      LogicalCreateFunction::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "function1", parser::PLType::PL_C,
                                  {}, {"param"}, {parser::BaseFunctionParameter::DataType::INTEGER},
                                  parser::BaseFunctionParameter::DataType::BOOLEAN, 1, false)
          .RegisterWithTxnContext(txn_context);

  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALCREATEFUNCTION);
  EXPECT_EQ(op1.GetName(), "LogicalCreateFunction");
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateFunction>()->GetDatabaseOid(), catalog::db_oid_t(1));
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateFunction>()->GetNamespaceOid(), catalog::namespace_oid_t(1));
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateFunction>()->GetFunctionName(), "function1");
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateFunction>()->GetUDFLanguage(), parser::PLType::PL_C);
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateFunction>()->GetFunctionBody(), std::vector<std::string>{});
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateFunction>()->GetFunctionParameterNames(), std::vector<std::string>{"param"});
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateFunction>()->GetFunctionParameterTypes(),
            std::vector<parser::BaseFunctionParameter::DataType>{parser::BaseFunctionParameter::DataType::INTEGER});
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateFunction>()->GetReturnType(),
            parser::BaseFunctionParameter::DataType::BOOLEAN);
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateFunction>()->GetParamCount(), 1);
  EXPECT_FALSE(op1.GetContentsAs<LogicalCreateFunction>()->IsReplace());

  Operator op2 =
      LogicalCreateFunction::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "function1", parser::PLType::PL_C,
                                  {}, {"param"}, {parser::BaseFunctionParameter::DataType::INTEGER},
                                  parser::BaseFunctionParameter::DataType::BOOLEAN, 1, false)
          .RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  Operator op3 =
      LogicalCreateFunction::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(3), "function1", parser::PLType::PL_C,
                                  {}, {"param"}, {parser::BaseFunctionParameter::DataType::INTEGER},
                                  parser::BaseFunctionParameter::DataType::BOOLEAN, 1, false)
          .RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 != op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  Operator op4 =
      LogicalCreateFunction::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "function4", parser::PLType::PL_C,
                                  {}, {"param"}, {parser::BaseFunctionParameter::DataType::INTEGER},
                                  parser::BaseFunctionParameter::DataType::BOOLEAN, 1, false)
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op4);
  EXPECT_NE(op1.Hash(), op4.Hash());

  Operator op5 = LogicalCreateFunction::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "function1",
                                             parser::PLType::PL_PGSQL, {}, {"param"},
                                             {parser::BaseFunctionParameter::DataType::INTEGER},
                                             parser::BaseFunctionParameter::DataType::BOOLEAN, 1, false)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op5);
  EXPECT_NE(op1.Hash(), op5.Hash());

  Operator op6 =
      LogicalCreateFunction::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "function1", parser::PLType::PL_C,
                                  {"body", "body2"}, {"param"}, {parser::BaseFunctionParameter::DataType::INTEGER},
                                  parser::BaseFunctionParameter::DataType::BOOLEAN, 1, false)
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op6);
  EXPECT_NE(op1.Hash(), op6.Hash());

  Operator op7 = LogicalCreateFunction::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "function1",
                                             parser::PLType::PL_C, {}, {"param1", "param2"},
                                             {parser::BaseFunctionParameter::DataType::INTEGER,
                                              parser::BaseFunctionParameter::DataType::BOOLEAN},
                                             parser::BaseFunctionParameter::DataType::BOOLEAN, 2, false)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op7);
  EXPECT_NE(op1.Hash(), op7.Hash());

  Operator op8 =
      LogicalCreateFunction::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "function1", parser::PLType::PL_C,
                                  {}, {}, {}, parser::BaseFunctionParameter::DataType::BOOLEAN, 0, false)
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op8);
  EXPECT_NE(op1.Hash(), op8.Hash());

  Operator op9 =
      LogicalCreateFunction::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "function1", parser::PLType::PL_C,
                                  {}, {"param"}, {parser::BaseFunctionParameter::DataType::VARCHAR},
                                  parser::BaseFunctionParameter::DataType::BOOLEAN, 1, false)
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op9);
  EXPECT_NE(op1.Hash(), op9.Hash());

  Operator op10 =
      LogicalCreateFunction::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "function1", parser::PLType::PL_C,
                                  {}, {"param"}, {parser::BaseFunctionParameter::DataType::INTEGER},
                                  parser::BaseFunctionParameter::DataType::INTEGER, 1, false)
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op10);
  EXPECT_NE(op1.Hash(), op10.Hash());

  Operator op11 =
      LogicalCreateFunction::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "function1", parser::PLType::PL_C,
                                  {}, {"param"}, {parser::BaseFunctionParameter::DataType::INTEGER},
                                  parser::BaseFunctionParameter::DataType::BOOLEAN, 1, true)
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op11);
  EXPECT_NE(op1.Hash(), op11.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalCreateIndexTest) {
  //===--------------------------------------------------------------------===//
  // LogicalCreateIndex
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  Operator op1 =
      LogicalCreateIndex::Make(catalog::namespace_oid_t(1), catalog::table_oid_t(1), parser::IndexType::BWTREE, true,
                               "index_1", std::vector<common::ManagedPointer<parser::AbstractExpression>>{})
          .RegisterWithTxnContext(txn_context);

  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALCREATEINDEX);
  EXPECT_EQ(op1.GetName(), "LogicalCreateIndex");
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateIndex>()->GetIndexName(), "index_1");
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateIndex>()->GetNamespaceOid(), catalog::namespace_oid_t(1));
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateIndex>()->GetTableOid(), catalog::table_oid_t(1));
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateIndex>()->GetIndexType(), parser::IndexType::BWTREE);
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateIndex>()->GetIndexAttr(),
            std::vector<common::ManagedPointer<parser::AbstractExpression>>{});
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateIndex>()->IsUnique(), true);

  Operator op2 =
      LogicalCreateIndex::Make(catalog::namespace_oid_t(1), catalog::table_oid_t(1), parser::IndexType::BWTREE, true,
                               "index_1", std::vector<common::ManagedPointer<parser::AbstractExpression>>{})
          .RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  std::vector<common::ManagedPointer<parser::AbstractExpression>> raw_values = {
      common::ManagedPointer<parser::AbstractExpression>(
          new parser::ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(1))),
      common::ManagedPointer<parser::AbstractExpression>(
          new parser::ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(9)))};
  auto raw_values_copy = raw_values;
  Operator op3 = LogicalCreateIndex::Make(catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                          parser::IndexType::BWTREE, true, "index_1", std::move(raw_values_copy))
                     .RegisterWithTxnContext(txn_context);
  EXPECT_EQ(op3.GetContentsAs<LogicalCreateIndex>()->GetIndexAttr(), raw_values);
  EXPECT_FALSE(op3 == op1);
  EXPECT_NE(op1.Hash(), op3.Hash());

  auto raw_values_copy2 = raw_values;
  Operator op4 = LogicalCreateIndex::Make(catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                          parser::IndexType::BWTREE, true, "index_1", std::move(raw_values_copy2))
                     .RegisterWithTxnContext(txn_context);
  EXPECT_EQ(op4.GetContentsAs<LogicalCreateIndex>()->GetIndexAttr(), raw_values);
  EXPECT_TRUE(op3 == op4);
  EXPECT_EQ(op4.Hash(), op3.Hash());

  std::vector<common::ManagedPointer<parser::AbstractExpression>> raw_values_2 = {
      common::ManagedPointer<parser::AbstractExpression>(
          new parser::ConstantValueExpression(type::TypeId::BOOLEAN, execution::sql::BoolVal(true))),
      common::ManagedPointer<parser::AbstractExpression>(
          new parser::ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(9)))};
  auto raw_values_copy3 = raw_values_2;
  Operator op10 = LogicalCreateIndex::Make(catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                           parser::IndexType::BWTREE, true, "index_1", std::move(raw_values_copy3))
                      .RegisterWithTxnContext(txn_context);
  EXPECT_EQ(op10.GetContentsAs<LogicalCreateIndex>()->GetIndexAttr(), raw_values_2);
  EXPECT_FALSE(op3 == op10);
  EXPECT_NE(op10.Hash(), op3.Hash());

  Operator op5 =
      LogicalCreateIndex::Make(catalog::namespace_oid_t(2), catalog::table_oid_t(1), parser::IndexType::BWTREE, true,
                               "index_1", std::vector<common::ManagedPointer<parser::AbstractExpression>>{})
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op5);
  EXPECT_NE(op1.Hash(), op5.Hash());

  Operator op6 =
      LogicalCreateIndex::Make(catalog::namespace_oid_t(1), catalog::table_oid_t(2), parser::IndexType::BWTREE, true,
                               "index_1", std::vector<common::ManagedPointer<parser::AbstractExpression>>{})
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op6);
  EXPECT_NE(op1.Hash(), op6.Hash());

  Operator op7 =
      LogicalCreateIndex::Make(catalog::namespace_oid_t(1), catalog::table_oid_t(1), parser::IndexType::HASH, true,
                               "index_1", std::vector<common::ManagedPointer<parser::AbstractExpression>>{})
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op7);
  EXPECT_NE(op1.Hash(), op7.Hash());

  Operator op8 =
      LogicalCreateIndex::Make(catalog::namespace_oid_t(1), catalog::table_oid_t(1), parser::IndexType::BWTREE, false,
                               "index_1", std::vector<common::ManagedPointer<parser::AbstractExpression>>{})
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op8);
  EXPECT_NE(op1.Hash(), op8.Hash());

  Operator op9 =
      LogicalCreateIndex::Make(catalog::namespace_oid_t(1), catalog::table_oid_t(1), parser::IndexType::BWTREE, true,
                               "index_2", std::vector<common::ManagedPointer<parser::AbstractExpression>>{})
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op9);
  EXPECT_NE(op1.Hash(), op9.Hash());

  for (auto entry : raw_values) delete entry.Get();
  for (auto entry : raw_values_2) delete entry.Get();

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalCreateTableTest) {
  //===--------------------------------------------------------------------===//
  // LogicalCreateTable
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  auto col_def = new parser::ColumnDefinition(
      "col_1", parser::ColumnDefinition::DataType::INTEGER, true, true, true,
      common::ManagedPointer<parser::AbstractExpression>(
          new parser::ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(9))),
      nullptr, 4);
  Operator op1 = LogicalCreateTable::Make(catalog::namespace_oid_t(1), "Table_1",
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{
                                              common::ManagedPointer<parser::ColumnDefinition>(col_def)},
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{})
                     .RegisterWithTxnContext(txn_context);
  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALCREATETABLE);
  EXPECT_EQ(op1.GetName(), "LogicalCreateTable");
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateTable>()->GetTableName(), "Table_1");
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateTable>()->GetNamespaceOid(), catalog::namespace_oid_t(1));
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateTable>()->GetForeignKeys(),
            std::vector<common::ManagedPointer<parser::ColumnDefinition>>{});
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateTable>()->GetColumns().size(), 1);
  EXPECT_EQ(*op1.GetContentsAs<LogicalCreateTable>()->GetColumns().at(0), *col_def);

  Operator op2 = LogicalCreateTable::Make(catalog::namespace_oid_t(1), "Table_1",
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{
                                              common::ManagedPointer<parser::ColumnDefinition>(col_def)},
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{})
                     .RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  Operator op3 = LogicalCreateTable::Make(catalog::namespace_oid_t(2), "Table_1",
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{
                                              common::ManagedPointer<parser::ColumnDefinition>(col_def)},
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{})
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  Operator op4 = LogicalCreateTable::Make(catalog::namespace_oid_t(1), "Table_2",
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{
                                              common::ManagedPointer<parser::ColumnDefinition>(col_def)},
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{})
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op4);
  EXPECT_NE(op1.Hash(), op4.Hash());

  Operator op5 = LogicalCreateTable::Make(catalog::namespace_oid_t(1), "Table_1",
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{
                                              common::ManagedPointer<parser::ColumnDefinition>(col_def),
                                              common::ManagedPointer<parser::ColumnDefinition>(col_def)},
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{})
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op5);
  EXPECT_NE(op1.Hash(), op5.Hash());

  Operator op6 = LogicalCreateTable::Make(catalog::namespace_oid_t(1), "Table_1",
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{},
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{})
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op6);
  EXPECT_NE(op1.Hash(), op6.Hash());

  auto col_def_2_string_val = execution::sql::ValueUtil::CreateStringVal(std::string_view("col"));
  auto col_def_2 = new parser::ColumnDefinition(
      "col_1", parser::ColumnDefinition::DataType::VARCHAR, true, true, true,
      common::ManagedPointer<parser::AbstractExpression>(new parser::ConstantValueExpression(
          type::TypeId::VARCHAR, col_def_2_string_val.first, std::move(col_def_2_string_val.second))),
      nullptr, 20);
  Operator op7 = LogicalCreateTable::Make(catalog::namespace_oid_t(1), "Table_2",
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{
                                              common::ManagedPointer<parser::ColumnDefinition>(col_def_2)},
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{})
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op7);
  EXPECT_NE(op1.Hash(), op7.Hash());

  auto foreign_def =
      new parser::ColumnDefinition({"foreign_col_1"}, {"col_1"}, "foreign", parser::FKConstrActionType::SETNULL,
                                   parser::FKConstrActionType::CASCADE, parser::FKConstrMatchType::FULL);
  Operator op8 = LogicalCreateTable::Make(catalog::namespace_oid_t(1), "Table_1",
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{
                                              common::ManagedPointer<parser::ColumnDefinition>(col_def)},
                                          std::vector<common::ManagedPointer<parser::ColumnDefinition>>{
                                              common::ManagedPointer<parser::ColumnDefinition>(foreign_def)})
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op8);
  EXPECT_NE(op1.Hash(), op8.Hash());
  EXPECT_EQ(op8.GetContentsAs<LogicalCreateTable>()->GetForeignKeys().size(), 1);
  EXPECT_EQ(*op8.GetContentsAs<LogicalCreateTable>()->GetForeignKeys().at(0), *foreign_def);

  delete col_def->GetDefaultExpression().Get();
  delete col_def;
  delete col_def_2->GetDefaultExpression().Get();
  delete col_def_2;
  delete foreign_def;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalCreateNamespaceTest) {
  //===--------------------------------------------------------------------===//
  // LogicalCreateNamespace
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  Operator op1 = LogicalCreateNamespace::Make("testns").RegisterWithTxnContext(txn_context);
  Operator op2 = LogicalCreateNamespace::Make("testns").RegisterWithTxnContext(txn_context);
  Operator op3 = LogicalCreateNamespace::Make("another_testns").RegisterWithTxnContext(txn_context);

  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALCREATENAMESPACE);
  EXPECT_EQ(op3.GetOpType(), OpType::LOGICALCREATENAMESPACE);
  EXPECT_EQ(op1.GetName(), "LogicalCreateNamespace");
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateNamespace>()->GetNamespaceName(), "testns");
  EXPECT_EQ(op3.GetContentsAs<LogicalCreateNamespace>()->GetNamespaceName(), "another_testns");
  EXPECT_TRUE(op1 == op2);
  EXPECT_FALSE(op1 == op3);
  EXPECT_EQ(op1.Hash(), op2.Hash());
  EXPECT_NE(op1.Hash(), op3.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalCreateTriggerTest) {
  //===--------------------------------------------------------------------===//
  // LogicalCreateTrigger
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  auto when = new parser::ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(1));
  Operator op1 = LogicalCreateTrigger::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                            "Trigger_1", {}, {}, {catalog::col_oid_t(1)},
                                            common::ManagedPointer<parser::AbstractExpression>(when), 0)
                     .RegisterWithTxnContext(txn_context);

  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALCREATETRIGGER);
  EXPECT_EQ(op1.GetName(), "LogicalCreateTrigger");
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateTrigger>()->GetTriggerName(), "Trigger_1");
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateTrigger>()->GetNamespaceOid(), catalog::namespace_oid_t(1));
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateTrigger>()->GetTableOid(), catalog::table_oid_t(1));
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateTrigger>()->GetDatabaseOid(), catalog::db_oid_t(1));
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateTrigger>()->GetTriggerType(), 0);
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateTrigger>()->GetTriggerFuncName(), std::vector<std::string>{});
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateTrigger>()->GetTriggerArgs(), std::vector<std::string>{});
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateTrigger>()->GetTriggerColumns(),
            std::vector<catalog::col_oid_t>{catalog::col_oid_t(1)});

  Operator op2 = LogicalCreateTrigger::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                            "Trigger_1", {}, {}, {catalog::col_oid_t(1)},
                                            common::ManagedPointer<parser::AbstractExpression>(when), 0)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  Operator op3 = LogicalCreateTrigger::Make(catalog::db_oid_t(2), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                            "Trigger_1", {}, {}, {catalog::col_oid_t(1)},
                                            common::ManagedPointer<parser::AbstractExpression>(when), 0)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op3 == op1);
  EXPECT_NE(op1.Hash(), op3.Hash());

  Operator op4 = LogicalCreateTrigger::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), catalog::table_oid_t(1),
                                            "Trigger_1", {}, {}, {catalog::col_oid_t(1)},
                                            common::ManagedPointer<parser::AbstractExpression>(when), 0)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op4);
  EXPECT_NE(op4.Hash(), op3.Hash());

  Operator op5 = LogicalCreateTrigger::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(2),
                                            "Trigger_1", {}, {}, {catalog::col_oid_t(1)},
                                            common::ManagedPointer<parser::AbstractExpression>(when), 0)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op5);
  EXPECT_NE(op1.Hash(), op5.Hash());

  Operator op6 = LogicalCreateTrigger::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                            "Trigger_2", {}, {}, {catalog::col_oid_t(1)},
                                            common::ManagedPointer<parser::AbstractExpression>(when), 0)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op6);
  EXPECT_NE(op1.Hash(), op6.Hash());

  Operator op7 = LogicalCreateTrigger::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                            "Trigger_1", {"func_name"}, {"func_arg"}, {catalog::col_oid_t(1)},
                                            common::ManagedPointer<parser::AbstractExpression>(when), 0)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_EQ(op7.GetContentsAs<LogicalCreateTrigger>()->GetTriggerFuncName(), std::vector<std::string>{"func_name"});
  EXPECT_EQ(op7.GetContentsAs<LogicalCreateTrigger>()->GetTriggerArgs(), std::vector<std::string>{"func_arg"});
  EXPECT_FALSE(op1 == op7);
  EXPECT_NE(op1.Hash(), op7.Hash());

  Operator op8 = LogicalCreateTrigger::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                            "Trigger_1", {"func_name"}, {"func_arg"}, {catalog::col_oid_t(1)},
                                            common::ManagedPointer<parser::AbstractExpression>(when), 0)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op7 == op8);
  EXPECT_EQ(op7.Hash(), op8.Hash());

  Operator op9 =
      LogicalCreateTrigger::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                 "Trigger_1", {"func_name", "func_name"}, {"func_arg", "func_arg"},
                                 {catalog::col_oid_t(1)}, common::ManagedPointer<parser::AbstractExpression>(when), 0)
          .RegisterWithTxnContext(txn_context);
  EXPECT_EQ(op9.GetContentsAs<LogicalCreateTrigger>()->GetTriggerFuncName(),
            std::vector<std::string>({"func_name", "func_name"}));
  EXPECT_EQ(op9.GetContentsAs<LogicalCreateTrigger>()->GetTriggerArgs(),
            std::vector<std::string>({"func_arg", "func_arg"}));
  EXPECT_FALSE(op1 == op9);
  EXPECT_FALSE(op7 == op9);
  EXPECT_NE(op1.Hash(), op9.Hash());
  EXPECT_NE(op7.Hash(), op9.Hash());

  Operator op10 =
      LogicalCreateTrigger::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                 "Trigger_1", {}, {}, {}, common::ManagedPointer<parser::AbstractExpression>(when), 0)
          .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op10 == op1);
  EXPECT_NE(op1.Hash(), op10.Hash());

  auto when_2 = new parser::ConstantValueExpression(type::TypeId::TINYINT, execution::sql::Integer(2));
  Operator op11 = LogicalCreateTrigger::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                             "Trigger_1", {}, {}, {catalog::col_oid_t(1)},
                                             common::ManagedPointer<parser::AbstractExpression>(when_2), 0)
                      .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op11 == op1);
  EXPECT_NE(op1.Hash(), op11.Hash());

  Operator op12 = LogicalCreateTrigger::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), catalog::table_oid_t(1),
                                             "Trigger_1", {}, {}, {catalog::col_oid_t(1)},
                                             common::ManagedPointer<parser::AbstractExpression>(when), 9)
                      .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op12 == op1);
  EXPECT_NE(op1.Hash(), op12.Hash());

  delete when;
  delete when_2;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalCreateViewTest) {
  //===--------------------------------------------------------------------===//
  // LogicalCreateView
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  Operator op1 = LogicalCreateView::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "test_view", nullptr)
                     .RegisterWithTxnContext(txn_context);

  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALCREATEVIEW);
  EXPECT_EQ(op1.GetName(), "LogicalCreateView");
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateView>()->GetViewName(), "test_view");
  EXPECT_EQ(op1.GetContentsAs<LogicalCreateView>()->GetViewQuery(), nullptr);

  Operator op2 = LogicalCreateView::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "test_view", nullptr)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  Operator op3 = LogicalCreateView::Make(catalog::db_oid_t(2), catalog::namespace_oid_t(1), "test_view", nullptr)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  Operator op4 = LogicalCreateView::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(2), "test_view", nullptr)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op4);
  EXPECT_NE(op1.Hash(), op4.Hash());

  Operator op5 = LogicalCreateView::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "test_view_2", nullptr)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op5);
  EXPECT_NE(op1.Hash(), op5.Hash());

  auto stmt = new parser::SelectStatement(std::vector<common::ManagedPointer<parser::AbstractExpression>>{}, true,
                                          nullptr, nullptr, nullptr, nullptr, nullptr);
  Operator op6 = LogicalCreateView::Make(catalog::db_oid_t(1), catalog::namespace_oid_t(1), "test_view",
                                         common::ManagedPointer<parser::SelectStatement>(stmt))
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op6);
  EXPECT_NE(op1.Hash(), op6.Hash());
  delete stmt;

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalDropDatabaseTest) {
  //===--------------------------------------------------------------------===//
  // LogicalDropDatabase
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  Operator op1 = LogicalDropDatabase::Make(catalog::db_oid_t(1)).RegisterWithTxnContext(txn_context);
  Operator op2 = LogicalDropDatabase::Make(catalog::db_oid_t(1)).RegisterWithTxnContext(txn_context);
  Operator op3 = LogicalDropDatabase::Make(catalog::db_oid_t(2)).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALDROPDATABASE);
  EXPECT_EQ(op3.GetOpType(), OpType::LOGICALDROPDATABASE);
  EXPECT_EQ(op1.GetName(), "LogicalDropDatabase");
  EXPECT_EQ(op1.GetContentsAs<LogicalDropDatabase>()->GetDatabaseOID(), catalog::db_oid_t(1));
  EXPECT_EQ(op3.GetContentsAs<LogicalDropDatabase>()->GetDatabaseOID(), catalog::db_oid_t(2));
  EXPECT_TRUE(op1 == op2);
  EXPECT_FALSE(op1 == op3);
  EXPECT_EQ(op1.Hash(), op2.Hash());
  EXPECT_NE(op1.Hash(), op3.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalDropTableTest) {
  //===--------------------------------------------------------------------===//
  // LogicalDropTable
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  Operator op1 = LogicalDropTable::Make(catalog::table_oid_t(1)).RegisterWithTxnContext(txn_context);
  Operator op2 = LogicalDropTable::Make(catalog::table_oid_t(1)).RegisterWithTxnContext(txn_context);
  Operator op3 = LogicalDropTable::Make(catalog::table_oid_t(2)).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALDROPTABLE);
  EXPECT_EQ(op3.GetOpType(), OpType::LOGICALDROPTABLE);
  EXPECT_EQ(op1.GetName(), "LogicalDropTable");
  EXPECT_EQ(op1.GetContentsAs<LogicalDropTable>()->GetTableOID(), catalog::table_oid_t(1));
  EXPECT_EQ(op3.GetContentsAs<LogicalDropTable>()->GetTableOID(), catalog::table_oid_t(2));
  EXPECT_TRUE(op1 == op2);
  EXPECT_FALSE(op1 == op3);
  EXPECT_EQ(op1.Hash(), op2.Hash());
  EXPECT_NE(op1.Hash(), op3.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalDropIndexTest) {
  //===--------------------------------------------------------------------===//
  // LogicalDropIndex
  //===--------------------------------------------------------------------===//
  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  Operator op1 = LogicalDropIndex::Make(catalog::index_oid_t(1)).RegisterWithTxnContext(txn_context);
  Operator op2 = LogicalDropIndex::Make(catalog::index_oid_t(1)).RegisterWithTxnContext(txn_context);
  Operator op3 = LogicalDropIndex::Make(catalog::index_oid_t(2)).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALDROPINDEX);
  EXPECT_EQ(op3.GetOpType(), OpType::LOGICALDROPINDEX);
  EXPECT_EQ(op1.GetName(), "LogicalDropIndex");
  EXPECT_EQ(op1.GetContentsAs<LogicalDropIndex>()->GetIndexOID(), catalog::index_oid_t(1));
  EXPECT_EQ(op3.GetContentsAs<LogicalDropIndex>()->GetIndexOID(), catalog::index_oid_t(2));
  EXPECT_TRUE(op1 == op2);
  EXPECT_FALSE(op1 == op3);
  EXPECT_EQ(op1.Hash(), op2.Hash());
  EXPECT_NE(op1.Hash(), op3.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalDropNamespaceTest) {
  //===--------------------------------------------------------------------===//
  // LogicalDropNamespace
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  Operator op1 = LogicalDropNamespace::Make(catalog::namespace_oid_t(1)).RegisterWithTxnContext(txn_context);
  Operator op2 = LogicalDropNamespace::Make(catalog::namespace_oid_t(1)).RegisterWithTxnContext(txn_context);
  Operator op3 = LogicalDropNamespace::Make(catalog::namespace_oid_t(2)).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALDROPNAMESPACE);
  EXPECT_EQ(op3.GetOpType(), OpType::LOGICALDROPNAMESPACE);
  EXPECT_EQ(op1.GetName(), "LogicalDropNamespace");
  EXPECT_EQ(op1.GetContentsAs<LogicalDropNamespace>()->GetNamespaceOID(), catalog::namespace_oid_t(1));
  EXPECT_EQ(op3.GetContentsAs<LogicalDropNamespace>()->GetNamespaceOID(), catalog::namespace_oid_t(2));
  EXPECT_TRUE(op1 == op2);
  EXPECT_FALSE(op1 == op3);
  EXPECT_EQ(op1.Hash(), op2.Hash());
  EXPECT_NE(op1.Hash(), op3.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalDropTriggerTest) {
  //===--------------------------------------------------------------------===//
  // LogicalDropTrigger
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  Operator op1 = LogicalDropTrigger::Make(catalog::db_oid_t(1), catalog::trigger_oid_t(1), false)
                     .RegisterWithTxnContext(txn_context);

  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALDROPTRIGGER);
  EXPECT_EQ(op1.GetName(), "LogicalDropTrigger");
  EXPECT_EQ(op1.GetContentsAs<LogicalDropTrigger>()->GetDatabaseOid(), catalog::db_oid_t(1));
  EXPECT_EQ(op1.GetContentsAs<LogicalDropTrigger>()->GetTriggerOid(), catalog::trigger_oid_t(1));
  EXPECT_FALSE(op1.GetContentsAs<LogicalDropTrigger>()->IsIfExists());

  Operator op2 = LogicalDropTrigger::Make(catalog::db_oid_t(1), catalog::trigger_oid_t(1), false)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  Operator op3 = LogicalDropTrigger::Make(catalog::db_oid_t(2), catalog::trigger_oid_t(1), false)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 != op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  Operator op5 = LogicalDropTrigger::Make(catalog::db_oid_t(1), catalog::trigger_oid_t(2), false)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op5);
  EXPECT_NE(op1.Hash(), op5.Hash());

  Operator op6 = LogicalDropTrigger::Make(catalog::db_oid_t(1), catalog::trigger_oid_t(1), true)
                     .RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op6);
  EXPECT_NE(op1.Hash(), op6.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
}

// NOLINTNEXTLINE
TEST(OperatorTests, LogicalDropViewTest) {
  //===--------------------------------------------------------------------===//
  // LogicalDropView
  //===--------------------------------------------------------------------===//

  // Due to the deferred action framework being used to manage memory, we need to
  // simulate a transaction to prevent leaks
  auto timestamp_manager = transaction::TimestampManager();
  auto deferred_action_manager = transaction::DeferredActionManager(common::ManagedPointer(&timestamp_manager));
  auto buffer_pool = storage::RecordBufferSegmentPool(100, 2);
  transaction::TransactionManager txn_manager = transaction::TransactionManager(
      common::ManagedPointer(&timestamp_manager), common::ManagedPointer(&deferred_action_manager),
      common::ManagedPointer(&buffer_pool), false, nullptr);

  transaction::TransactionContext *txn_context = txn_manager.BeginTransaction();

  Operator op1 =
      LogicalDropView::Make(catalog::db_oid_t(1), catalog::view_oid_t(1), false).RegisterWithTxnContext(txn_context);

  EXPECT_EQ(op1.GetOpType(), OpType::LOGICALDROPVIEW);
  EXPECT_EQ(op1.GetName(), "LogicalDropView");
  EXPECT_EQ(op1.GetContentsAs<LogicalDropView>()->GetDatabaseOid(), catalog::db_oid_t(1));
  EXPECT_EQ(op1.GetContentsAs<LogicalDropView>()->GetViewOid(), catalog::view_oid_t(1));
  EXPECT_FALSE(op1.GetContentsAs<LogicalDropView>()->IsIfExists());

  Operator op2 =
      LogicalDropView::Make(catalog::db_oid_t(1), catalog::view_oid_t(1), false).RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 == op2);
  EXPECT_EQ(op1.Hash(), op2.Hash());

  Operator op3 =
      LogicalDropView::Make(catalog::db_oid_t(2), catalog::view_oid_t(1), false).RegisterWithTxnContext(txn_context);
  EXPECT_TRUE(op1 != op3);
  EXPECT_NE(op1.Hash(), op3.Hash());

  Operator op5 =
      LogicalDropView::Make(catalog::db_oid_t(1), catalog::view_oid_t(2), false).RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op5);
  EXPECT_NE(op1.Hash(), op5.Hash());

  Operator op6 =
      LogicalDropView::Make(catalog::db_oid_t(1), catalog::view_oid_t(1), true).RegisterWithTxnContext(txn_context);
  EXPECT_FALSE(op1 == op6);
  EXPECT_NE(op1.Hash(), op6.Hash());

  txn_manager.Abort(txn_context);
  delete txn_context;
}

}  // namespace noisepage::optimizer
