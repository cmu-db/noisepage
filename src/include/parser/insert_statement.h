#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "common/sql_node_visitor.h"
#include "parser/parser_defs.h"
#include "parser/select_statement.h"
#include "parser/sql_statement.h"

namespace terrier {
namespace parser {
/**
 * InsertStatement represents the sql "INSERT ..."
 */
class InsertStatement : public SQLStatement {
 public:
  /**
   * Insert from SELECT
   * @param columns columns to insert into
   * @param table_ref table
   * @param select select statement to insert from
   */
  InsertStatement(std::unique_ptr<std::vector<std::string>> columns, std::unique_ptr<TableRef> table_ref,
                  std::unique_ptr<SelectStatement> select)
      : SQLStatement(StatementType::INSERT),
        type_(InsertType::SELECT),
        columns_(std::move(columns)),
        table_(std::move(table_ref)),
        select_(std::move(select)) {}

  /**
   * Insert from VALUES
   * @param columns columns to insert into
   * @param table_ref table
   * @param insert_values values to be inserted
   */
  InsertStatement(std::unique_ptr<std::vector<std::string>> columns, std::unique_ptr<TableRef> table_ref,
                  std::unique_ptr<std::vector<std::vector<common::ManagedPointer<AbstractExpression>>>> insert_values)
      : SQLStatement(StatementType::INSERT),
        type_(InsertType::VALUES),
        columns_(std::move(columns)),
        table_(std::move(table_ref)),
        insert_values_(std::move(insert_values)) {}

  /** @param type insert type (SELECT or VALUES) */
  explicit InsertStatement(InsertType type) : SQLStatement(StatementType::INSERT), type_(type) {}

  ~InsertStatement() override = default;

  /**
   * @return the hashed value of this InsertStatement
   */
  common::hash_t Hash() const override {
    common::hash_t hash = SQLStatement::Hash();
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(type_));
    if (columns_ != nullptr) {
      for (auto &column : *columns_) {
        hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(column));
      }
    }
    if (table_ != nullptr) hash = common::HashUtil::CombineHashes(hash, table_->Hash());
    if (select_ != nullptr) hash = common::HashUtil::CombineHashes(hash, select_->Hash());
    if (insert_values_ != nullptr) {
      for (size_t i = 0; i < insert_values_->size(); i++) {
        for (size_t j = 0; j < (*insert_values_)[i].size(); j++) {
          hash = common::HashUtil::CombineHashes(hash, (*insert_values_)[i][j]->Hash());
        }
      }
    }
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two InsertStatements are logically equal
   */
  bool operator==(const SQLStatement &rhs) const override {
    if (!SQLStatement::operator==(rhs)) return false;
    auto const &insert_stmt = dynamic_cast<const InsertStatement &>(rhs);

    if (type_ != insert_stmt.type_) return false;

    if (columns_ != nullptr && insert_stmt.columns_ == nullptr) return false;
    if (columns_ == nullptr && insert_stmt.columns_ != nullptr) return false;
    if (columns_ != nullptr && insert_stmt.columns_ != nullptr) {
      if (columns_->size() != insert_stmt.columns_->size()) return false;
      for (size_t i = 0; i < columns_->size(); i++)
        if ((*columns_)[i] != (*insert_stmt.columns_)[i]) return false;
    }

    if (table_ != nullptr && insert_stmt.table_ == nullptr) return false;
    if (table_ == nullptr && insert_stmt.table_ != nullptr) return false;
    if (table_ != nullptr && insert_stmt.table_ != nullptr && *(table_) != *(insert_stmt.table_)) return false;

    if (select_ != nullptr && insert_stmt.select_ == nullptr) return false;
    if (select_ == nullptr && insert_stmt.select_ != nullptr) return false;
    if (select_ != nullptr && insert_stmt.select_ != nullptr && *(select_) != *(insert_stmt.select_)) return false;

    if (insert_values_ != nullptr && insert_stmt.insert_values_ == nullptr) return false;
    if (insert_values_ == nullptr && insert_stmt.insert_values_ != nullptr) return false;
    if (insert_values_ != nullptr && insert_stmt.insert_values_ != nullptr) {
      if (insert_values_->size() != insert_stmt.insert_values_->size()) return false;
      for (size_t i = 0; i < insert_values_->size(); i++) {
        if ((*insert_values_)[i].size() != (*insert_stmt.insert_values_)[i].size()) return false;
        for (size_t j = 0; j < (*insert_values_)[i].size(); j++) {
          if ((*insert_values_)[i][j] != (*insert_stmt.insert_values_)[i][j]) return false;
        }
      }
    }
    return true;
  }

  void Accept(SqlNodeVisitor *v, ParseResult *parse_result) override { v->Visit(this, parse_result); }

  /** @return type of insertion */
  InsertType GetInsertType() { return type_; }

  /** @return columns to insert into */
  common::ManagedPointer<std::vector<std::string>> GetInsertColumns() { return common::ManagedPointer(columns_); }

  /** @return table to insert into */
  common::ManagedPointer<TableRef> GetInsertionTable() const { return common::ManagedPointer(table_); }

  /** @return select statement we're inserting from */
  common::ManagedPointer<SelectStatement> GetSelect() const { return common::ManagedPointer(select_); }

  /** @return values that we're inserting */
  common::ManagedPointer<std::vector<std::vector<common::ManagedPointer<AbstractExpression>>>> GetValues() {
    return common::ManagedPointer(insert_values_);
  }

 private:
  const InsertType type_;
  const std::unique_ptr<std::vector<std::string>> columns_;
  const std::unique_ptr<TableRef> table_;
  const std::unique_ptr<SelectStatement> select_;
  const std::unique_ptr<std::vector<std::vector<common::ManagedPointer<AbstractExpression>>>> insert_values_;
};

}  // namespace parser
}  // namespace terrier
