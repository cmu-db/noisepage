#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/managed_pointer.h"

namespace noisepage::parser {

class AbstractExpression;
class SQLStatement;

/**
 * ParseResult is the parser's output to the binder. It allows you to obtain non-owning managed pointers to the
 * statements and expressions that were generated during the parse. If you need to take ownership, you can do that
 * too, but then the parse result's copy is invalidated.
 */
class ParseResult {
 public:
  /**
   * @return true if no statements exist
   */
  bool Empty() const { return statements_.empty(); }

  /**
   * Adds a statement to this parse result.
   */
  void AddStatement(std::unique_ptr<SQLStatement> statement) { statements_.emplace_back(std::move(statement)); }

  /**
   * Adds an expression to this parse result.
   */
  void AddExpression(std::unique_ptr<AbstractExpression> expression) {
    expressions_.emplace_back(std::move(expression));
  }

  /**
   * @return non-owning list of all the statements contained in this parse result
   */
  std::vector<common::ManagedPointer<SQLStatement>> GetStatements() {
    std::vector<common::ManagedPointer<SQLStatement>> statements;
    statements.reserve(statements_.size());
    for (const auto &statement : statements_) {
      statements.emplace_back(common::ManagedPointer(statement));
    }
    return statements;
  }

  /**
   * @return size of internal statements_ vector
   */
  uint32_t NumStatements() const { return statements_.size(); }

  /**
   * @return the statement at a particular index
   */
  common::ManagedPointer<SQLStatement> GetStatement(size_t idx) { return common::ManagedPointer(statements_[idx]); }

  /**
   * @return non-owning list of all the expressions contained in this parse result
   */
  std::vector<common::ManagedPointer<AbstractExpression>> GetExpressions() {
    std::vector<common::ManagedPointer<AbstractExpression>> expressions;
    expressions.reserve(expressions_.size());
    for (const auto &statement : expressions_) {
      expressions.emplace_back(common::ManagedPointer(statement));
    }
    return expressions;
  }

  /**
   * @return the expression at a particular index
   */
  common::ManagedPointer<AbstractExpression> GetExpression(size_t idx) {
    return common::ManagedPointer(expressions_[idx]);
  }

  /**
   * Returns ownership of the statements in this parse result.
   * @return moved statements
   */
  std::vector<std::unique_ptr<SQLStatement>> &&TakeStatementsOwnership() { return std::move(statements_); }

  /**
   * Returns ownership of the expressions in this parse result.
   * @return moved expressions
   */
  std::vector<std::unique_ptr<AbstractExpression>> &&TakeExpressionsOwnership() { return std::move(expressions_); }

 private:
  std::vector<std::unique_ptr<SQLStatement>> statements_;
  std::vector<std::unique_ptr<AbstractExpression>> expressions_;
};

}  // namespace noisepage::parser
