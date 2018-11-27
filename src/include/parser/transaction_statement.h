#pragma once

#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"

namespace terrier::parser {

/**
 * @class TransactionStatement
 * @brief Represents "BEGIN or COMMIT or ROLLBACK [TRANSACTION]"
 */
class TransactionStatement : public SQLStatement {
 public:
  /**
   * Command used in the transaction.
   */
  enum CommandType {
    kBegin,
    kCommit,
    kRollback,
  };

  /**
   * @param type transaction command
   */
  explicit TransactionStatement(CommandType type) : SQLStatement(StatementType::TRANSACTION), type_(type) {}

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  /**
   * @return transaction command
   */
  CommandType GetTransactionType() { return type_; }

 private:
  const CommandType type_;
};

}  // namespace terrier::parser
