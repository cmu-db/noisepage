#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/sql_node_visitor.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace terrier {
namespace parser {

enum OrderType { kOrderAsc, kOrderDesc };
using terrier::parser::OrderType;

/**
 * Describes OrderBy clause in a select statement.
 */
class OrderByDescription {
  // TODO(WAN): hold multiple expressions to be sorted by

 public:
  OrderByDescription(std::vector<OrderType> types, std::vector<std::unique_ptr<AbstractExpression>> exprs)
      : types_(std::move(types)), exprs_(std::move(exprs)) {}

  virtual ~OrderByDescription() = default;

  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  const std::vector<OrderType> types_;
  const std::vector<std::unique_ptr<AbstractExpression>> exprs_;
};

/**
 * Describes the limit clause in a SELECT statement.
 */
class LimitDescription {
 public:
  static constexpr int NO_LIMIT = -1;
  static constexpr int NO_OFFSET = -1;

  LimitDescription(int64_t limit, int64_t offset) : limit_(limit), offset_(offset) {}

  ~LimitDescription() = default;

  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  int64_t GetLimit() { return limit_; }
  int64_t GetOffset() { return offset_; }

 private:
  int64_t limit_;
  int64_t offset_;
};

class GroupByDescription {
 public:
  GroupByDescription(std::vector<std::unique_ptr<AbstractExpression>> columns,
                     std::unique_ptr<AbstractExpression> having)
      : columns_(std::move(columns)), having_(std::move(having)) {}

  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  const std::vector<std::unique_ptr<AbstractExpression>> columns_;
  const std::unique_ptr<AbstractExpression> having_;
};

/**
 * Represents the sql "SELECT ..."
 */
class SelectStatement : public SQLStatement {
  /*
   * TODO(WAN): old system says, add union_order and union_limit
   */
 public:
  SelectStatement(std::vector<std::unique_ptr<AbstractExpression>> select, const bool &select_distinct,
                  std::unique_ptr<TableRef> from, std::unique_ptr<AbstractExpression> where,
                  std::unique_ptr<GroupByDescription> group_by, std::unique_ptr<OrderByDescription> order_by,
                  std::unique_ptr<LimitDescription> limit)
      : SQLStatement(StatementType::SELECT),
        select_(std::move(select)),
        select_distinct_(select_distinct),
        from_(std::move(from)),
        where_(std::move(where)),
        group_by_(std::move(group_by)),
        order_by_(std::move(order_by)),
        limit_(std::move(limit)),
        union_select_(nullptr) {}

  ~SelectStatement() override = default;

  void Accept(SqlNodeVisitor *v) override { v->Visit(this); }

  const std::vector<std::unique_ptr<AbstractExpression>> select_;
  const bool select_distinct_;
  const std::unique_ptr<TableRef> from_;
  const std::unique_ptr<AbstractExpression> where_;
  const std::unique_ptr<GroupByDescription> group_by_;
  const std::unique_ptr<OrderByDescription> order_by_;
  const std::unique_ptr<LimitDescription> limit_;

  std::unique_ptr<SelectStatement> union_select_;
};

}  // namespace parser
}  // namespace terrier
