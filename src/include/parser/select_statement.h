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
  /**
   * @param types order by types
   * @param exprs order by expressions
   */
  OrderByDescription(std::vector<OrderType> types, std::vector<std::shared_ptr<AbstractExpression>> exprs)
      : types_(std::move(types)), exprs_(std::move(exprs)) {}

  virtual ~OrderByDescription() = default;

  // TODO(WAN): no SQLStatement? maybe a Description base class?
  /**
   * @param v visitor
   */
  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  /**
   * @return order by types
   */
  std::vector<OrderType> GetOrderByTypes() { return types_; }

  /**
   * @return order by expressions
   */
  std::vector<std::shared_ptr<AbstractExpression>> GetOrderByExpressions() { return exprs_; }

 private:
  const std::vector<OrderType> types_;
  const std::vector<std::shared_ptr<AbstractExpression>> exprs_;
};

/**
 * Describes the limit clause in a SELECT statement.
 */
class LimitDescription {
 public:
  /**
   * Denotes that there is no limit.
   */
  static constexpr int NO_LIMIT = -1;
  /**
   * Denotes that there is no offset.
   */
  static constexpr int NO_OFFSET = -1;

  /**
   * @param limit limit
   * @param offset offset
   */
  LimitDescription(int64_t limit, int64_t offset) : limit_(limit), offset_(offset) {}

  ~LimitDescription() = default;

  // TODO(WAN): not SQL statement?
  /**
   * @param v visitor
   */
  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  /**
   * @return limit
   */
  int64_t GetLimit() { return limit_; }

  /**
   * @return offset
   */
  int64_t GetOffset() { return offset_; }

 private:
  int64_t limit_;
  int64_t offset_;
};

/**
 * Represents the sql "GROUP BY".
 */
class GroupByDescription {
 public:
  /**
   * @param columns group by columns
   * @param having having clause
   */
  GroupByDescription(std::vector<std::shared_ptr<AbstractExpression>> columns,
                     std::shared_ptr<AbstractExpression> having)
      : columns_(std::move(columns)), having_(std::move(having)) {}

  // TODO(WAN): not a SQLStatement?
  /**
   * Visitor pattern for GroupByDescription.
   * @param v visitor
   */
  void Accept(SqlNodeVisitor *v) { v->Visit(this); }

  /**
   * @return group by columns
   */
  std::vector<std::shared_ptr<AbstractExpression>> GetColumns() { return columns_; }

  /**
   * @return having clause
   */
  std::shared_ptr<AbstractExpression> GetHaving() { return having_; }

 private:
  const std::vector<std::shared_ptr<AbstractExpression>> columns_;
  const std::shared_ptr<AbstractExpression> having_;
};

/**
 * Represents the sql "SELECT ..."
 */
class SelectStatement : public SQLStatement {
  /*
   * TODO(WAN): old system says, add union_order and union_limit
   */
 public:
  /**
   * @param select columns being selected
   * @param select_distinct true if "SELECT DISTINCT" was used
   * @param from table to select from
   * @param where select condition
   * @param group_by group by condition
   * @param order_by order by condition
   * @param limit limit condition
   */
  SelectStatement(std::vector<std::shared_ptr<AbstractExpression>> select, const bool &select_distinct,
                  std::shared_ptr<TableRef> from, std::shared_ptr<AbstractExpression> where,
                  std::shared_ptr<GroupByDescription> group_by, std::shared_ptr<OrderByDescription> order_by,
                  std::shared_ptr<LimitDescription> limit)
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

  /**
   * @return select columns
   */
  std::vector<std::shared_ptr<AbstractExpression>> GetSelectColumns() { return select_; }

  /**
   * @return true if "SELECT DISTINCT", false otherwise
   */
  bool IsSelectDistinct() { return select_distinct_; }

  /**
   * @return table being selected from
   */
  std::shared_ptr<TableRef> GetSelectTable() { return from_; }

  /**
   * @return select condition
   */
  std::shared_ptr<AbstractExpression> GetSelectCondition() { return where_; }

  /**
   * @return select group by
   */
  std::shared_ptr<GroupByDescription> GetSelectGroupBy() { return group_by_; }

  /**
   * @return select order by
   */
  std::shared_ptr<OrderByDescription> GetSelectOrderBy() { return order_by_; }

  /**
   * @return select limit
   */
  std::shared_ptr<LimitDescription> GetSelectLimit() { return limit_; }

  /**
   * Adds a select statement child as a union target.
   * @param select_stmt select statement to union with
   */
  void SetUnionSelect(std::shared_ptr<SelectStatement> select_stmt) { union_select_ = std::move(select_stmt); }

 private:
  const std::vector<std::shared_ptr<AbstractExpression>> select_;
  const bool select_distinct_;
  const std::shared_ptr<TableRef> from_;
  const std::shared_ptr<AbstractExpression> where_;
  const std::shared_ptr<GroupByDescription> group_by_;
  const std::shared_ptr<OrderByDescription> order_by_;
  const std::shared_ptr<LimitDescription> limit_;

  std::shared_ptr<SelectStatement> union_select_;
};

}  // namespace parser
}  // namespace terrier
