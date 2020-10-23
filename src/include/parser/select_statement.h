#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "common/json_header.h"
#include "parser/sql_statement.h"
#include "parser/table_ref.h"

namespace noisepage {

namespace binder {
class BindNodeVisitor;
}  // namespace binder

namespace parser {

enum OrderType { kOrderAsc, kOrderDesc };
using noisepage::parser::OrderType;

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
  OrderByDescription(std::vector<OrderType> types, std::vector<common::ManagedPointer<AbstractExpression>> exprs)
      : types_(std::move(types)), exprs_(std::move(exprs)) {}

  /**
   * Default constructor for deserialization
   */
  OrderByDescription() = default;

  /**
   * @return a copy of the order by description
   */
  std::unique_ptr<OrderByDescription> Copy() {
    std::vector<OrderType> types;
    types.reserve(types_.size());
    for (const auto &type : types_) {
      types.emplace_back(type);
    }
    std::vector<common::ManagedPointer<AbstractExpression>> exprs;
    exprs.reserve(exprs_.size());
    for (const auto &expr : exprs_) {
      exprs.emplace_back(expr);
    }
    return std::make_unique<OrderByDescription>(std::move(types), std::move(exprs));
  }

  // TODO(WAN): no SQLStatement? maybe a Description base class?
  /**
   * @param v Visitor pattern for the statement
   */
  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) { v->Visit(common::ManagedPointer(this)); }

  /**
   * @return order by types
   */
  std::vector<OrderType> GetOrderByTypes() { return types_; }

  /**
   * @return number of order by expressions
   */
  size_t GetOrderByExpressionsSize() const { return exprs_.size(); }

  /**
   * @return order by expression
   */
  std::vector<common::ManagedPointer<AbstractExpression>> &GetOrderByExpressions() { return exprs_; }

  /**
   * @return the hashed value of this Order by description
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(types_.size());
    hash = common::HashUtil::CombineHashInRange(hash, types_.begin(), types_.end());
    for (const auto &expr : exprs_) {
      hash = common::HashUtil::CombineHashes(hash, expr->Hash());
    }
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two OrderByDescriptions are logically equal
   */
  bool operator==(const OrderByDescription &rhs) const {
    if (types_.size() != rhs.types_.size()) return false;
    for (size_t i = 0; i < types_.size(); i++)
      if (types_[i] != rhs.types_[i]) return false;
    if (exprs_.size() != rhs.exprs_.size()) return false;
    for (size_t i = 0; i < exprs_.size(); i++)
      if (*(exprs_[i]) != *(rhs.exprs_[i])) return false;
    return true;
  }

  /**
   * Logical inequality check.
   * @param rhs other
   * @return true if the two OrderByDescriptions are logically unequal
   */
  bool operator!=(const OrderByDescription &rhs) const { return !(operator==(rhs)); }

  /**
   * @return OrderByDescription serialized to json
   */
  nlohmann::json ToJson() const;

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j);

 private:
  std::vector<OrderType> types_;
  std::vector<common::ManagedPointer<AbstractExpression>> exprs_;
};

DEFINE_JSON_HEADER_DECLARATIONS(OrderByDescription);

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

  /**
   * Default constructor for deserialization
   */
  LimitDescription() = default;

  ~LimitDescription() = default;

  /**
   * @return a copy of the limit description
   */
  std::unique_ptr<LimitDescription> Copy() { return std::make_unique<LimitDescription>(limit_, offset_); }

  // TODO(WAN): not SQL statement?
  /**
   * @param v Visitor pattern for the statement
   */
  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) { v->Visit(common::ManagedPointer(this)); }

  /**
   * @return limit
   */
  int64_t GetLimit() { return limit_; }

  /**
   * @return offset
   */
  int64_t GetOffset() { return offset_; }

  /**
   * @return the hashed value of this Limit description
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(limit_);
    hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(offset_));
    return hash;
  }
  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two GroupByDescriptions are logically equal
   */
  bool operator==(const LimitDescription &rhs) const {
    if (limit_ != rhs.limit_) return false;
    return offset_ == rhs.offset_;
  }

  /**
   * Logical inequality check.
   * @param rhs other
   * @return true if the two LimitDescription are logically unequal
   */
  bool operator!=(const LimitDescription &rhs) const { return !(operator==(rhs)); }

  /**
   * @return LimitDescription serialized to json
   */
  nlohmann::json ToJson() const;

  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j);

 private:
  int64_t limit_;
  int64_t offset_;
};

DEFINE_JSON_HEADER_DECLARATIONS(LimitDescription);

/**
 * Represents the sql "GROUP BY".
 */
class GroupByDescription {
 public:
  /**
   * @param columns group by columns
   * @param having having clause
   */
  GroupByDescription(std::vector<common::ManagedPointer<AbstractExpression>> columns,
                     common::ManagedPointer<AbstractExpression> having)
      : columns_(std::move(columns)), having_(having) {}

  /**
   * Default constructor for deserialization
   */
  GroupByDescription() = default;

  /**
   * @return a copy of the group by description
   */
  std::unique_ptr<GroupByDescription> Copy() {
    std::vector<common::ManagedPointer<AbstractExpression>> columns;
    columns.reserve(columns.size());
    for (const auto &col : columns_) {
      columns.emplace_back(col);
    }
    auto having = having_;
    return std::make_unique<GroupByDescription>(std::move(columns), having);
  }

  // TODO(WAN): not a SQLStatement?
  /**
   * Visitor pattern for GroupByDescription.
   * @param v Visitor pattern for the statement
   */
  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) { v->Visit(common::ManagedPointer(this)); }

  /** @return group by columns */
  const std::vector<common::ManagedPointer<AbstractExpression>> &GetColumns() { return columns_; }

  /** @return having clause */
  common::ManagedPointer<AbstractExpression> GetHaving() { return common::ManagedPointer(having_); }

  /**
   * @return the hashed value of this group by description
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(columns_.size());
    for (const auto &col : columns_) {
      hash = common::HashUtil::CombineHashes(hash, col->Hash());
    }
    if (having_ != nullptr) hash = common::HashUtil::CombineHashes(hash, having_->Hash());
    return hash;
  }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two GroupByDescriptions are logically equal
   */
  bool operator==(const GroupByDescription &rhs) const {
    if (columns_.size() != rhs.columns_.size()) return false;
    for (size_t i = 0; i < columns_.size(); i++)
      if (*(columns_[i]) != *(rhs.columns_[i])) return false;

    if (having_ != nullptr && rhs.having_ == nullptr) return false;
    if (having_ == nullptr && rhs.having_ != nullptr) return false;
    if (having_ == nullptr && rhs.having_ == nullptr) return true;
    return *(having_) == *(rhs.having_);
  }

  /**
   * Logical inequality check.
   * @param rhs other
   * @return true if the two GroupByDescription are logically unequal
   */
  bool operator!=(const GroupByDescription &rhs) const { return !(operator==(rhs)); }

  /**
   * @return GroupDescription serialized to json
   */
  nlohmann::json ToJson() const;
  /**
   * @param j json to deserialize
   */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j);

 private:
  std::vector<common::ManagedPointer<AbstractExpression>> columns_;
  common::ManagedPointer<AbstractExpression> having_;
};

DEFINE_JSON_HEADER_DECLARATIONS(GroupByDescription);

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
  SelectStatement(std::vector<common::ManagedPointer<AbstractExpression>> select, bool select_distinct,
                  std::unique_ptr<TableRef> from, common::ManagedPointer<AbstractExpression> where,
                  std::unique_ptr<GroupByDescription> group_by, std::unique_ptr<OrderByDescription> order_by,
                  std::unique_ptr<LimitDescription> limit)
      : SQLStatement(StatementType::SELECT),
        select_(std::move(select)),
        select_distinct_(select_distinct),
        from_(std::move(from)),
        where_(where),
        group_by_(std::move(group_by)),
        order_by_(std::move(order_by)),
        limit_(std::move(limit)),
        union_select_(nullptr) {}

  /** Default constructor for deserialization. */
  SelectStatement() = default;

  void Accept(common::ManagedPointer<binder::SqlNodeVisitor> v) override { v->Visit(common::ManagedPointer(this)); }

  /** @return a copy of the select statement */
  std::unique_ptr<SelectStatement> Copy();

  /** @return select columns */
  const std::vector<common::ManagedPointer<AbstractExpression>> &GetSelectColumns() { return select_; }

  /** @return true if "SELECT DISTINCT", false otherwise */
  bool IsSelectDistinct() { return select_distinct_; }

  /** @return table being selected from */
  common::ManagedPointer<TableRef> GetSelectTable() { return common::ManagedPointer(from_); }

  /** @return select condition */
  common::ManagedPointer<AbstractExpression> GetSelectCondition() { return where_; }

  /** @return select group by */
  common::ManagedPointer<GroupByDescription> GetSelectGroupBy() { return common::ManagedPointer(group_by_); }

  /** @return select order by */
  common::ManagedPointer<OrderByDescription> GetSelectOrderBy() { return common::ManagedPointer(order_by_); }

  /** @return select limit */
  common::ManagedPointer<LimitDescription> GetSelectLimit() { return common::ManagedPointer(limit_); }

  /** @return depth of the select statement */
  int GetDepth() { return depth_; }

  /**
   * Adds a select statement child as a union target.
   * @param select_stmt select statement to union with
   */
  void SetUnionSelect(std::unique_ptr<SelectStatement> select_stmt) { union_select_ = std::move(select_stmt); }

  /**
   * @return the hashed value of this select statement
   */
  common::hash_t Hash() const;

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two SelectStatement are logically equal
   */
  bool operator==(const SelectStatement &rhs) const;

  /**
   * Logical inequality check.
   * @param rhs other
   * @return true if the two SelectStatement are logically unequal
   */
  bool operator!=(const SelectStatement &rhs) const { return !(operator==(rhs)); }

  /** @return statement serialized to json */
  nlohmann::json ToJson() const override;

  /** @param j json to deserialize */
  std::vector<std::unique_ptr<AbstractExpression>> FromJson(const nlohmann::json &j) override;

 private:
  friend class binder::BindNodeVisitor;
  std::vector<common::ManagedPointer<AbstractExpression>> select_;
  bool select_distinct_;
  std::unique_ptr<TableRef> from_;
  common::ManagedPointer<AbstractExpression> where_;
  std::unique_ptr<GroupByDescription> group_by_;
  std::unique_ptr<OrderByDescription> order_by_;
  std::unique_ptr<LimitDescription> limit_;
  std::unique_ptr<SelectStatement> union_select_;
  int depth_ = -1;

  /** @param select List of select columns */
  void SetSelectColumns(std::vector<common::ManagedPointer<AbstractExpression>> select) { select_ = std::move(select); }

  /** @param depth Depth of the select statement */
  void SetDepth(int depth) { depth_ = depth; }
};

DEFINE_JSON_HEADER_DECLARATIONS(SelectStatement);

}  // namespace parser
}  // namespace noisepage
