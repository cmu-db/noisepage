#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "catalog/catalog_defs.h"
#include "common/macros.h"
#include "common/managed_pointer.h"
#include "common/strong_typedef.h"
#include "parser/expression/abstract_expression.h"
#include "parser/expression_defs.h"

namespace terrier::optimizer {

/**
 * typedef for GroupID
 */
STRONG_TYPEDEF_HEADER(group_id_t, int32_t);

/**
 * Definition for a UNDEFINED_GROUP
 */
const group_id_t UNDEFINED_GROUP = group_id_t(-1);

/**
 * Enumeration defining external file formats
 */
enum class ExternalFileFormat { CSV };

/**
 * OrderBy ordering
 */
enum class OrderByOrderingType { ASC, DESC };

/**
 * Operator type
 */
enum class OpType {
  UNDEFINED = 0,

  // Special wildcard
  LEAF,

  // Logical Operators
  LOGICALGET,
  LOGICALEXTERNALFILEGET,
  LOGICALQUERYDERIVEDGET,
  LOGICALPROJECTION,
  LOGICALFILTER,
  LOGICALMARKJOIN,
  LOGICALDEPENDENTJOIN,
  LOGICALSINGLEJOIN,
  LOGICALINNERJOIN,
  LOGICALLEFTJOIN,
  LOGICALRIGHTJOIN,
  LOGICALOUTERJOIN,
  LOGICALSEMIJOIN,
  LOGICALAGGREGATEANDGROUPBY,
  LOGICALINSERT,
  LOGICALINSERTSELECT,
  LOGICALDELETE,
  LOGICALUPDATE,
  LOGICALLIMIT,
  LOGICALEXPORTEXTERNALFILE,
  LOGICALCREATEDATABASE,
  LOGICALCREATEFUNCTION,
  LOGICALCREATEINDEX,
  LOGICALCREATETABLE,
  LOGICALCREATENAMESPACE,
  LOGICALCREATETRIGGER,
  LOGICALCREATEVIEW,
  LOGICALDROPDATABASE,
  LOGICALDROPTABLE,
  LOGICALDROPINDEX,
  LOGICALDROPNAMESPACE,
  LOGICALDROPFUNCTION,
  LOGICALDROPTRIGGER,
  LOGICALDROPVIEW,
  LOGICALANALYZE,
  // Separation of logical and physical operators
  LOGICALPHYSICALDELIMITER,

  // Physical Operators
  TABLEFREESCAN,  // Scan Op for SELECT without FROM
  SEQSCAN,
  INDEXSCAN,
  EXTERNALFILESCAN,
  QUERYDERIVEDSCAN,
  ORDERBY,
  LIMIT,
  INNERINDEXJOIN,
  INNERNLJOIN,
  LEFTNLJOIN,
  RIGHTNLJOIN,
  OUTERNLJOIN,
  INNERHASHJOIN,
  LEFTHASHJOIN,
  RIGHTHASHJOIN,
  OUTERHASHJOIN,
  LEFTSEMIHASHJOIN,
  INSERT,
  INSERTSELECT,
  DELETE,
  UPDATE,
  AGGREGATE,
  HASHGROUPBY,
  SORTGROUPBY,
  EXPORTEXTERNALFILE,
  CREATEDATABASE,
  CREATEFUNCTION,
  CREATEINDEX,
  CREATETABLE,
  CREATENAMESPACE,
  CREATETRIGGER,
  CREATEVIEW,
  DROPDATABASE,
  DROPTABLE,
  DROPINDEX,
  DROPNAMESPACE,
  DROPFUNCTION,
  DROPTRIGGER,
  DROPVIEW,
  ANALYZE
};

/**
 * Augmented expression with a set of table aliases
 */
class AnnotatedExpression {
 public:
  /**
   * Create an AnnotatedExpression
   * @param expr expression to be annotated
   * @param table_alias_set an unordered set of table aliases
   */
  AnnotatedExpression(common::ManagedPointer<parser::AbstractExpression> expr,
                      std::unordered_set<std::string> &&table_alias_set)
      : expr_(expr), table_alias_set_(std::move(table_alias_set)) {}

  /**
   * Default copy constructor
   * @param ant_expr reference to the AnnotatedExpression to copy from
   */
  AnnotatedExpression(const AnnotatedExpression &ant_expr) = default;

  /**
   * @return the expresion to be annotated
   */
  common::ManagedPointer<parser::AbstractExpression> GetExpr() const { return expr_; }

  /**
   * @return the unordered set of table aliases
   */
  const std::unordered_set<std::string> &GetTableAliasSet() const { return table_alias_set_; }

  /**
   * Logical equality check.
   * @param rhs other
   * @return true if the two expressions are logically equal
   */
  bool operator==(const AnnotatedExpression &rhs) const {
    /**
     * In the original code, the comparison was implemented in
     * /src/optimizer/operators.cpp by comparing only the expr of the AnnotatedExpression
     */
    if (!expr_ && !(rhs.expr_)) return true;
    if (expr_ && rhs.expr_) return *expr_ == *(rhs.expr_);
    return false;
  }

  /**
   * Logical inequality check.
   * @param rhs other
   * @return true if the two expressions are logically not equal
   */
  bool operator!=(const AnnotatedExpression &rhs) const { return !operator==(rhs); }

 private:
  /**
   * Expression to be annotated
   */
  common::ManagedPointer<parser::AbstractExpression> expr_;

  /**
   * Unordered set of table aliases
   */
  std::unordered_set<std::string> table_alias_set_;
};

/**
 * Struct implementing equality comparisons for both terrier::parser::AbstractExpression*
 * and the const version const terrier::parser::AbstractExpression*
 */
struct ExprEqualCmp {
  /**
   * Checks two AbstractExpression for equality
   * @param lhs one of the AbstractExpression
   * @param rhs the other AbstractExpression
   * @return whether the two are equal
   *
   * @pre lhs != nullptr && rhs != nullptr
   */
  bool operator()(common::ManagedPointer<terrier::parser::AbstractExpression> lhs,
                  common::ManagedPointer<terrier::parser::AbstractExpression> rhs) {
    TERRIER_ASSERT(lhs != nullptr && rhs != nullptr, "AbstractExpressions should not be null");
    return (*lhs == *rhs);
  }

  /**
   * Checks two AbstractExpression for equality (const version)
   * @param lhs one of the AbstractExpression
   * @param rhs the other AbstractExpression
   * @return whether the two are equal
   *
   * @pre lhs != nullptr && rhs != nullptr
   */
  bool operator()(const common::ManagedPointer<terrier::parser::AbstractExpression> lhs,
                  const common::ManagedPointer<terrier::parser::AbstractExpression> rhs) const {
    TERRIER_ASSERT(lhs != nullptr && rhs != nullptr, "AbstractExpressions should not be null");
    return (*lhs == *rhs);
  }
};

/**
 * Struct implementing Hash() for const terrier::parser::AbstractExpression*
 */
struct ExprHasher {
  /**
   * Hashes the given expression
   * @param expr the expression to hash
   * @return hash code of the given expression
   *
   * @pre expr != nullptr
   */
  size_t operator()(const common::ManagedPointer<terrier::parser::AbstractExpression> expr) const {
    TERRIER_ASSERT(expr != nullptr, "AbstractExpression should not be null");
    return expr->Hash();
  }
};

/**
 * Defines an ExprMap
 * ExprMap is used exclusively in the optimizer to map from an AbstractExpression
 * to a given column offset created by specific operators.
 */
using ExprMap =
    std::unordered_map<common::ManagedPointer<parser::AbstractExpression>, unsigned, ExprHasher, ExprEqualCmp>;

/**
 * Defines an ExprSet.
 * ExprSet is used in the optimizer to speed up AbstractExpression comparisons
 * (checking whether an AbstractExpression already exists in a collection).
 */
using ExprSet = std::unordered_set<common::ManagedPointer<parser::AbstractExpression>, ExprHasher, ExprEqualCmp>;

}  // namespace terrier::optimizer
