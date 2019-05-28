#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include "catalog/catalog_defs.h"
#include "common/macros.h"
#include "parser/expression_defs.h"
#include "parser/expression/abstract_expression.h"

namespace terrier {

namespace parser {
class AbstractExpression;
}

namespace optimizer {
/**
 * Operator type
 */
enum class OpType {
  UNDEFINED = 0,

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
  LOGICALDISTINCT,
  LOGICALEXPORTEXTERNALFILE,

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
  DISTINCT,
  INNERNLJOIN,
  LEFTNLJOIN,
  RIGHTNLJOIN,
  OUTERNLJOIN,
  INNERHASHJOIN,
  LEFTHASHJOIN,
  RIGHTHASHJOIN,
  OUTERHASHJOIN,
  INSERT,
  INSERTSELECT,
  DELETE,
  UPDATE,
  AGGREGATE,
  HASHGROUPBY,
  SORTGROUPBY,
  EXPORTEXTERNALFILE,
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
  AnnotatedExpression(std::shared_ptr<parser::AbstractExpression> expr,
                      std::unordered_set<std::string> &&table_alias_set)
      : expr_(std::move(expr)), table_alias_set_(std::move(table_alias_set)) {}

  /**
   * Default copy constructor
   * @param ant_expr reference to the AnnotatedExpression to copy from
   */
  AnnotatedExpression(const AnnotatedExpression &ant_expr) = default;

  /**
   * @return the expresion to be annotated
   */
  std::shared_ptr<parser::AbstractExpression> GetExpr() const { return expr_; }

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
    if (!expr_ && !(rhs.expr_))
      return true;
    if (expr_ && rhs.expr_)
      return *expr_ == *(rhs.expr_);
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
  std::shared_ptr<parser::AbstractExpression> expr_;

  /**
   * Unordered set of table aliases
   */
  std::unordered_set<std::string> table_alias_set_;
};

}  // namespace optimizer
}  // namespace terrier
