#pragma once

#include <algorithm>
#include <cstdlib>
#include <string>
#include <unordered_set>

#include "common/managed_pointer.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier {
namespace optimizer {
namespace util {

/**
 * Convert upper case letters into lower case in a string
 *
 * @param str The string to operate on
 */
inline void to_lower_string(std::string &str) {
  std::transform(str.begin(), str.end(), str.begin(), ::tolower);
}

/**
 * Check if a set is a subset of another set
 *
 * @param super_set The potential super set
 * @param child_set The potential child set
 *
 * @return True if the second set is a subset of the first one
 */
template <class T>
bool IsSubset(const std::unordered_set<T> &super_set,
              const std::unordered_set<T> &child_set) {
  for (auto element : child_set) {
    if (super_set.find(element) == super_set.end()) return false;
  }
  return true;
}

/**
 * Split Conjunction AND expression tree into a vector of expressions.
 *
 * @param expr Expression tree to split (if root not CONJUNCTION_AND, no splitting happens)
 * @param predicates vector to place split expression trees (shared_ptr to expr nodes)
 */
void SplitPredicates(std::shared_ptr<parser::AbstractExpression> expr,
                     std::vector<std::shared_ptr<parser::AbstractExpression>> &predicates);

/**
 * @brief Extract single table precates and multi-table predicates from the expr
 *
 * @param expr The original predicate
 * @param annotated_predicates The extracted conjunction predicates
 */
std::vector<AnnotatedExpression> ExtractPredicates(
    std::shared_ptr<parser::AbstractExpression> expr,
    std::vector<AnnotatedExpression> annotated_predicates = {});

/**
 * Construct the map from subquery column name to the actual expression
 * at the subquery level, for example SELECT a FROM (SELECT a + b as a FROM
 * test), we'll build the map {"a" -> a + b}
 *
 * @param select_list The select list of a subquery
 * @return The mapping mentioned above
 */
std::unordered_map<std::string, std::shared_ptr<parser::AbstractExpression>>
ConstructSelectElementMap(std::vector<std::shared_ptr<parser::AbstractExpression>> &select_list);

/**
 * Walks through a vector of join predicates. Generates join keys based on the sets of left
 * and right table aliases.
 *
 * @param join_predicates vector of join predicates
 * @param left_keys output vector of left keys
 * @param right_keys output vector of right keys
 * @param left_alias Alias set for left table
 * @param right_alias Alias set for right table
 */
void ExtractEquiJoinKeys(
    const std::vector<AnnotatedExpression> join_predicates,
    std::vector<common::ManagedPointer<parser::AbstractExpression>> &left_keys,
    std::vector<common::ManagedPointer<parser::AbstractExpression>> &right_keys,
    const std::unordered_set<std::string> &left_alias,
    const std::unordered_set<std::string> &right_alias);

}  // namespace util
}  // namespace optimizer
}  // namespace terrier
