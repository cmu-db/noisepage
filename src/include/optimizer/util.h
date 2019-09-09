#pragma once

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/managed_pointer.h"
#include "parser/expression/abstract_expression.h"
#include "planner/plannodes/abstract_plan_node.h"

namespace terrier::optimizer::util {

/**
 * Check if a set is a subset of another set
 *
 * @param super_set The potential super set
 * @param child_set The potential child set
 *
 * @return True if the second set is a subset of the first one
 */
template <class T>
bool IsSubset(const std::unordered_set<T> &super_set, const std::unordered_set<T> &child_set) {
  for (auto &element : child_set) {
    if (super_set.find(element) == super_set.end()) return false;
  }
  return true;
}

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
void ExtractEquiJoinKeys(const std::vector<AnnotatedExpression> &join_predicates,
                         std::vector<common::ManagedPointer<const parser::AbstractExpression>> *left_keys,
                         std::vector<common::ManagedPointer<const parser::AbstractExpression>> *right_keys,
                         const std::unordered_set<std::string> &left_alias,
                         const std::unordered_set<std::string> &right_alias);

}  // namespace terrier::optimizer::util
