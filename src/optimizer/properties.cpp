#include "optimizer/properties.h"

#include "common/hash_util.h"
#include "optimizer/property.h"
#include "optimizer/property_visitor.h"

namespace noisepage::optimizer {

/**
 * Checks whether this is greater than or equal to another property.
 * If the other property is not a sort property, this function returns FALSE.
 * This property ensures that Sort(a,b,c,d,e) >= Sort(a,b,c)
 *
 * @param r other property to compare against
 * @returns TRUE if this >= r
 */
bool PropertySort::operator>=(const Property &r) const {
  // check the type
  if (r.Type() != PropertyType::SORT) return false;
  const PropertySort &r_sort = *reinterpret_cast<const PropertySort *>(&r);

  // All the sorting orders in r must be satisfied
  size_t l_num_sort_columns = sort_columns_.size();
  size_t r_num_sort_columns = r_sort.sort_columns_.size();
  NOISEPAGE_ASSERT(r_num_sort_columns == r_sort.sort_ascending_.size(),
                   "Sort property num_sort_columns not match sort_ascending_.size()");

  // We want to ensure that Sort(a, b, c, d, e) >= Sort(a, b, c)
  if (l_num_sort_columns < r_num_sort_columns) {
    return false;
  }

  // Check that the AbstractExpression of sort column match.
  // This relies on AbstractExpression::operator== working correctly.
  for (size_t idx = 0; idx < r_num_sort_columns; ++idx) {
    if (*sort_columns_[idx] != *r_sort.sort_columns_[idx]) {
      return false;
    }
  }
  return true;
}

/**
 * Hashes this PropertySort
 * @returns Hash code
 */
common::hash_t PropertySort::Hash() const {
  // hash the type
  common::hash_t hash = Property::Hash();

  // hash sorting columns
  size_t num_sort_columns = sort_columns_.size();
  for (size_t i = 0; i < num_sort_columns; ++i) {
    hash = common::HashUtil::CombineHashes(hash, sort_columns_[i]->Hash());

    auto asc_hash = common::HashUtil::Hash<OrderByOrderingType>(sort_ascending_[i]);
    hash = common::HashUtil::CombineHashes(hash, asc_hash);
  }
  return hash;
}

/**
 * PropertySort's Accept function for the visitor
 * @param v Visitor
 */
void PropertySort::Accept(PropertyVisitor *v) const { v->Visit(this); }

}  // namespace noisepage::optimizer
