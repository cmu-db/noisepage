#pragma once

#include "common/hash_util.h"
#include "common/managed_pointer.h"
#include "parser/expression/tuple_value_expression.h"
#include "optimizer/property.h"

namespace terrier {
namespace optimizer {

// Specifies the required sorting order of the query
class PropertySort : public Property {
 public:
  /**
   * Constructor for PropertySort
   * @param sort_columns vector of AbstractExpressions representing sort columns
   * @param sort_ascending Whether each sort_column is ascending or descending
   */
  PropertySort(std::vector<common::ManagedPointer<parser::AbstractExpression>> sort_columns,
               std::vector<bool> sort_ascending)
    : sort_columns_(std::move(sort_columns)),
      sort_ascending_(std::move(sort_ascending)) {}

  /**
   * Returns the type of PropertySort
   * @returns PropertyType::SORT
   */
  PropertyType Type() const override {
    return PropertyType::SORT;
  }

  /**
   * Copy
   */
  PropertySort* Copy() {
    return new PropertySort(sort_columns_, sort_ascending_);
  }

  /**
   * Gets the number of sort columns
   * @returns Number of sort columns
   */
  inline size_t GetSortColumnSize() const { return sort_columns_.size(); }

  /**
   * Gets a sort column at specified index
   * @param idx Index of sort column to retrieve
   * @returns Sort Column
   */
  inline common::ManagedPointer<parser::AbstractExpression> GetSortColumn(size_t idx) const {
    return sort_columns_[idx];
  }

  /**
   * Gets whether a sort column is sorted ascending
   * @param idx Index of ascending flag to retrieve
   * @returns Whether sort column at index idx is sorted in ascending order
   */
  inline bool GetSortAscending(int idx) const { return sort_ascending_[idx]; }

  /**
   * Hashes this PropertySort
   * @returns Hash code
   */
  common::hash_t Hash() const override;

  /**
   * Checks whether this is greater than or equal to another property.
   * If the other property is not a sort property, this function returns FALSE.
   * This property ensures that Sort(a,b,c,d,e) >= Sort(a,b,c)
   *
   * @param r other property to compare against
   * @returns TRUE if this >= r
   */
  bool operator>=(const Property &r) const override;

  /**
   * PropertySort's Accept function for the visitor
   * @param v Visitor
   */
  void Accept(PropertyVisitor *v) const override;

 private:
  std::vector<common::ManagedPointer<parser::AbstractExpression>> sort_columns_;
  std::vector<bool> sort_ascending_;
};

}  // namespace optimizer
}  // namespace terrier
