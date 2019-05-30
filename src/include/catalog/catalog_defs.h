#pragma once

#include <string>
#include <utility>

#include "common/strong_typedef.h"
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier::catalog {

#define NULL_OID 0  // error return value
#define START_OID 1001

#define INVALID_COLUMN_OID col_oid_t(NULL_OID)
#define INVALID_DATABASE_OID db_oid_t(NULL_OID)
#define INVALID_INDEX_OID index_oid_t(NULL_OID)
#define INVALID_INDEXKEYCOL_OID index_keycoloid_t(NULL_OID)
#define INVALID_NAMESPACE_OID namespace_oid_t(NULL_OID)
#define INVALID_TABLE_OID table_oid_t(NULL_OID)
#define INVALID_TYPE_OID type_oid_t(NULL_OID)
#define INVALID_CONSTRAINT_OID constraint_oid_t(NULL_OID)

#define DEFAULT_DATABASE "terrier"

// in name order
STRONG_TYPEDEF(col_oid_t, uint32_t);
STRONG_TYPEDEF(db_oid_t, uint32_t);
STRONG_TYPEDEF(index_oid_t, uint32_t);
STRONG_TYPEDEF(indexkeycol_oid_t, uint32_t);
STRONG_TYPEDEF(namespace_oid_t, uint32_t);
STRONG_TYPEDEF(settings_oid_t, uint32_t);
STRONG_TYPEDEF(table_oid_t, uint32_t);
STRONG_TYPEDEF(tablespace_oid_t, uint32_t);
STRONG_TYPEDEF(trigger_oid_t, uint32_t);
STRONG_TYPEDEF(type_oid_t, uint32_t);
STRONG_TYPEDEF(view_oid_t, uint32_t);
STRONG_TYPEDEF(constraint_oid_t, uint32_t);

/**
 * Wraps the concept of a default value expression into an object that can be
 * passed between the catalog and the consumers of these expressions.
 */
class DefaultValue {
 public:
  /**
   * Constructs a default value.  We perform a deep copy for now to make lifecycle
   * reasoning easier.  This may need to be updated if it becomes a bottleneck.
   * @param type_id of the default value
   * @param serialized_expression for calculating default value at runtime
   */
  explicit DefaultValue(type::TypeId type_id, const AbstractExpression *expression)
      : type_(type_id), expression_(expression) {}

  /**
   * @return the type of the default value
   */
  const type::TypeId &GetType() { return type_; }

  /**
   * @return the expression that calculates the default value
   */
  const AbstractExpression *GetExpression() { return expression_; }

 private:
  type::TypeId type_;
  const AbstractExpression *expression_;
};
}  // namespace terrier::catalog
