#pragma once

#include <vector>
#include "catalog/catalog_defs.h"
#include "type/type_id.h"

namespace terrier::storage::index {

/**
 * A column of the index key has an identifier, type, and describes whether it can be null.
 */
class IndexKeyColumn {
 public:

  /**
   * Non-varlen constructor for index key columns.
   * @param oid key column oid
   * @param nullable whether the column is nullable
   * @param type_id the non-varlen type of the column
   */
  IndexKeyColumn(catalog::indexkeycol_oid_t oid, type::TypeId type_id, bool nullable)
      : oid_(oid) {
    TERRIER_ASSERT(!(type_id == type::TypeId::VARCHAR || type_id == type::TypeId::VARBINARY), "Non-varlen constructor.");
    SetTypeId(type_id);
    SetNullable(nullable);
  }

  /**
   * Varlen constructor for index key columns.
   * @param oid key column oid
   * @param nullable whether the column is nullable
   * @param type_id the varlen type of the column
   * @param max_varlen_size the maximum varlen size
   */
  IndexKeyColumn(catalog::indexkeycol_oid_t oid, type::TypeId type_id, bool nullable, uint16_t max_varlen_size)
  : oid_(oid) {
    TERRIER_ASSERT(type_id == type::TypeId::VARCHAR || type_id == type::TypeId::VARBINARY, "Varlen constructor.");
    SetTypeId(type_id);
    SetNullable(nullable);
    SetMaxVarlenSize(max_varlen_size);
  }

  /**
   * @return oid of this key column
   */
  catalog::indexkeycol_oid_t GetOid() const { return oid_; }

  /**
   * @warning only defined for varlen types
   * @return maximum varlen size of this varlen column
   */
  uint16_t GetMaxVarlenSize() const { return max_varlen_size_; }

  /**
   * @return type of this key column
   */
  type::TypeId GetType() const { return type_id_; }

  /**
   * @return true if this column is nullable
   */
  bool IsNullable() const { return is_nullable_; }

 private:
  catalog::indexkeycol_oid_t oid_;
  uint16_t max_varlen_size_;
  type::TypeId type_id_;
  bool is_nullable_;

  void SetMaxVarlenSize(uint16_t max_varlen_size) { max_varlen_size_ = max_varlen_size; }
  void SetTypeId(type::TypeId type_id) { type_id_ = type_id; }
  void SetNullable(bool nullable) { is_nullable_ = nullable; }
};

/**
 * A schema for the index key.
 * @warning the columns of the IndexKeySchema are assumed to be in comparison order
 */
using IndexKeySchema = std::vector<IndexKeyColumn>;

/**
 * The type of index.
 */
enum class ConstraintType : uint8_t {
  // invalid index constraint type
  INVALID = 0,
  // default type - not used to enforce constraints
  DEFAULT = 1,
  // used to enforce primary key constraint
  PRIMARY_KEY = 2,
  // used for unique constraint
  UNIQUE = 3
};
}  // namespace terrier::storage::index
