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
  IndexKeyColumn(catalog::indexkeycol_oid_t oid, type::TypeId type_id, bool nullable) : oid_(oid), packed_type_(0) {
    TERRIER_ASSERT(!(type_id == type::TypeId::VARCHAR || type_id == type::TypeId::VARBINARY),
                   "Non-varlen constructor.");
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
      : oid_(oid), packed_type_(0) {
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
  uint16_t GetMaxVarlenSize() const { return static_cast<uint16_t>((packed_type_ & MASK_VARLEN) >> OFFSET_VARLEN); }

  /**
   * @return type of this key column
   */
  type::TypeId GetType() const { return static_cast<type::TypeId>(packed_type_ & MASK_TYPE); }

  /**
   * @return true if this column is nullable
   */
  bool IsNullable() const { return static_cast<bool>(packed_type_ & MASK_NULLABLE); }

 private:
  static constexpr uint32_t MASK_VARLEN = 0x00FFFF00;
  static constexpr uint32_t MASK_NULLABLE = 0x00000080;
  static constexpr uint32_t MASK_TYPE = 0x0000007F;
  static constexpr uint32_t OFFSET_VARLEN = 8;

  catalog::indexkeycol_oid_t oid_;
  uint32_t packed_type_;

  void SetMaxVarlenSize(uint16_t max_varlen_size) {
    TERRIER_ASSERT((packed_type_ & MASK_VARLEN) == 0, "Should only set max varlen size once.");
    const auto varlen_bits = (max_varlen_size << OFFSET_VARLEN) & MASK_VARLEN;
    packed_type_ = packed_type_ | varlen_bits;
  }

  void SetTypeId(type::TypeId type_id) {
    TERRIER_ASSERT((packed_type_ & MASK_TYPE) == 0, "Should only set type once.");
    packed_type_ = packed_type_ | (static_cast<uint32_t>(type_id) & MASK_TYPE);
  }

  void SetNullable(bool nullable) {
    TERRIER_ASSERT((packed_type_ & MASK_NULLABLE) == 0, "Should only set nullability once.");
    packed_type_ = nullable ? packed_type_ | MASK_NULLABLE : packed_type_;
  }
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
  // used for unique constraint
  UNIQUE = 2
};
}  // namespace terrier::storage::index
