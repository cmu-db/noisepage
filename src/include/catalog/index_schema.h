#pragma once

#include <vector>
#include "catalog/catalog_defs.h"
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier::catalog {

namespace postgres {
class Builder;
}

/**
 * A schema for an index.  It contains the definitions for the columns in the
 * key as well as additional metdata.
 */
class IndexSchema {
 public:
  /**
   * A column of the index key has an identifier, type, and describes whether it can be null.
   */
  class Column {
   public:
    /**
     * Non-varlen constructor for index key columns.
     * @param oid key column oid
     * @param nullable whether the column is nullable
     * @param type_id the non-varlen type of the column
     */
    Column(type::TypeId type_id, bool nullable, const parser::AbstractExpression &expression)
        : oid_(INVALID_INDEXKEYCOL_OID), packed_type_(0) {
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
    Column(type::TypeId type_id, bool nullable, const parser::AbstractExpression &expression, uint16_t max_varlen_size)
        : oid_(INVALID_INDEXKEYCOL_OID), packed_type_(0) {
      TERRIER_ASSERT(type_id == type::TypeId::VARCHAR || type_id == type::TypeId::VARBINARY, "Varlen constructor.");
      SetTypeId(type_id);
      SetNullable(nullable);
      SetMaxVarlenSize(max_varlen_size);
    }

    /**
     * @return oid of this key column
     */
    indexkeycol_oid_t GetOid() const { return oid_; }

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

    /**
     * Note(Amadou): I added this to make sure this has the same name as Schema::Column
     * This way we can make templatized classes can use both classes interchangeably.
     * @return true if this column is nullable
     */
    bool GetNullable() const { return static_cast<bool>(packed_type_ & MASK_NULLABLE); }

   private:
    static constexpr uint32_t MASK_VARLEN = 0x00FFFF00;
    static constexpr uint32_t MASK_NULLABLE = 0x00000080;
    static constexpr uint32_t MASK_TYPE = 0x0000007F;
    static constexpr uint32_t OFFSET_VARLEN = 8;

    indexkeycol_oid_t oid_;
    uint32_t packed_type_;
    parser::AbstractExpression *expresion;
    std::string serialized_expression_;

    // TODO(John): Should these "OIDS" be implicitly set by the index in the columns?
    void SetOid(indexkeycol_oid_t oid) { oid_ = oid; }

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

    friend class DatabaseCatalog;
    friend class postgres::Builder;
  };

  /**
   * Instantiates a new catalog description of an index
   * @param columns describing the individual parts of the key
   * @param is_unique indicating whether the same key can be (logically) visible repeats are allowed in the index
   * @param is_primary indicating whether this will be the index for a primary key
   * @param is_exclusion indicating whether this index is for exclusion constraints
   * @param is_immediate indicating that the uniqueness check fails at insertion time
   */
  IndexSchema(std::vector<Column> columns, bool is_unique, bool is_primary, bool is_exclusion, bool is_immediate)
      : columns_(std::move(columns)),
        is_unique_(is_unique),
        is_primary_(is_primary),
        is_exclusion_(is_exclusion),
        is_immediate_(is_immediate),
        is_valid_(false),
        is_ready_(false),
        is_live_(true) {}

  IndexSchema() = default;

  /**
   * @return the columns which define the index's schema
   */
  const std::vector<Column> &GetColumns() const { return columns_; }

  /**
   * @param index in the column vector for the requested column
   * @return requested key column
   */
  const Column &GetColumn(int index) const { return columns_.at(index); }

 private:
  const std::vector<Column> columns_;
  bool is_unique_;
  bool is_primary_;
  bool is_exclusion_;
  bool is_immediate_;
  bool is_valid_;
  bool is_ready_;
  bool is_live_;

  void SetValid(bool is_valid) { is_valid_ = is_valid; }
  void SetReady(bool is_ready) { is_ready_ = is_ready; }
  void SetLive(bool is_live) { is_live_ = is_live; }

  friend class Catalog;
};

}  // namespace terrier::catalog
