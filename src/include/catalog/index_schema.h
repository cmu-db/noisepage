#pragma once

#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "parser/expression/abstract_expression.h"
#include "type/type_id.h"

namespace terrier {
class StorageTestUtil;
}

namespace terrier::tpcc {
class Schemas;
}

namespace terrier::catalog {
class DatabaseCatalog;

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
     * @param name column name (column name or "expr")
     * @param type_id the non-varlen type of the column
     * @param nullable whether the column is nullable
     * @param definition definition of this attribute
     */
    Column(std::string name, type::TypeId type_id, bool nullable, const parser::AbstractExpression &definition)
        : name_(std::move(name)),
          oid_(INVALID_INDEXKEYCOL_OID),
          packed_type_(0),
          definition_(common::ManagedPointer<const parser::AbstractExpression>(&definition)) {
      TERRIER_ASSERT(!(type_id == type::TypeId::VARCHAR || type_id == type::TypeId::VARBINARY),
                     "Non-varlen constructor.");
      SetTypeId(type_id);
      SetNullable(nullable);
    }

    /**
     * Varlen constructor for index key columns.
     * @param name column name (column name or "expr")
     * @param type_id the varlen type of the column
     * @param max_varlen_size the maximum varlen size
     * @param nullable whether the column is nullable
     * @param definition definition of this attribute
     */
    Column(std::string name, type::TypeId type_id, uint16_t max_varlen_size, bool nullable,
           const parser::AbstractExpression &definition)
        : name_(std::move(name)),
          oid_(INVALID_INDEXKEYCOL_OID),
          packed_type_(0),
          definition_(common::ManagedPointer<const parser::AbstractExpression>(&definition)) {
      TERRIER_ASSERT(type_id == type::TypeId::VARCHAR || type_id == type::TypeId::VARBINARY, "Varlen constructor.");
      SetTypeId(type_id);
      SetNullable(nullable);
      SetMaxVarlenSize(max_varlen_size);
    }

    /**
     * @return column name
     */
    const std::string &Name() const { return name_; }

    /**
     * @return oid of this key column
     */
    indexkeycol_oid_t Oid() const { return oid_; }

    /**
     * @return definition expression
     */
    common::ManagedPointer<const parser::AbstractExpression> StoredExpression() const { return definition_; }

    /**
     * @warning only defined for varlen types
     * @return maximum varlen size of this varlen column
     */
    uint16_t MaxVarlenSize() const { return static_cast<uint16_t>((packed_type_ & MASK_VARLEN) >> OFFSET_VARLEN); }

    /**
     * @return size of the attribute in bytes. Varlen attributes have the sign bit set.
     */
    uint8_t AttrSize() const { return type::TypeUtil::GetTypeSize(Type()); }

    /**
     * @return type of this key column
     */
    type::TypeId Type() const { return static_cast<type::TypeId>(packed_type_ & MASK_TYPE); }

    /**
     * @return true if this column is nullable
     */
    bool Nullable() const { return static_cast<bool>(packed_type_ & MASK_NULLABLE); }

   private:
    static constexpr uint32_t MASK_VARLEN = 0x00FFFF00;
    static constexpr uint32_t MASK_NULLABLE = 0x00000080;
    static constexpr uint32_t MASK_TYPE = 0x0000007F;
    static constexpr uint32_t OFFSET_VARLEN = 8;

    std::string name_;
    indexkeycol_oid_t oid_;
    uint32_t packed_type_;
    common::ManagedPointer<const parser::AbstractExpression> definition_;
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

    friend class tpcc::Schemas;
    friend class terrier::StorageTestUtil;
  };

  /**
   * Instantiates a new catalog description of an index
   * @param columns describing the individual parts of the key
   * @param is_unique indicating whether the same key can be (logically) visible repeats are allowed in the index
   * @param is_primary indicating whether this will be the index for a primary key
   * @param is_exclusion indicating whether this index is for exclusion constraints
   * @param is_immediate indicating that the uniqueness check fails at insertion time
   */
  IndexSchema(std::vector<Column> columns, const bool is_unique, const bool is_primary, const bool is_exclusion,
              const bool is_immediate)
      : columns_(std::move(columns)),
        is_unique_(is_unique),
        is_primary_(is_primary),
        is_exclusion_(is_exclusion),
        is_immediate_(is_immediate),
        is_valid_(false),
        is_ready_(false),
        is_live_(true) {
    TERRIER_ASSERT((is_primary && is_unique) || (!is_primary), "is_primary requires is_unique to be true as well.");
  }

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

  /**
   * @return true if this schema is for a unique index
   */
  bool Unique() const { return is_unique_; }

 private:
  friend class DatabaseCatalog;
  std::vector<Column> columns_;
  bool is_unique_;
  bool is_primary_;
  bool is_exclusion_;
  bool is_immediate_;
  bool is_valid_;
  bool is_ready_;
  bool is_live_;

  void SetValid(const bool is_valid) { is_valid_ = is_valid; }
  void SetReady(const bool is_ready) { is_ready_ = is_ready; }
  void SetLive(const bool is_live) { is_live_ = is_live; }

  friend class Catalog;
  friend class postgres::Builder;
};

}  // namespace terrier::catalog
