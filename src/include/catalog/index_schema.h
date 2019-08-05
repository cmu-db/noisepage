#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/json.h"
#include "common/macros.h"
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
        : name_(std::move(name)), oid_(INVALID_INDEXKEYCOL_OID), packed_type_(0), definition_(definition.Copy()) {
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
        : name_(std::move(name)), oid_(INVALID_INDEXKEYCOL_OID), packed_type_(0), definition_(definition.Copy()) {
      TERRIER_ASSERT(type_id == type::TypeId::VARCHAR || type_id == type::TypeId::VARBINARY, "Varlen constructor.");
      TERRIER_ASSERT(definition_.use_count() == 1, "This expression should only be shared using managed pointers");
      SetTypeId(type_id);
      SetNullable(nullable);
      SetMaxVarlenSize(max_varlen_size);
    }

    /**
     * Overrides default copy constructor to ensure we do a deep copy on the abstract expressions
     * @param old_column to be copied
     */
    Column(const Column &old_column)
        : name_(old_column.name_),
          oid_(old_column.oid_),
          packed_type_(old_column.packed_type_),
          definition_(old_column.definition_->Copy()) {
      TERRIER_ASSERT(definition_.use_count() == 1, "This expression should only be shared using managed pointers");
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
    common::ManagedPointer<const parser::AbstractExpression> StoredExpression() const {
      TERRIER_ASSERT(definition_.use_count() == 1, "This expression should only be shared using managed pointers");
      return common::ManagedPointer(static_cast<const parser::AbstractExpression *>(definition_.get()));
    }

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

    /**
     * Default constructor for deserialization
     */
    Column() = default;

    /**
     * @return column serialized to json
     */
    nlohmann::json ToJson() const {
      nlohmann::json j;
      j["name"] = name_;
      j["type"] = Type();
      j["max_varlen_size"] = MaxVarlenSize();
      j["nullable"] = Nullable();
      j["oid"] = oid_;
      j["definition"] = definition_;
      return j;
    }

    /**
     * Deserializes a column
     * @param j serialized column
     */
    void FromJson(const nlohmann::json &j) {
      name_ = j.at("name").get<std::string>();
      SetTypeId(j.at("type").get<type::TypeId>());
      SetMaxVarlenSize(j.at("max_varlen_size").get<uint16_t>());
      SetNullable(j.at("nullable").get<bool>());
      SetOid(j.at("oid").get<indexkeycol_oid_t>());
      definition_ = parser::DeserializeExpression(j.at("definition"));
    }

   private:
    static constexpr uint32_t MASK_VARLEN = 0x00FFFF00;
    static constexpr uint32_t MASK_NULLABLE = 0x00000080;
    static constexpr uint32_t MASK_TYPE = 0x0000007F;
    static constexpr uint32_t OFFSET_VARLEN = 8;

    std::string name_;
    indexkeycol_oid_t oid_;
    uint32_t packed_type_;

    // TODO(John) this should become a unique_ptr as part of addressing #489
    std::shared_ptr<parser::AbstractExpression> definition_;

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
   * @param name name of the Column to access
   * @return description of the schema for a specific column
   * @throw std::out_of_range if the column doesn't exist.
   */
  Column GetColumn(const std::string &name) const {
    for (auto &c : columns_) {
      if (c.Name() == name) {
        return c;
      }
    }
    // TODO(John): Should this be a TERRIER_ASSERT to have the same semantics
    // as the other accessor methods above?
    throw std::out_of_range("Column name doesn't exist");
  }

  /**
   * @return true if this schema is for a unique index
   */
  bool Unique() const { return is_unique_; }

  /**
   * @return true if this schema is for a unique index
   */
  bool Primary() const { return is_primary_; }

  /**
   * @return true if this schema is for a unique index
   */
  bool Exclusion() const { return is_exclusion_; }

  /**
   * @return true if this schema is for a unique index
   */
  bool Immediate() const { return is_immediate_; }

  /**
   * @return serialized schema
   */
  nlohmann::json ToJson() const {
    // Only need to serialize columns_ because col_oid_to_offset is derived from columns_
    nlohmann::json j;
    j["columns"] = columns_;
    j["unique"] = is_unique_;
    j["primary"] = is_primary_;
    j["exclusion"] = is_exclusion_;
    j["immediate"] = is_immediate_;
    j["valid"] = is_valid_;
    j["ready"] = is_ready_;
    j["live"] = is_live_;
    return j;
  }

  /**
   * Should not be used. See TERRIER_ASSERT
   */
  void FromJson(const nlohmann::json &j) {
    TERRIER_ASSERT(false, "Schema::FromJson should never be invoked directly; use DeserializeSchema");
  }

  /**
   * Deserialize a schema
   * @param j json containing serialized schema
   * @return deserialized schema object
   */
  std::shared_ptr<IndexSchema> static DeserializeSchema(const nlohmann::json &j) {
    auto columns = j.at("columns").get<std::vector<IndexSchema::Column>>();
    auto unique = j.at("unique").get<bool>();
    auto primary = j.at("primary").get<bool>();
    auto exclusion = j.at("exclusion").get<bool>();
    auto immediate = j.at("immediate").get<bool>();

    auto schema = std::make_shared<IndexSchema>(columns, unique, primary, exclusion, immediate);

    schema->SetValid(j.at("valid").get<bool>());
    schema->SetReady(j.at("ready").get<bool>());
    schema->SetLive(j.at("live").get<bool>());

    return schema;
  }

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

DEFINE_JSON_DECLARATIONS(IndexSchema::Column);
DEFINE_JSON_DECLARATIONS(IndexSchema);

}  // namespace terrier::catalog
