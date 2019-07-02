#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "catalog/catalog_defs.h"
#include "common/constants.h"
#include "common/macros.h"
#include "common/strong_typedef.h"
#include "parser/expression/abstract_expression.h"
#include "storage/storage_defs.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace terrier::catalog {

namespace postgres {
class Builder;
}

/**
 * Internal object for representing SQL table schema. Currently minimal until we add more features to the system.
 * TODO(Matt): we should make sure to revisit the fields and their uses as we bring in a catalog to replace some of the
 * reliance on these classes
 */
class Schema {
 public:
  /**
   * Internal object for representing SQL table column. Currently minimal until we add more features to the system.
   * TODO(Matt): we should make sure to revisit the fields and their uses as we bring in a catalog to replace some of
   * the reliance on these classes
   */
  class Column {
   public:
    /**
     * Instantiates a Column object, primary to be used for building a Schema object (non VARLEN attributes)
     * @param name column name
     * @param type SQL type for this column
     * @param nullable true if the column is nullable, false otherwise
     * @param default_value for the column
     */
    Column(std::string name, const type::TypeId type, const bool nullable,
           const parser::AbstractExpression &default_value)
        : name_(std::move(name)),
          type_(type),
          attr_size_(type::TypeUtil::GetTypeSize(type_)),
          nullable_(nullable),
          oid_(INVALID_COLUMN_OID) {
      TERRIER_ASSERT(attr_size_ == 1 || attr_size_ == 2 || attr_size_ == 4 || attr_size_ == 8,
                     "This constructor is meant for non-VARLEN columns.");
      TERRIER_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");
    }

    /**
     * Instantiates a Column object, primary to be used for building a Schema object (VARLEN attributes only)
     * @param name column name
     * @param type SQL type for this column
     * @param max_varlen_size the maximum length of the varlen entry
     * @param nullable true if the column is nullable, false otherwise
     * @param default_value for the column
     */
    Column(std::string name, const type::TypeId type, const uint16_t max_varlen_size, const bool nullable,
           const parser::AbstractExpression &default_value)
        : name_(std::move(name)),
          type_(type),
          attr_size_(type::TypeUtil::GetTypeSize(type_)),
          max_varlen_size_(max_varlen_size),
          nullable_(nullable),
          oid_(INVALID_COLUMN_OID) {
      TERRIER_ASSERT(attr_size_ == VARLEN_COLUMN, "This constructor is meant for VARLEN columns.");
      TERRIER_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");
    }

    /**
     * @return column name
     */
    const std::string &GetName() const { return name_; }
    /**
     * @return true if the column is nullable, false otherwise
     */
    bool GetNullable() const { return nullable_; }
    /**
     * @return size of the attribute in bytes. Varlen attributes have the sign bit set.
     */
    uint8_t GetAttrSize() const { return attr_size_; }

    /**
     * @return The maximum length of this column (only valid if it's VARLEN)
     */
    uint16_t GetMaxVarlenSize() const {
      TERRIER_ASSERT(attr_size_ == VARLEN_COLUMN, "This attribute has no meaning for non-VARLEN columns.");
      return max_varlen_size_;
    }

    /**
     * @return SQL type for this column
     */
    type::TypeId GetType() const { return type_; }
    /**
     * @return internal unique identifier for this column
     */
    col_oid_t GetOid() const { return oid_; }

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
      j["type"] = type_;
      j["attr_size"] = attr_size_;
      j["max_varlen_size"] = max_varlen_size_;
      j["nullable"] = nullable_;
      j["oid"] = oid_;
      return j;
    }

    /**
     * Deserializes a column
     * @param j serialized column
     */
    void FromJson(const nlohmann::json &j) {
      name_ = j.at("name").get<std::string>();
      type_ = j.at("type").get<type::TypeId>();
      attr_size_ = j.at("attr_size").get<uint8_t>();
      max_varlen_size_ = j.at("max_varlen_size").get<uint16_t>();
      nullable_ = j.at("nullable").get<bool>();
      oid_ = j.at("oid").get<col_oid_t>();
    }

   private:
    std::string name_;
    type::TypeId type_;
    uint8_t attr_size_;
    uint16_t max_varlen_size_;
    bool nullable_;
    col_oid_t oid_;
    // TODO(Matt): default value would go here. requires pg_attribute to be implemented?
    // common::ManagedPointer<parser::AbstractExpression> default_value_;

    void SetOid(col_oid_t oid) { oid_ = oid; }

    friend class DatabaseCatalog;
    friend class postgres::Builder;
  };

  /**
   * Instantiates a Schema object from a vector of previously-defined Columns
   * @param columns description of this SQL table's schema as a collection of Columns
   */
  explicit Schema(std::vector<Column> columns) : columns_(std::move(columns)) {
    TERRIER_ASSERT(!columns_.empty() && columns_.size() <= common::Constants::MAX_COL,
                   "Number of columns must be between 1 and MAX_COL.");
    for (uint32_t i = 0; i < columns_.size(); i++) {
      // If not all columns assigned OIDs, then clear the map because this is
      // a definition of a new/modified table not a catalog generated schema.
      if (columns_[i].GetOid() == catalog::INVALID_COLUMN_OID) {
        col_oid_to_offset.clear();
        return;
      }
      col_oid_to_offset[columns_[i].GetOid()] = i;
    }
  }

  /**
   * Default constructor used for deserialization
   */
  Schema() = default;

  /**
   * @param col_offset offset into the schema specifying which Column to access
   * @return description of the schema for a specific column
   */
  Column GetColumn(const uint32_t col_offset) const {
    TERRIER_ASSERT(col_offset < columns_.size(), "column id is out of bounds for this Schema");
    return columns_[col_offset];
  }
  /**
   * @param col_oid identifier of a Column in the schema
   * @return description of the schema for a specific column
   */
  Column GetColumn(const col_oid_t col_oid) const {
    TERRIER_ASSERT(col_oid_to_offset.count(col_oid) > 0, "col_oid does not exist in this Schema");
    const uint32_t col_offset = col_oid_to_offset.at(col_oid);
    return columns_[col_offset];
  }

  /**
   * @param name name of the Column to access
   * @return description of the schema for a specific column
   * @throw std::out_of_range if the column doesn't exist.
   */
  Column GetColumn(const std::string &name) const {
    for (auto &c : columns_) {
      if (c.GetName() == name) {
        return c;
      }
    }
    // TODO(John): Should this be a TERRIER_ASSERT to have the same semantics
    // as the other accessor methods above?
    throw std::out_of_range("Column name doesn't exist");
  }
  /**
   * @return description of this SQL table's schema as a collection of Columns
   */
  const std::vector<Column> &GetColumns() const { return columns_; }

  /**
   * @return serialized schema
   */
  nlohmann::json ToJson() const {
    // Only need to serialize columns_ because col_oid_to_offset is derived from columns_
    nlohmann::json j;
    j["columns"] = columns_;
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
  std::shared_ptr<Schema> static DeserializeSchema(const nlohmann::json &j) {
    auto columns = j.at("columns").get<std::vector<Schema::Column>>();
    return std::make_shared<Schema>(columns);
  }

 private:
  friend class DatabaseCatalog;
  std::vector<Column> columns_;
  std::unordered_map<col_oid_t, uint32_t> col_oid_to_offset;
};

DEFINE_JSON_DECLARATIONS(Schema::Column);
DEFINE_JSON_DECLARATIONS(Schema);

}  // namespace terrier::catalog
