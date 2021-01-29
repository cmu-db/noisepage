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
#include "parser/expression/constant_value_expression.h"
#include "type/type_id.h"
#include "type/type_util.h"

namespace noisepage {
class StorageTestUtil;
}  // namespace noisepage

namespace noisepage::catalog {

namespace postgres {
class Builder;
class PgCoreImpl;
}  // namespace postgres

/**
 * Internal object for representing SQL table schema
 */
class Schema {
 public:
  /**
   * Internal object for representing SQL table column
   */
  class Column {
   public:
    // TODO(Matt): does having two constructors even make sense anymore?

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
          attr_length_(type::TypeUtil::GetTypeSize(type_)),
          nullable_(nullable),
          oid_(INVALID_COLUMN_OID),
          default_value_(default_value.Copy()) {
      Validate();
    }

    /**
     * Instantiates a Column object, primary to be used for building a Schema object (VARLEN attributes only)
     * @param name column name
     * @param type SQL type for this column
     * @param type_modifier the maximum varlen size or precision for decimal
     * @param nullable true if the column is nullable, false otherwise
     * @param default_value for the column
     */
    Column(std::string name, const type::TypeId type, const int32_t type_modifier, const bool nullable,
           const parser::AbstractExpression &default_value)
        : name_(std::move(name)),
          type_(type),
          attr_length_(type::TypeUtil::GetTypeSize(type_)),
          type_modifier_(type_modifier),
          nullable_(nullable),
          oid_(INVALID_COLUMN_OID),
          default_value_(default_value.Copy()) {
      Validate();
    }

    /**
     * Overrides default copy constructor to ensure we do a deep copy on the abstract expressions
     * @param old_column to be copied
     */
    Column(const Column &old_column)
        : name_(old_column.name_),
          type_(old_column.type_),
          attr_length_(old_column.attr_length_),
          type_modifier_(old_column.type_modifier_),
          nullable_(old_column.nullable_),
          oid_(old_column.oid_),
          default_value_(old_column.default_value_->Copy()) {
      Validate();
    }

    /**
     * Allows operator= to call Column's custom copy-constructor.
     * @param col column to be copied
     * @return the current column after update
     */
    Column &operator=(const Column &col) {
      name_ = col.name_;
      type_ = col.type_;
      attr_length_ = col.attr_length_;
      type_modifier_ = col.type_modifier_;
      nullable_ = col.nullable_;
      oid_ = col.oid_;
      default_value_ = col.default_value_->Copy();
      Validate();
      return *this;
    }

    /**
     * @return column name
     */
    const std::string &Name() const { return name_; }

    /**
     * @return true if the column is nullable, false otherwise
     */
    bool Nullable() const { return nullable_; }

    /**
     * @return size of the attribute in bytes. Varlen attributes have the sign bit set.
     */
    uint16_t AttributeLength() const { return attr_length_; }

    /**
     * @return The maximum length of this column (only valid if it's VARLEN)
     */
    int32_t TypeModifier() const { return type_modifier_; }

    /**
     * @return SQL type for this column
     */
    type::TypeId Type() const { return type_; }

    /**
     * @return internal unique identifier for this column
     */
    col_oid_t Oid() const { return oid_; }

    /**
     * @return default value expression
     */
    common::ManagedPointer<const parser::AbstractExpression> StoredExpression() const {
      return common::ManagedPointer(default_value_).CastManagedPointerTo<const parser::AbstractExpression>();
    }

    /**
     * Default constructor for deserialization
     */
    Column() = default;

    /**
     * @return the hashed value of this column
     */
    common::hash_t Hash() const {
      common::hash_t hash = common::HashUtil::Hash(name_);
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(type_));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(attr_length_));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(type_modifier_));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(nullable_));
      hash = common::HashUtil::CombineHashes(hash, common::HashUtil::Hash(oid_));
      if (default_value_ != nullptr) hash = common::HashUtil::CombineHashes(hash, default_value_->Hash());
      return hash;
    }

    /**
     * Perform a comparison of column
     * @param rhs other column to compare against
     * @return true if the two columns are equal
     */
    bool operator==(const Column &rhs) const {
      if (name_ != rhs.name_) return false;
      if (type_ != rhs.type_) return false;
      if (attr_length_ != rhs.attr_length_) return false;
      if ((ShouldHaveTypeModifier() != rhs.ShouldHaveTypeModifier()) ||
          ((ShouldHaveTypeModifier() && rhs.ShouldHaveTypeModifier()) && type_modifier_ != rhs.type_modifier_))
        return false;
      if (nullable_ != rhs.nullable_) return false;
      if (oid_ != rhs.oid_) return false;
      if (default_value_ == nullptr) return rhs.default_value_ == nullptr;
      return rhs.default_value_ != nullptr && *default_value_ == *rhs.default_value_;
    }

    /**
     * Perform a comparison of column
     * @param rhs other column to compare against
     * @return true if the two columns are not equal
     */
    bool operator!=(const Column &rhs) const { return !operator==(rhs); }

    /**
     * @return column serialized to json
     */
    nlohmann::json ToJson() const;

    /**
     * Deserializes a column
     * @param j serialized column
     */
    std::vector<std::unique_ptr<parser::AbstractExpression>> FromJson(const nlohmann::json &j);

   private:
    bool ShouldHaveTypeModifier() const {
      return type_ == type::TypeId::VARCHAR || type_ == type::TypeId::VARBINARY || type_ == type::TypeId::DECIMAL;
    }

    void Validate() const {
      NOISEPAGE_ASSERT(type_ != type::TypeId::INVALID, "Attribute type cannot be INVALID.");
      NOISEPAGE_ASSERT(default_value_ == nullptr || default_value_->GetReturnValueType() != type::TypeId::INVALID ||
                           (default_value_->GetReturnValueType() == type::TypeId::INVALID &&
                            common::ManagedPointer(default_value_)
                                .CastManagedPointerTo<parser::ConstantValueExpression>()
                                ->IsNull()),
                       "Default value either: 1) shouldn't exist 2) shouldn't have INVALID type 3) UNLESS it's NULL.");
      // TODO(Matt): I don't love that last part that NULL default values come out of the parser with TypeId::INVALID.

      if (type_ == type::TypeId::VARCHAR || type_ == type::TypeId::VARBINARY) {
        NOISEPAGE_ASSERT(attr_length_ == storage::VARLEN_COLUMN, "Invalid attribute length.");
        NOISEPAGE_ASSERT(type_modifier_ == -1 || type_modifier_ > 0,
                         "Type modifier should be -1 (no limit), or a positive integer.");
      } else if (type_ == type::TypeId::DECIMAL) {
        NOISEPAGE_ASSERT(attr_length_ == 16, "Invalid attribute length.");
        NOISEPAGE_ASSERT(type_modifier_ > 0, "Type modifier should be a  positive integer.");
      } else {
        NOISEPAGE_ASSERT(attr_length_ == 1 || attr_length_ == 2 || attr_length_ == 4 || attr_length_ == 8,
                         "Invalid attribute length.");
        NOISEPAGE_ASSERT(type_modifier_ == -1, "Invalid attribute modifier. Should be -1 for types that don't use it.");
      }
    }

    std::string name_;
    type::TypeId type_;
    uint16_t attr_length_;
    int32_t type_modifier_ = -1;  // corresponds to Postgres' atttypmod int4: atttypmod records type-specific data
                                  // supplied at table creation time (for example, the maximum length of a varchar
                                  // column). It is passed to type-specific input functions and length coercion
                                  // functions. The value will generally be -1 for types that do not need atttypmod.
    bool nullable_;
    col_oid_t oid_;

    std::unique_ptr<parser::AbstractExpression> default_value_;

    void SetOid(col_oid_t oid) { oid_ = oid; }

    friend class DatabaseCatalog;
    friend class postgres::Builder;
    friend class postgres::PgCoreImpl;

    friend class noisepage::StorageTestUtil;
  };

  /**
   * Instantiates a Schema object from a vector of previously-defined Columns
   * @param columns description of this SQL table's schema as a collection of Columns
   */
  explicit Schema(std::vector<Column> columns) : columns_(std::move(columns)) {
    NOISEPAGE_ASSERT(!columns_.empty() && columns_.size() <= common::Constants::MAX_COL,
                     "Number of columns must be between 1 and MAX_COL.");
    for (uint32_t i = 0; i < columns_.size(); i++) {
      // If not all columns assigned OIDs, then clear the map because this is
      // a definition of a new/modified table not a catalog generated schema.
      if (columns_[i].Oid() == catalog::INVALID_COLUMN_OID) {
        col_oid_to_offset_.clear();
        return;
      }
      col_oid_to_offset_[columns_[i].Oid()] = i;
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
  const Column &GetColumn(const uint32_t col_offset) const {
    NOISEPAGE_ASSERT(col_offset < columns_.size(), "column id is out of bounds for this Schema");
    return columns_[col_offset];
  }
  /**
   * @param col_oid identifier of a Column in the schema
   * @return description of the schema for a specific column
   */
  const Column &GetColumn(const col_oid_t col_oid) const {
    NOISEPAGE_ASSERT(col_oid_to_offset_.count(col_oid) > 0, "col_oid does not exist in this Schema");
    const uint32_t col_offset = col_oid_to_offset_.at(col_oid);
    return columns_[col_offset];
  }

  /**
   * @param name name of the Column to access
   * @return description of the schema for a specific column
   * @throw std::out_of_range if the column doesn't exist.
   */
  const Column &GetColumn(const std::string &name) const {
    for (const auto &c : columns_) {
      if (c.Name() == name) {
        return c;
      }
    }
    // TODO(John): Should this be a NOISEPAGE_ASSERT to have the same semantics
    // as the other accessor methods above?
    //  (Ling): Probably not? Or is there a proper way for binder to check if a column exists?
    throw std::out_of_range("Column name doesn't exist");
  }
  /**
   * @return description of this SQL table's schema as a collection of Columns
   */
  const std::vector<Column> &GetColumns() const { return columns_; }

  /**
   * @return serialized schema
   */
  nlohmann::json ToJson() const;

  /**
   * Should not be used. See NOISEPAGE_ASSERT
   */
  void FromJson(const nlohmann::json &j);

  /**
   * Deserialize a schema
   * @param j json containing serialized schema
   * @return deserialized schema object
   */
  std::unique_ptr<Schema> static DeserializeSchema(const nlohmann::json &j);

  /**
   * @return the hashed value of this schema
   */
  common::hash_t Hash() const {
    common::hash_t hash = common::HashUtil::Hash(col_oid_to_offset_.size());
    for (const auto &col : columns_) hash = common::HashUtil::CombineHashes(hash, col.Hash());
    return hash;
  }

  /**
   * Perform a comparison of schema
   * @param rhs other schema to compare against
   * @return true if the two schema are equal
   */
  bool operator==(const Schema &rhs) const { return columns_ == rhs.columns_; }

  /**
   * Perform a comparison of schema
   * @param rhs other schema to compare against
   * @return true if the two schema are not equal
   */
  bool operator!=(const Schema &rhs) const { return !operator==(rhs); }

 private:
  friend class DatabaseCatalog;
  std::vector<Column> columns_;
  std::unordered_map<col_oid_t, uint32_t> col_oid_to_offset_;
};

DEFINE_JSON_HEADER_DECLARATIONS(Schema::Column);
DEFINE_JSON_HEADER_DECLARATIONS(Schema);

}  // namespace noisepage::catalog
