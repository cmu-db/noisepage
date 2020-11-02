#pragma once

#include <string>

#include "common/error/exception.h"
#include "network/postgres/postgres_defs.h"

namespace noisepage::catalog::postgres {
/**
 * NameBuilder generates names for database objects such as constraints and indexes
 *
 * Following Postgres naming convention
 */
class NameBuilder {
 public:
  /**
   *  FK: foreign key, KEY: UNIQUE, EMPTY: empty input
   */
  enum ObjectType { FOREIGN_KEY, UNIQUE_KEY, NONE };
  /**
   * Construct an object name
   * @param table_name name of the table to get foreign key constraints
   * @param field_name name of the field
   * @param type name of the type
   * @return name of the object
   */
  static std::string MakeName(const std::string &table_name, const std::string &field_name, const ObjectType type) {
    // initial values
    auto underscore = 0;
    auto table_name_length = table_name.length();
    auto field_name_length = field_name.length();
    auto type_name = GetTypeName(type);
    auto type_length = type_name.length();
    auto total_length = table_name_length;
    // check field_name and type
    if (field_name_length != 0) {
      total_length += field_name_length;
      underscore++;
    }
    if (type_length != 0) {
      total_length += type_length;
      underscore++;
    }

    // truncate to the max length
    while (total_length + underscore > network::MAX_NAME_LENGTH) {
      if (table_name_length > field_name_length)
        table_name_length--;
      else
        field_name_length--;
      total_length = table_name_length + field_name_length + type_length;
    }

    // construct final string
    auto name = table_name.substr(0, table_name_length);
    if (field_name_length != 0) {
      name += "_" + field_name.substr(0, field_name_length);
    }
    if (type_length != 0) {
      name += "_" + type_name;
    }
    return name;
  }

 private:
  /**
   * Extract string from enum class
   * @param type: enum class variable
   * @return corresponding string
   */
  static std::string GetTypeName(const ObjectType type) {
    switch (type) {
      case FOREIGN_KEY:
        return "fkey";
      case UNIQUE_KEY:
        return "key";
      case NONE:
        return "";
      default:
        throw CATALOG_EXCEPTION("Type name not recognized, add it to enum class if needed");
    }
  }
};
}  // namespace noisepage::catalog::postgres
