#pragma once

#include <string>

#include "catalog/postgres/pg_defs.h"

namespace terrier::catalog::postgres {
/* NameBuilder generates names for database objects such as constraints and indexes
 *
 * Following Postgres naming convention
 * */
class NameBuilder {
 public:
    /*
     * Construct an object name
     * @param table_name name of the table to get foreign key constraints
     * @param field_name name of the field
     * @param type name of the type
     * @return name of the object
     * */
    static std::string MakeName(const std::string &table_name,
                                const std::string &field_name,
                                const std::string &type) {
      /* initial values */
      auto underscore = 0;
      auto table_name_length = table_name.length();
      auto field_name_length = field_name.length();
      auto type_length = type.length();
      auto total_length = table_name_length;
      /* check field_name and type */
      if (field_name_length != 0) {
        total_length += field_name_length;
        underscore ++;
      }
      if (type_length != 0) {
        total_length += type_length;
        underscore++;
      }

      /* truncate to the max length */
      while (total_length + underscore > MAX_NAME_LENGTH) {
        if (table_name_length > field_name_length) table_name_length --;
        else field_name_length --;
        total_length = table_name_length + field_name_length + type_length;
      }

      /* construct final string */
      auto name = table_name.substr(0, table_name_length);
      if (field_name_length != 0) {
        name += "_" + field_name.substr(0, field_name_length);
      }
      if (type_length != 0) {
        name += "_" + type;
      }

      return name;

    }

};
}
