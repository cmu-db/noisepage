#pragma once
#include "common/exception.h"
#include "type/transient_value.h"
#include "type/transient_value_factory.h"
#include "type/transient_value_peeker.h"

namespace terrier::catalog {
/**
 * Utility class for TransientValues
 * TODO(Yesheng): move to TransientValueFactory?
 */
class TransientValueUtil {
 public:
  /**
   * @param value TransientValue
   * @return a copied TransientValue
   */
  static type::TransientValue MakeCopy(const type::TransientValue &value) {
    // NOLINTNEXTLINE
    switch (value.Type()) {
      case type::TypeId::BOOLEAN:
        return type::TransientValueFactory::GetBoolean(type::TransientValuePeeker::PeekBoolean(value));
        break;
      case type::TypeId::TINYINT:
        return type::TransientValueFactory::GetTinyInt(type::TransientValuePeeker::PeekTinyInt(value));
        break;
      case type::TypeId::SMALLINT:
        return type::TransientValueFactory::GetSmallInt(type::TransientValuePeeker::PeekSmallInt(value));
        break;
      case type::TypeId::INTEGER:
        return type::TransientValueFactory::GetInteger(type::TransientValuePeeker::PeekInteger(value));
        break;
      case type::TypeId::BIGINT:
        return type::TransientValueFactory::GetBigInt(type::TransientValuePeeker::PeekBigInt(value));
        break;
      case type::TypeId::DECIMAL:
        return type::TransientValueFactory::GetDecimal(type::TransientValuePeeker::PeekDecimal(value));
        break;
      case type::TypeId::TIMESTAMP:
        return type::TransientValueFactory::GetTimestamp(type::TransientValuePeeker::PeekTimestamp(value));
        break;
      case type::TypeId::DATE:
        return type::TransientValueFactory::GetDate(type::TransientValuePeeker::PeekDate(value));
        break;
      case type::TypeId::VARCHAR:
        return type::TransientValueFactory::GetVarChar(type::TransientValuePeeker::PeekVarChar(value));
        break;
      default:
        throw CATALOG_EXCEPTION("invalid TransientValue copy.");
        break;
    }
  }
};

}  // namespace terrier::catalog
