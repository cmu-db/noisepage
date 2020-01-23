#pragma once

#include "common/managed_pointer.h"
#include "network/postgres/postgres_defs.h"
#include "type/transient_value.h"

namespace terrier::network {

class Portal {
 public:
  const std::vector<FieldFormat> &ResultFormats() const { return result_formats_; }
  common::ManagedPointer<const std::vector<type::TransientValue>> Parameters() {
    return common::ManagedPointer(&parameters_);
  }

 private:
  const std::vector<FieldFormat> result_formats_;
  const std::vector<type::TransientValue> parameters_;
};

}  // namespace terrier::network