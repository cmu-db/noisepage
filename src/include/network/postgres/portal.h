#pragma once

#include "network/postgres/postgres_defs.h"
#include "type/transient_value.h"

namespace terrier::network {

class Portal {
 public:
  const std::vector<FieldFormat> &ResultFormats() const { return result_formats_; }

 private:
  const std::vector<FieldFormat> result_formats_;
  const std::vector<type::TransientValue> parameters_;
};

}  // namespace terrier::network