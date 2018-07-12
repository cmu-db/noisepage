#pragma
#include <sstream>
#include "common/printable.h"
#include "common/common_defs.h"

namespace terrier {
STRONG_TYPEDEF(block_id_t, uint32_t)

struct TupleId : public Printable {
  block_id_t block_id_;
  uint32_t offset_;

  const std::string GetInfo() const override {
    std::ostringstream out;
    out << "block id: " << block_id_ << ", offset: " << offset_ << std::endl;
    return out.str();
  }
};

}
