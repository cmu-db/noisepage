#include <sstream>

#include "common/printable.h"

namespace poopdish {

// Get a string representation for debugging
std::ostream &operator<<(std::ostream &os, const Printable &printable) {
  os << printable.GetInfo();
  return os;
};

}  // namespace poopdish