#include <sstream>

#include "common/printable.h"

namespace terrier {

// Get a string representation for debugging
std::ostream &operator<<(std::ostream &os, const Printable &printable) {
  os << printable.GetInfo();
  return os;
};

}