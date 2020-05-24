#pragma once

namespace terrier::common {

/**
 * Base for classes that should NOT be instantiated, i.e., classes that only have static functions.
 */
class AllStatic {
#ifndef NDEBUG
 public:
  AllStatic() = delete;
#endif
};

}  // namespace terrier::common