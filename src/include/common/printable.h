#pragma once

#include <iosfwd>
#include <string>

namespace terrier {
/**
 * Stateless interface to allow printing of debug information
 *
 * Most stateful classes should implement this interface and return useful
 * information about its state for debugging purposes.
 */
class Printable {
 public:
  virtual ~Printable() = default;;

  /** @brief Return information about the object in human-readable way. */
  virtual const std::string GetInfo() const = 0;

  friend std::ostream &operator<<(std::ostream &os, const Printable &printable);
};

}