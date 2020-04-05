#pragma once

#include <memory>
#include <vector>
#include "storage/sql_table.h"

namespace terrier::execution::sql {

class CteScanIterator {
 public:
  /**
   */
  explicit CteScanIterator() {

  }

  /**
   * Destructor
   */
  ~CteScanIterator();

  /**
   * This class cannot be copied or moved
   */
  DISALLOW_COPY_AND_MOVE(CteScanIterator);

  /**
   * Initialize the iterator, returning true if the initialization succeeded
   * @return True if the initialization succeeded; false otherwise
   */
  storage::TupleSlot Next(storage::TupleSlot input) {
    return input;
  }


 private:
};

}  // namespace terrier::execution::sql
