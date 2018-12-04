#pragma once

#include <memory>
#include "storage/sql_table.h"
namespace terrier::catalog {

class DatabaseHandle {
 public:
  DatabaseHandle();
  ~DatabaseHandle(){

  };

  void BootStrap();

 private:
};

}  // namespace terrier::catalog
