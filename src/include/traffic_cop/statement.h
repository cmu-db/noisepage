#pragma once

#include <sqlite3.h>
#include <vector>
#include "type/transient_value.h"
#include "network/postgres_protocol_utils.h"


namespace terrier::traffic_cop {

class Statement {

 private:
  std::vector<type::TransientValue> params;

};

}
