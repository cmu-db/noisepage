#pragma once
#include <string>

namespace terrier {

int peloton_close(int fd);

std::string peloton_error_message();
}  // namespace terrier
