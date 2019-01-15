#pragma once
#include <string>

namespace terrier {

int terrier_close(int fd);

std::string terrier_error_message();
}  // namespace terrier
