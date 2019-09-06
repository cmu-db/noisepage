#pragma once
#include <string>

namespace terrier {

int TerrierClose(int fd);

std::string TerrierErrorMessage();
}  // namespace terrier
