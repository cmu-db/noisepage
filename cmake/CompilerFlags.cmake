# ---------------------------------------------------------
# Default flags
# ---------------------------------------------------------

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall -Wextra -Werror -mcx16 -march=native")

# ---------------------------------------------------------
# Disabled warnings/errors
#
# Please place a reason next to any flags that you add
# ---------------------------------------------------------

# Needed for Google Test
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")

# ---------------------------------------------------------
# Default flags (Debug)
# ---------------------------------------------------------

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -ggdb -fno-omit-frame-pointer -fno-optimize-sibling-calls")

# ---------------------------------------------------------
# Disabled warnings/errors (Debug)
#
# Please place a reason next to any flags that you add
# ---------------------------------------------------------

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wno-strict-aliasing -Wno-implicit-fallthrough -Wno-invalid-offsetof")