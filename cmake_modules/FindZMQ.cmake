# TODO(WAN): I have no fucking idea how cmake works. Someone else check this?

find_package(PkgConfig)
pkg_check_modules(PC_ZeroMQ QUIET zmq)

find_path(ZMQ_INCLUDE_DIR NAMES zmq.hpp PATHS ${PC_ZeroMQ_INCLUDE_DIRS})
find_library(ZMQ_LIBRARIES NAMES zmq PATH ${PC_ZeroMQ_LIBRARY_DIRS})