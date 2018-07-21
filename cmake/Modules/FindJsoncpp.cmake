# - Try to find jsoncpp headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(Jsoncpp)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  JSONCPP_ROOT_DIR Set this variable to the root installation of
#                   jsoncpp if the module has problems finding
#                   the proper installation path.
#
# Variables defined by this module:
#
#  JSONCPP_FOUND             System has jsoncpp libs/headers
#  JSONCPP_LIBRARIES         The jemalloc library/libraries
#  JSONCPP_INCLUDE_DIR       The location of jsoncpp headers

find_path(JSONCPP_ROOT_DIR
    NAMES include/jsoncpp/json/json.h
)

find_library(JSONCPP_LIBRARIES
    NAMES jsoncpp
    HINTS ${JSONCPP_ROOT_DIR}/lib
)

find_path(JSONCPP_INCLUDE_DIR
    NAMES jsoncpp/json/json.h
    HINTS ${JSONCPP_ROOT_DIR}/include
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Jsoncpp_lib DEFAULT_MSG
    JSONCPP_LIBRARIES
    JSONCPP_INCLUDE_DIR
)

mark_as_advanced(
    JSONCPP_ROOT_DIR
    JSONCPP_LIBRARIES
    JSONCPP_INCLUDE_DIR
)

message(STATUS "Found jsoncpp (include: ${JSONCPP_INCLUDE_DIRS}, library: ${JSONCPP_LIBRARIES})")
