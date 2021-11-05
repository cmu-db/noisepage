#pragma once

#include "common/macros.h"

namespace noisepage::common {

constexpr std::string_view NOISEPAGE_NAME = "NoisePage";
// TODO(WAN): There used to be a fragile hack in parse_data.py that would try to regex out the version number.
//  Please update script/testing/reporting/parsers/parse_data.py manually if you change this version number.
//  And also script/testing/reporting/parsers/summary_parser.py.
constexpr std::string_view NOISEPAGE_VERSION = "1.0.0";
constexpr std::string_view NOISEPAGE_VERSION_STR = "NoisePage 1.0.0";

}  // namespace noisepage::common
