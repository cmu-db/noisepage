#pragma once
#include "network/abstract_network_command.h"

namespace terrier::network {

// Set all to force flush for now
DEFINE_COMMAND(SimpleQueryCommand, true);
DEFINE_COMMAND(ParseCommand, true);
DEFINE_COMMAND(BindCommand, true);
DEFINE_COMMAND(DescribeCommand, true);
DEFINE_COMMAND(ExecuteCommand, true);
DEFINE_COMMAND(SyncCommand, true);
DEFINE_COMMAND(CloseCommand, true);
DEFINE_COMMAND(TerminateCommand, true);

DEFINE_COMMAND(EmptyCommand, true);

}  // namespace terrier::network
