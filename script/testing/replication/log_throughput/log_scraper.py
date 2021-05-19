import argparse
import re
from typing import Union

from ...util.constants import LOG


def scrape_logs(input_file: str, replica_name: str, output_file: str):
    """
    Uses database logs to scrape messages sent from a primary NoisePage server to a replica NoisePage server

    Parameters
    ----------
    input_file
        file containing the database logs
    replica_name
        identity name of replica that messages should be scraped for
    output_file
        where to save the scraped messages
    """
    with open(input_file, 'r') as in_file, open(output_file, 'w') as out_file:
        for log in in_file:
            if f"SENT-TO {replica_name}" in log:
                msg = extract_msg(log, replica_name)
                if msg:
                    out_file.write(f"{msg}\n")


def extract_msg(log: str, replica_name: str) -> Union[str, None]:
    """
    Extracts a message from a single log

    Parameters
    ----------
    log
        full log string
    replica_name
        identity name of replica

    Returns
    -------
    msg
        message sent from primary to replica
    """
    pattern = rf".*SENT-TO {replica_name}: (\d+-\d+-\d+-.*)"
    matches = re.findall(pattern, log)
    if len(matches) != 1:
        LOG.warn(f"Unknown log format: {log}")
        return None
    return matches[0]


def main():
    """
    This file helps to scrape logs produced by the Messenger component of NoisePage. It will go through the logs and
    extract all the messages that were sent by the primary and save them in a separate file. This is useful for being
    able to resend messages to replica nodes without having an actual primary node.

    To generate messenger logs just add the following line to Messenger::RunTask()
        messenger_logger->set_level(spdlog::level::trace);
    Then run a workload using the primary node. Either copy the logs generated to a file or start the database with
    logs redirected to a file.
    """
    aparser = argparse.ArgumentParser(description="Messenger log scraper")
    aparser.add_argument("--input-log-file",
                         required=True,
                         help="Location of log file")
    aparser.add_argument("--output-file",
                         default="script/testing/replication/log_throughput/resources/log-messages.txt",
                         help="Location to save log messages")
    aparser.add_argument("--replica-name",
                         default="replica1",
                         help="Name of replica node in logs")

    args = vars(aparser.parse_args())

    scrape_logs(args["input_log_file"], args["replica_name"], args["output_file"])


if __name__ == '__main__':
    main()
