import argparse
import re
from typing import List


def scrape_logs(input_file: str, replica_name: str, output_file: str):
    """
    Uses database logs to scrape messages sent from a primary NoisePage server to a replica NoisePage server

    :param input_file file containing the database logs
    :param replica_name identity name of replica that messages should be scraped for
    :param output_file where to save the scraped messages
    """
    msgs = extract_msgs(input_file, replica_name)
    with open(output_file, 'w') as f:
        f.writelines(msgs)


def extract_msgs(input_file: str, replica_name: str) -> List[str]:
    """
    Uses database logs to extract messages from primary NoisePage server to a replica NoisePage server

    :param input_file file containing the database logs
    :param replica_name identity name of replica that messages should be scraped for

    :return list of messages
    """
    with open(input_file, 'r') as f:
        logs = f.readlines()
        message_logs = [log for log in logs if f"SENT-TO {replica_name}" in log]
        msgs = [f"{extract_msg(log, replica_name)}\n" for log in message_logs]
        # Filters out empty strings
        return list(filter(None, msgs))


def extract_msg(log: str, replica_name: str) -> str:
    """
    Extracts a message from a single log

    :param log full log string
    :param replica_name identity name of replica

    :return message sent from primary to replica
    """
    pattern = rf".*SENT-TO {replica_name}: (\d+-\d+-\d+-.*)"
    matches = re.findall(pattern, log)
    if len(matches) != 1:
        print(f"Unknown log format: {log}")
        return ""
    return matches[0]


def main():
    """
    This file helps scrape logs produced by the Messenger component of NoisePage. It will go through the logs and extract
    all the messages that were sent to the primary and save them in a separate file. This is useful for being able to resend
    messages to replica nodes without having an actual primary node.

    To generate messenger logs just add the following line to Messenger::RunTask()
        messenger_logger->set_level(spdlog::level::trace);
    """
    aparser = argparse.ArgumentParser(description="Messenger log scraper")
    aparser.add_argument("--input-log-file",
                         required=True,
                         help="Location of log file")
    aparser.add_argument("--output-file",
                         default="log-messages.txt",
                         help="Location to save log messages")
    aparser.add_argument("--replica-name",
                         default="replica1",
                         help="Name of replica node in logs")

    args = vars(aparser.parse_args())

    scrape_logs(args["input_log_file"], args["replica_name"], args["output_file"])


if __name__ == '__main__':
    main()
