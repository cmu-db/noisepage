#!/usr/bin/python3

import xml.etree.ElementTree as xml
import json


def parse_config_file(path):
    """
    Read data from file ends with ".expconfig".

    Args:
        path (str): The location of the expconfig file.

    Returns:
        parameters (json): Information about the parameters the test was run with.
                It contains the follow attributes.
            client_time (int): The length of time the text executed
            transaction_weights (json array): The weight used for benchmark.

    """
    config_root = xml.parse(path).getroot()
    return {
        # The current API uses duration. When we update the schema we can flip this back
        'client_time': int(parse_client_time(config_root)),
        # 'client_time': int(parse_client_time(config_root)),
        'transaction_weights': parse_transaction_weights(config_root)
    }


def parse_client_time(config_root):
    try:
        return config_root.find('works').find('work').find('time').text
    except:
        raise KeyError('Could not parse config xml for time')


def parse_transaction_weights(config_root):
    """
    Find the transaction types and weights in the xml and match up the 
    weight with its transaction type

    Args:
        config_root( xml.Element): The root of the config xml

    Returns:
        transaction_weights (json array): An array of objects formatted
        as {"name": "transaction name (str)", weight: value(int)}
    """
    try:
        transaction_types = config_root.find(
            'transactiontypes').findall('transactiontype')
        weights_values = config_root.find(
            'works').find('work').findall('weights')
    except:
        raise KeyError(
            'Could not parse config xml for transaction types or weights')

    if len(transaction_types) != len(weights_values):
        raise RuntimeError(
            'number of transaction types do not match the number of weights')

    transaction_weights_xml = zip(transaction_types, weights_values)
    transaction_weights = []
    for transaction_type, weight in transaction_weights_xml:
        transaction_name = transaction_type.find('name').text
        weight_value = int(weight.text)
        transaction_weights.append(
            {'name': transaction_name, 'weight': weight_value})

    return transaction_weights
