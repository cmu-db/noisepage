import xml.etree.ElementTree as xml


def parse_config_file(path):
    """
    Read data from a file that ends with ".expconfig".

    Parameters
    ----------
    path : str
        Path to an ".expconfig" file.

    Returns
    -------
    parameters : dict
        The parameters that the test was run with. The keys to the dict are:
        client_time : int
            The duration that the test should be executed for.
        transaction_weights : [dict]
            The weights used for the benchmark.
    """
    config_root = xml.parse(path).getroot()
    return {
        'client_time': parse_client_time(config_root),
        'transaction_weights': parse_transaction_weights(config_root)
    }


def parse_client_time(config_root):
    """
    Parse the client time (the duration that the test should be executed for)
    from the XML configuration.

    Parameters
    ----------
    config_root : xml.Element
        The root of the XML config file.

    Returns
    -------
    client_time : int
        The duration that the test should be executed for.

    Raises
    -------
    KeyError
        If the relevant key (works.work.time) is not present in the XML file.
    """
    try:
        return int(config_root.find('works').find('work').find('time').text)
    except:
        raise KeyError("Couldn't parse the config file for works.work.time.")


def parse_transaction_weights(config_root):
    """
    Parse the transaction types and weights from the XML configuration.

    Parameters
    ----------
    config_root : xml.Element
        The root of the XML config file.

    Returns
    -------
    transaction_weights : [dict]
        An array of dictionaries formatted as
            {"name": str(transaction_name), weight: int(value)}
        that corresponds to the transaction types and weights.

    Raises
    -------
    KeyError
        If works.work.weights or transactiontype is not a key in the XML file.
    RuntimeError
        If there there are a different number of transaction types and weights.
    """
    try:
        transaction_types = \
            config_root.find('transactiontypes').findall('transactiontype')
        weights_values = \
            config_root.find('works').find('work').findall('weights')
    except:
        raise KeyError(
            "Couldn't parse the config file for txn types and weights.")

    if len(transaction_types) != len(weights_values):
        raise RuntimeError("Mismatched number of txn types and weights.")

    weights = []
    for txn_type, weight in zip(transaction_types, weights_values):
        txn_name = txn_type.find('name').text
        weights.append({'name': txn_name, 'weight': int(weight.text)})

    return weights
