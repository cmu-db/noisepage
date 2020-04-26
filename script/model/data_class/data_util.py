def convert_string_to_numeric(value):
    """Break up a string that contains ";" to a list of values

    :param value: the raw string
    :return: a list of int/float values
    """
    if ';' in value:
        return list(map(convert_string_to_numeric, value.split(';')))

    try:
        return int(value)
    finally:
        # Scientific notation
        return int(float(value))
