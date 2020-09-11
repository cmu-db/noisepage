import math

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
        return math.ceil(float(value))


def round_to_interval(time, interval):
    """ Round a timestamp based on interval

    :param time: in us
    :param interval: in us
    :return: time in us rounded to the closest interval ahead
    """
    return time - time % interval

