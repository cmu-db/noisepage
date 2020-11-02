#!/usr/bin/python3

def get_value_by_pattern(dict_obj, pattern, default):
    """
    This is similar to .get() for a dict but it matches the
    key based on a substring. This function is case insensitive.
    """
    for key, value in dict_obj.items():
        if pattern.lower() in key.lower():
            return value
    return default
