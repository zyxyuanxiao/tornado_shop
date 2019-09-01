# coding:utf-8
import decimal


def dict_decimal_to_float(d):
    if not isinstance(d, dict):
        raise TypeError("d must be dict")
    for key, value in d.items():
        if isinstance(value, decimal.Decimal):
            d[key] = float(value)
    return d
