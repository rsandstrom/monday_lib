"""
std_utility Functions
"""
from decimal import Decimal


# simple match return true or false if matched
def match(a, b) -> bool:
    if a is None or b is None:
        return False;
    return str(a).lower() == str(b).lower()


# convert to decimal
def convert_to_decimal(a):
    try:
        return Decimal(a)
    except Exception:
        return Decimal(0)


# convert to int
def convert_to_int(a):
    try:
        return int(a)
    except Exception:
        return int(0)


