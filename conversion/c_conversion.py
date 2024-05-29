"""
Encodes an integer in base 31 (digits + uppercase no vowels)
"""
import logging
import math
from decimal import Decimal



class Conversion:
    BASE31 = "0123456789BCDFGHJKLMNPQRSTVWXYZ"
    BASE36 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

    def to_base31(num: int) -> str:
        """
        Converts an int into a base 31 string
        :param num: the integer we are converting
        :return: a base 31 string
        """
        return Conversion.base10to31(num)
        # # ignore non integer values
        # if type(num) != int:
        #     return ""
        #
        # if num == 0:
        #     return "0"
        #
        # # chars we want to encode to
        # chars_in_convert = string.digits + "".join([char for char in string.ascii_uppercase if char not in "AEIOU"])
        #
        # # map characters
        # base31_map = {i: char for i, char in enumerate(chars_in_convert)}
        #
        # # create encoded string
        # encoded_string = ""  # string we will return
        # while num > 0:
        #     remainder = num % 31
        #     encoded_string = base31_map[remainder] + encoded_string  # add encoded remainder to the left of the string
        #
        #     num = num // 31  # compute next num
        #
        # return encoded_string

    @staticmethod
    # convert to decimal
    def to_decimal(a):
        try:
            return Decimal(a)
        except Exception:
            return Decimal(0)

    @staticmethod
    # convert to int
    def to_int(a):
        try:
            return int(a)
        except Exception:
            return int(0)

    @staticmethod
    def dec_to_base(num, base):  # Maximum base - 36
        num = Conversion.to_int(num)
        base_num = ""
        while num > 0:
            dig = int(num % base)
            if dig < 10:
                base_num += str(dig)
            else:
                base_num += chr(ord('A') + dig - 10)  # Using uppercase letters
            num //= base
        base_num = base_num[::-1]  # To reverse the string
        return base_num

    @staticmethod
    def base10to36(num):  # Maximum base - 36
        num = Conversion.to_int(num)
        base = 36
        base_num = ""
        while num > 0:
            dig = int(num % base)
            base_num += Conversion.BASE36[dig]
            num //= base
        base_num = base_num[::-1]  # To reverse the string
        return base_num

    @staticmethod
    def base36to10(num):  # Maximum base - 36
        num = str(num)
        base = 36
        _number = 0
        for c in num:
            _number = _number * base + Conversion.BASE36.index(c)
        return _number

    @staticmethod
    def base10to31(num):  # Maximum base - 36
        num = Conversion.to_int(num)
        base = 31
        base_num = ""
        while num > 0:
            dig = int(num % base)
            base_num += Conversion.BASE31[dig]
            num //= base
        base_num = base_num[::-1]  # To reverse the string
        return base_num

    @staticmethod
    def base31to10(num):  # Maximum base - 36
        num = str(num)
        base = 31
        _number = 0
        for c in num:
            _number = _number * base + Conversion.BASE31.index(c)
        return _number

    @staticmethod
    def truncate(the_number, decimals=0):
        """
        Returns a value truncated to a specific number of decimal places.
        """
        the_number = Conversion.to_float(the_number)
        if not isinstance(decimals, int):
            raise TypeError("decimal places must be an integer.")
        elif decimals < 0:
            raise ValueError("decimal places has to be 0 or more.")
        elif decimals == 0:
            return math.trunc(the_number)

        factor = 10.0 ** decimals
        return math.trunc(the_number * factor) / factor

    @staticmethod
    def to_money(the_number):
        return Conversion.truncate(the_number, 2)

    @staticmethod
    def to_float(v):
        """
        convert string to float
        :param v:
        :return:
        """
        retval = 0.0
        if v != '' and v is not None:
            try:
                if isinstance(v, str):
                    v = v.replace('w', '')
                    # handle 5.4.2 = 5.42
                    if v.count('.') > 1:
                        s = v.split('.')
                        v = f"{s[0]}."
                        first = True
                        for s0 in s:
                            if first:
                                first = False
                                continue
                            v += s0

                # some values have a trailing period, if so remove it.
                if isinstance(v, str) and '.' in v and v[len(v) - 1] == '.':
                    v = v[0:len(v) - 1]
                retval = float(v)
            except Exception as ex11:
                logging.debug(ex11)
        return retval

    @staticmethod
    def meters_to_inches(v):
        """
        convert meters to inches
        :param v:
        :return:
        """
        retval = 0.0
        v = Conversion.to_float(v)
        if isinstance(v, float):
            retval = v * 39.37
        return retval

    @staticmethod
    def feet_to_inches(v):
        """
        convert feet to inches
        :param v:
        :return:
        """
        retval = 0.0
        try:
            if isinstance(v, str):
                v = Conversion.to_float(v)
            if isinstance(v, float):
                feet_inches = int(v) * 12
                inches_part = (v % 1 * 10)
                retval = feet_inches + inches_part
        except Exception as ex:
            logging.debug(ex)
        return retval


if __name__ == '__main__':
    number = 10000
    x31 = Conversion.base10to31(number)
    x10 = Conversion.base31to10(x31)
    y36 = Conversion.base10to36(number)
    y10 = Conversion.base36to10(y36)
    z = Conversion.to_base31(number)

    print(f"b31 [{x31} = {x10}], b36 [{y36} = {y10}]")
    exit(0)
