import re
import unicodedata

from more_itertools import peekable


class Format:
    def __init__(self, value, fixup=False):
        self.value = value
        self.formatted = self.value
        self.fixup = fixup

    @property
    def snake_case(self):
        s = self.value
        if isinstance(s, str) and not self.is_snake_case:
            s = Format.convert_symbols_to_words(s)
            s = Format.as_ascii(s)
            s = Format.upper_case_to_space_lower_case(s)
            s = Format.alpha_numeric_and_spaces_only(s)
            s = Format.only_one_space_between_words(s)
            s = s.lower()
            s = s.strip()
            s = s.replace(' ', '_')
        return s

    @property
    def camel_case(self):
        s = self.value
        if isinstance(s, str) and not self.is_camel_case:
            s = Format.convert_symbols_to_words(s)
            s = Format.as_ascii(s)
            s = Format.non_alpha_numeric_to_spaces(s)
            s = Format.upper_case_to_space_lower_case(s)
            s = Format.alpha_numeric_and_spaces_only(s)
            s = Format.only_one_space_between_words(s)
            s = Format.space_then_char_to_upper(s)
            s = Format.remove_spaces(s)
        return s

    @property
    def name(self):
        s = self.value
        if isinstance(s, str):
            s = Format.convert_symbols_to_words(s)
            s = Format.as_ascii(s)
            s = Format.only_one_space_between_words(s)
        return s

    @property
    def title_case(self):
        s = self.name
        s = self.titlecase(s)
        return s

    @property
    def class_case(self):
        s = self.title_case
        s = self.remove_spaces(s)
        return s

    @property
    def lower(self):
        s = self.value
        if isinstance(s, str):
            s = s.lower()
        return s

    @property
    def db_lookup(self):
        """
                no extra spaces or special chars or newlines
                Returns:

                """
        s = self.value
        if isinstance(s, str):
            s = Format.remove_newlines(s)
            s = Format.remove_monday_newlines(s)
            s = Format.as_ascii(s)
            s = Format.only_one_space_between_words(s)
        return s

    @property
    def clean(self):
        """
        no extra spaces or special chars or newlines
        Returns:

        """
        s = self.value
        if isinstance(s, str):
            s = Format.remove_newlines(s)
            s = Format.remove_monday_newlines(s)
            s = Format.convert_symbols_to_words(s)
            s = Format.as_ascii(s)
            s = Format.alpha_numeric_and_spaces_only(s)
            s = Format.only_one_space_between_words(s)
        return s

    @property
    def ascii(self):
        return Format.as_ascii(self.value)

    @property
    def text(self):
        return Format.as_ascii(self.value)

    @property
    def is_snake_case(self):
        a = self.value == self.value.lower()
        b = not bool(re.search(r"\s", self.value))
        c = self.as_ascii(self.value) == self.convert_symbols_to_words(self.value)
        return a and b and c

    @property
    def is_camel_case(self):
        a = self.value == self.camel_case
        return a

    @staticmethod
    def convert_symbols_to_words(s) -> str:
        if isinstance(s, str):
            s = s.replace('\u2013', '-')
            s = s.replace('&', ' and ')
            s = s.replace('|', ' or ')
            s = s.replace('-', ' ')
            s = s.replace('?', ' Q ')
        return s

    @staticmethod
    def only_one_space_between_words(s):
        if isinstance(s, str):
            s = re.sub(" +", " ", s)
        return s

    @staticmethod
    def as_ascii(s):
        if isinstance(s, str):
            s = unicodedata.normalize(u'NFKD', s).encode('ascii', 'ignore').decode('utf-8')
        return s

    @staticmethod
    def remove_spaces(s):
        if isinstance(s, str):
            s = s.replace(' ', '')
        return s

    @staticmethod
    def remove_newlines(s):
        if isinstance(s, str):
            s = re.sub('\n', '', s)
        return s

    @staticmethod
    def remove_monday_newlines(s):
        if isinstance(s, str):
            s = re.sub('\\\\n', '', s)
        return s

    @staticmethod
    def upper_case_to_space_lower_case(s):
        v = s
        if isinstance(s, str):
            s = peekable(s)
            v = ''
            while s:
                c = next(s)
                z = s.peek(' ')
                if c.isupper():
                    if z != ' ' and not z.isupper():
                        c = f" {c.lower()}"
                v += c.lower()
        return v

    @staticmethod
    def space_then_char_to_upper(s):
        v = s
        if isinstance(s, str):
            s = peekable(s)
            v = ''
            while s:
                c = next(s)
                z = s.peek(' ')
                if c == ' ' and z != ' ':
                    v += z.upper()
                    next(s)
                v += c.lower()
        return v

    @staticmethod
    def titlecase(s):
        return re.sub(r"[A-Za-z]+('[A-Za-z]+)?", lambda word: word.group(0).capitalize(), s)

    @staticmethod
    def alpha_numeric_and_spaces_only(s):
        if isinstance(s, str):
            s = re.sub('[^A-Za-z0-9 ]+', '', s)
        return s

    @staticmethod
    def netsuite_class_safe_search(s):
        s = str(s)
        s = Format.as_ascii(s)
        s = re.sub('[^A-Za-z0-9:\\-/\'.()&,# ]+', '', s)
        return s

    @staticmethod
    def netsuite_class_chars_only(s):
        s = Format.as_ascii(s)
        return s

    @staticmethod
    def netsuite_class_code(s):
        s = re.sub('[^A-Z0-9&]+', '', s)
        return s

    @staticmethod
    def non_alpha_numeric_to_spaces(s):
        if isinstance(s, str):
            s1 = s
            y = re.sub('[A-Za-z0-9]+', '', s1)
            for c in y:
                s = s.replace(c, ' ')
        return s
