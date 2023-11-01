from __future__ import annotations
import sys


class Log:
    LINE_SEPARATOR = '\n'
    output = sys.stdout
    disabled = False

    @classmethod
    def print(cls, message):
        if not cls.is_disabled():
            try:
                cls.get_output().write(message)
            except IOError as e:
                print(e)

    @classmethod
    def print_line(cls, message=''):
        if not cls.is_disabled():
            cls.print(message + cls.LINE_SEPARATOR)

    @classmethod
    def format(cls, format_string, *args):
        if not cls.is_disabled():
            cls.print(format_string % args)

    @classmethod
    def format_line(cls, format_string, *args):
        if not cls.is_disabled():
            cls.print_line(format_string % args)

    @classmethod
    def set_output(cls, _output):
        cls.output = _output

    @classmethod
    def get_output(cls):
        if cls.output is None:
            cls.set_output(sys.stdout)
        return cls.output

    @classmethod
    def set_disabled(cls, _disabled):
        cls.disabled = _disabled

    @classmethod
    def is_disabled(cls):
        return cls.disabled

    @classmethod
    def disable(cls):
        cls.set_disabled(True)

    @classmethod
    def enable(cls):
        cls.set_disabled(False)
