from __future__ import annotations

from enum import Enum
from typing import List


class FileType(Enum):
    NONE = 0
    INPUT = 1
    OUTPUT = 2

    def __str__(self):
        return f"{self.name}"
    
    def __repr__(self):
        return f"{self.name}"


class FileItem:
    def __init__(self, name: str, size: float):
        self.name = name
        self.size = size
        self.type = FileType.NONE


    def __repr__(self):
        return f"Name: {self.name}, Size: {self.size}, FileType: {self.type}"


    def __str__(self):
        return f"Name: {self.name}, Size: {self.size}, FileType: {self.type}"


    def set_name(self, name: str):
        self.name = name


    def set_size(self, size: float):
        self.size = size


    def set_type(self, file_type: FileType):
        self.type = file_type


    def get_name(self) -> str:
        return self.name
    

    def get_size(self) -> float:
        return self.size
    

    def get_type(self) -> FileType:
        return self.type


    def is_real_input_file(self, file_list: List['FileItem']) -> bool:
        if self.get_type() == FileType.INPUT:
            for another in file_list:
                if another.get_name() == self.get_name() and another.get_type() == FileType.OUTPUT:
                    return False
            return True
        return False