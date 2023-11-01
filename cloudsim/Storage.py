from __future__ import annotations

from typing import List, Union
from cloudsim.File import File

class Storage:
    def get_name(self) -> str:
        pass

    def get_capacity(self) -> float:
        pass

    def get_current_size(self) -> float:
        pass

    def get_max_transfer_rate(self) -> float:
        pass

    def get_available_space(self) -> float:
        pass

    def set_max_transfer_rate(self, rate: int) -> bool:
        pass

    def is_full(self) -> bool:
        pass

    def get_num_stored_file(self) -> int:
        pass


    def reserve_space(self, fileSize: int) -> bool:
        pass


    def add_reserved_file(self, file: File) -> float:
        pass


    def has_potential_available_space(self, fileSize: int) -> bool:
        pass


    def get_file(self, fileName: str) -> File:
        pass


    def get_file_name_list(self) -> List[str]:
        pass


    def add_file(self, file) -> float:
        pass


    def add_files(self, fileList: List[File]) -> float:
        pass


    def delete_file(self, file: Union[str, File]) -> File:
        pass


    def delete_file_with_return(self, fileName: str, file) -> float:
        pass


    def delete_file_only(self, file) -> float:
        pass


    def contains(self, fileName: str) -> bool:
        pass


    def contains_file(self, file) -> bool:
        pass


    def rename_file(self, file, newName: str) -> bool:
        pass