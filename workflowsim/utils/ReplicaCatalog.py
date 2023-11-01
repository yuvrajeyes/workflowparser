from __future__ import annotations
import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")

from workflowsim.FileItem import FileItem
from typing import List, Dict
from enum import Enum

class ReplicaCatalog:
    class FileSystem(Enum):
        NONE: int = -1
        SHARED: int = 0
        LOCAL: int = 1
    fileSystem: FileSystem = FileSystem.NONE
    fileName2File: Dict[str, FileItem] = dict()
    dataReplicaCatalog: Dict[str, List[str]] = dict()
    
    @staticmethod
    def init(fs: FileSystem):
        ReplicaCatalog.fileSystem = fs
        ReplicaCatalog.fileName2File = dict()
        ReplicaCatalog.dataReplicaCatalog = dict()

    @staticmethod
    def get_file_system() -> FileSystem :
        return ReplicaCatalog.fileSystem

    @staticmethod
    def get_file(fileName) -> FileItem :
        return ReplicaCatalog.fileName2File[fileName]

    @staticmethod
    def set_file(fileName, file) -> None:
        ReplicaCatalog.fileName2File[fileName] = file

    @staticmethod
    def contains_file(fileName: str) -> bool:
        return fileName in ReplicaCatalog.fileName2File

    @staticmethod
    def get_storage_list(file:str) -> List[str]:
        return ReplicaCatalog.dataReplicaCatalog[file]

    @staticmethod
    def add_file_to_storage(file: str, storage: str) -> None:
        if file not in ReplicaCatalog.dataReplicaCatalog:
            ReplicaCatalog.dataReplicaCatalog[file] = []

        storage_list: List[str] = ReplicaCatalog.get_storage_list(file)
        if storage not in storage_list:
            storage_list.append(storage)