from __future__ import annotations

from typing import List, cast, Union
from cloudsim.Storage import Storage
from cloudsim.File import File
from cloudsim.Log import Log

class HarddriveStorage(Storage):
    def __init__(self, name: str="HarddriveStorage", capacity: float=0) -> None:
        assert name is not None and len(name) > 0, "HarddriveStorage(): Error - invalid storage name."
        assert capacity > 0, "HarddriveStorage(): Error - capacity <= 0."
        self.name = name
        self.capacity = capacity
        self.fileList: List[File] = list()
        self.nameList: List[str] = list()
        self.currentSize: float = 0.0
        self.maxTransferRate: float = 133  # in MB/sec
        self.latency: float = 0.00417      # 4.17 ms
        self.avgSeekTime: float = 0.009    # 9 ms


    def init(self) -> None:
        self.fileList: List[File] = list()
        self.nameList: List[str] = list()
        self.currentSize: float = 0.0
        self.maxTransferRate: float = 133  # in MB/sec
        self.latency: float = 0.00417      # 4.17 ms
        self.avgSeekTime: float = 0.009    # 9 ms


    def get_available_space(self) -> float:
        return self.capacity - self.currentSize
    

    def is_full(self) -> bool:
        return abs(self.currentSize - self.currentSize) < 0.0000001
    

    def get_num_stored_file(self) -> int:
        return len(self.fileList)
    
    
    def reserve_space(self, file_size: int) -> bool:
        if (file_size <= 0 or self.currentSize+file_size >= self.capacity):
            return False
        self.currentSize += file_size
        return True
    

    def add_reserved_file(self, file: File) -> float:
        if (file is None):
            return 0
        self.currentSize -= file.get_size()
        result: float = self.add_file(file)
        # if add file fails, then set the current size back to its old value
        if (result == 0.0):
            self.currentSize += file.get_size()
        return result
    

    def has_potential_available_space(self, file_size: int) -> bool:
        if (file_size <= 0):
            return False
        # check if enough space left
        if (self.get_available_space() > file_size):
            return True
        
        deletedFileSize: int = 0
        # if not enough space, then if want to clear/delete some files
		# then check whether it still have space or not
        result: bool = False
        for file in self.fileList:
            if (not file.is_read_only()):
                deletedFileSize += file.get_size()
            if (deletedFileSize > file_size):
                result = True
                break
        return result
    
    
    def get_capacity(self) -> float:
        return self.currentSize
    
    
    def get_name(self) -> str:
        return self.name
    
    
    def set_latency(self, latency: float) -> None:
        self.latency=latency


    def get_latency(self) -> float:
        return self.latency
    
    
    def set_max_transfer_rate(self, rate: int) -> bool:
        if (rate <= 0):
            return False
        self.maxTransferRate = rate
        return True
    

    def get_max_transfer_rate(self) -> float:
        return self.maxTransferRate
    

    def set_avg_seek_time(self, seekTime: float) -> None:
        if (seekTime <= 0.0):
            return False
        self.avgSeekTime=seekTime
        return True
    
    
    def get_avg_seek_time(self) -> float:
        return self.avgSeekTime
    

    def get_file(self, file_name: str) -> File:
        obj: File = None
        if (file_name is None or len(file_name) == 0):
            Log.print_line(f"{self.name}.get_file(): Warning - invalid file name")
            return cast(File, None)
        
        size: int = 0
        index: int = 0
        found: bool = False
        for tempFile in self.fileList:
            size += tempFile.get_size()
            if (tempFile.get_name()==file_name):
                found=True
                obj=tempFile
                break
            index+=1

        # if the file is found, then determine the time taken to get it
        if (found):
            obj = self.fileList[index]
            seekTime: float = self.get_seek_time(size)
            transferTime: float = self.get_transfer_time(obj.get_size())
            # total time for this operation
            obj.set_transaction_time(seekTime+transferTime)

        return obj
    

    def get_file_name_list(self) -> List[str]:
        return self.nameList
    
    
    def get_seek_time(self, file_size: int) -> float:
        result: float = 0
        if (file_size > 0 and self.capacity != 0):
            result += (file_size / self.capacity)
        return result
    
    
    def get_transfer_time(self, file_size: int) -> float:
        result: float = 0
        if (file_size > 0 and self.capacity != 0):
            result += ((file_size*self.maxTransferRate)/self.capacity)
        return result
    

    def is_file_valid(self, file: File, method_name: str) -> bool:
        if file is None:
            Log.print_line(f"{self.name}.{method_name}: Warning - the given file is null.")
            return False

        file_name: str = file.get_name
        if not file_name or len(file_name) == 0:
            Log.print_line(f"{self.name}.{method_name}: Warning - invalid file name.")
            return False
        return True
    
    
    def add_file(self, file: File) -> float:
        result: float = 0.0
        # Check if the file is valid or not
        if not self.is_file_valid(file, "add_file()"):
            return result

        # Check the capacity
        if file.get_size() + self.currentSize > self.capacity:
            Log.print_line(f"{self.name}.add_file(): Warning - not enough space to store {file.name}")
            return result

        # Check if the same file name is already taken
        if not self.contains(file.get_name()):
            seek_time = self.get_seek_time(file.get_size())
            transfer_time = self.get_transfer_time(file.get_size())

            self.fileList.append(file)  # Add the file to the HD
            self.nameList.append(file.get_name())  # Add the name to the name list
            self.currentSize += file.get_size()  # Increment the current HD size
            result = seek_time + transfer_time  # Add total time

        file.set_transaction_time(result)
        return result
    
    
    def add_files(self, file_list: List[File]) -> float:
        result: float = 0.0
        if file_list is None or len(file_list) == 0:
            Log.print_line(f"{self.name}.add_files(): Warning - list is empty.")
            return result

        for file in file_list:
            result += self.add_file(file)  # Add each file in the list
        return result
    

    def deleteFile(self, fileN: Union[str, File], file: File=None) -> Union[bool, File]:
        result: float = 0.0
        
        if (isinstance(fileN, File) or  file is not None):
            if not self.is_file_valid(fileN, "deleteFile()"):
                return result
            seekTime: float = self.get_seek_time(fileN.get_size())
            transferTime: float = self.get_transfer_time(fileN.get_size())
            if self.contains(fileN):
                self.fileList.remove(fileN)
                self.nameList.remove(fileN.get_name())
                self.currentSize -= fileN.get_size()
                result = seekTime + transferTime
                fileN.set_transaction_time(result)

        elif isinstance(fileN, str):
            if fileN is None or len(fileN) == 0:
                return cast(File, None)

            for file in self.fileList:
                # If a file is found, then delete it
                if fileN == file.get_name():
                    result = self.delete_file(file)
                    file.set_transaction_time(result)
                    return file
            return cast(File, None)
        
    
    def contains(self, file: Union[str, File]):
        result = False
        if file is None or len(file) == 0:
            Log.print_line(f"{self.name}.contains(): Warning - invalid file reference")
            return result
        if isinstance(file, str):
            # Check each file name in the list
            for name in self.nameList:
                if name == file:
                    result = True
                    break
        elif isinstance(file, File):
            if not self.is_file_valid(file, "contains()"):
                return result
            # Check each file name in the list
            for name in self.nameList:
                if name == file.get_name():
                    result = True
                    break
        return result
    

    def rename_file(self, file: File, new_name: str) -> bool:
        result = False
        # Check whether the new filename conflicts with existing ones
        if self.contains(new_name):
            return result
        # Replace the file name in the file (physical) list
        obj: File = self.get_file(file.get_name())
        if obj is None:
            return result
        else:
            obj.set_name(new_name)
        # Replace the file name in the name list
        for i, name in enumerate(self.nameList):
            if name == file.get_name():
                file.set_transaction_time(0)
                self.nameList[i] = new_name
                result = True
                break
        return result