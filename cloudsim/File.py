
from __future__ import annotations

from typing import Final
from cloudsim.Consts import Consts
import cloudsim.DataCenter as DataCenter
from cloudsim.core import CloudSim

class File:
    NOT_REGISTERED: Final[int] = -1
    TYPE_UNKOWN: Final[int] = 0
    TYPE_RAW_DATA: Final[int] = 1
    TYPE_RECONSTRUCTED_DATA: Final[int] = 2
    TYPE_TAG_DATA: Final[int] = 3

    def __init__(self, fileName: str, fileSize: int) -> None:
        if fileName is None or len(fileName) == 0:
            raise ValueError("File(): Error - invalid file name.")
        if fileSize <= 0:
            raise ValueError("File(): Error - size <= 0.")
        self.name = fileName  # logical file name
        self.attribute = FileAttribute(fileName, fileSize)  # a file attribute
        self.transactionTime = 0.0  # a transaction time for adding / getting / deleting this file

    def make_replica(self) -> 'File':
        return self.make_copy()

    def make_master_copy(self) -> 'File':
        file = self.make_copy()
        if file is not None:
            file.set_master_copy(True)
        return file

    def make_copy(self) -> 'File':
        file = None
        try:
            file = File(self.name, self.attribute.get_file_size())
            file_attr = file.get_file_attribute()
            self.attribute.copy_value(file_attr)
            file_attr.set_master_copy(False)  # set this file to replica
        except Exception as e:
            file = None
        return file

    def get_file_attribute(self) -> 'FileAttribute':
        return self.attribute

    def get_attribute_size(self) -> int:
        return self.attribute.get_attribute_size()

    def set_resource_id(self, resource_id: int) -> bool:
        return self.attribute.set_resource_id(resource_id)

    def get_resource_id(self) -> int:
        return self.attribute.get_resource_id()

    def get_name(self) -> str:
        return self.attribute.get_name()

    def set_name(self, name: str) -> None:
        self.attribute.set_name(name)

    def set_owner_name(self, name: str) -> bool:
        return self.attribute.set_owner_name(name)

    def get_owner_name(self) -> str:
        return self.attribute.get_owner_name()

    def get_size(self) -> int:
        return self.attribute.get_file_size()

    def get_size_in_byte(self) -> int:
        return self.attribute.get_file_size_in_byte()

    def set_file_size(self, file_size: int) -> bool:
        return self.attribute.set_file_size(file_size)

    def set_update_time(self, time: float) -> bool:
        return self.attribute.set_update_time(time)

    def get_last_update_time(self) -> float:
        return self.attribute.get_last_update_time()

    def set_registration_id(self, id: int) -> bool:
        return self.attribute.set_registration_id(id)

    def get_registration_id(self) -> int:
        return self.attribute.get_registration_id()

    def set_type(self, type: int) -> bool:
        return self.attribute.set_type(type)

    def get_type(self) -> int:
        return self.attribute.get_type()

    def set_checksum(self, checksum: int) -> bool:
        return self.attribute.set_checksum(checksum)

    def get_checksum(self) -> int:
        return self.attribute.get_checksum()

    def set_cost(self, cost: float) -> bool:
        return self.attribute.set_cost(cost)

    def get_cost(self) -> float:
        return self.attribute.get_cost()

    def get_creation_time(self) -> int:
        return self.attribute.get_creation_time()

    def is_registered(self) -> bool:
        return self.attribute.is_registered()

    def set_master_copy(self, master_copy: bool) -> None:
        self.attribute.set_master_copy(master_copy)

    def is_master_copy(self) -> bool:
        return self.attribute.is_master_copy()

    def set_read_only(self, read_only: bool) -> None:
        self.attribute.set_read_only(read_only)

    def is_read_only(self) -> bool:
        return self.attribute.is_read_only()

    def set_transaction_time(self, time: float) -> bool:
        if time < 0:
            return False
        self.transactionTime = time
        return True

    def get_transaction_time(self) -> float:
        return self.transactionTime


from datetime import datetime

class FileAttribute:
    def __init__(self, fileName: str, fileSize: int) -> None:
        if fileName is None or len(fileName) == 0:
            raise ValueError("FileAttribute(): Error - invalid file name.")
        if fileSize <= 0:
            raise ValueError("FileAttribute(): Error - size <= 0.")
        
        self.name: str = fileName  # logical file name
        self.ownerName: str = None  # owner name of this file
        self.id: int = File.NOT_REGISTERED  # file ID given by a Replica Catalogue
        self.type: int = File.TYPE_UNKOWN  # file type, e.g., raw, reconstructed, etc
        self.size: int = fileSize  # file size in byte
        self.checksum: int = 0  # check sum
        self.lastUpdateTime: float = 0.0  # last updated time (sec) - relative
        self.creationTime: int = 0  # creation time (ms) - absolute/relative
        self.cost: float = 0.0  # price of this file
        self.masterCopy: bool = True  # false if it is a replica
        self.readOnly: bool = False  # false if it can be rewritten
        self.resourceId: int = -1  # resource ID storing this file

        # Set the file creation time. This is absolute time
        date = CloudSim().get_simulation_calendar().get_time()
        if date is not None:
            self.creationTime = int(date.timestamp() * 1000)  # Convert to milliseconds

    def copy_value(self, attr: 'FileAttribute') -> bool:
        if attr is None:
            return False

        attr.set_file_size(self.size)
        attr.set_resource_id(self.resourceId)
        attr.set_owner_name(self.ownerName)
        attr.set_update_time(self.lastUpdateTime)
        attr.set_registration_id(self.id)
        attr.set_type(self.type)
        attr.set_checksum(self.checksum)
        attr.set_cost(self.cost)
        attr.set_master_copy(self.masterCopy)
        attr.set_read_only(self.readOnly)
        attr.set_name(self.name)
        attr.set_creation_time(self.creationTime)
        return True

    def set_creation_time(self, creationTime: int) -> bool:
        if creationTime <= 0:
            return False
        self.creationTime = creationTime
        return True

    def get_creation_time(self) -> int:
        return self.creationTime

    def set_resource_id(self, resourceID: int) -> bool:
        if resourceID == -1:
            return False
        self.resourceId = resourceID
        return True

    def get_resource_id(self) -> int:
        return self.resourceId

    def set_owner_name(self, name: str) -> bool:
        if name is None or len(name) == 0:
            return False
        self.ownerName = name
        return True

    def get_owner_name(self) -> str:
        return self.ownerName

    def get_attribute_size(self) -> int:
        length = DataCenter.DataCloudTags.PKT_SIZE
        if self.ownerName is not None:
            length += len(self.ownerName)
        if self.name is not None:
            length += len(self.name)
        return length

    def set_file_size(self, fileSize: int) -> bool:
        if fileSize < 0:
            return False
        self.size = fileSize
        return True

    def get_file_size(self) -> int:
        return self.size

    def get_file_size_in_byte(self) -> int:
        return self.size * Consts.MILLION

    def set_update_time(self, time: float) -> bool:
        if time <= 0 or time < self.lastUpdateTime:
            return False
        self.lastUpdateTime = time
        return True

    def get_last_update_time(self) -> float:
        return self.lastUpdateTime

    def set_registration_id(self, id: int) -> bool:
        if id < 0:
            return False
        self.id = id
        return True

    def get_registration_id(self) -> int:
        return self.id

    def set_type(self, fileType: int) -> bool:
        if fileType < 0:
            return False
        self.type = fileType
        return True

    def get_type(self) -> int:
        return self.type

    def set_checksum(self, checksum: int) -> bool:
        if checksum < 0:
            return False
        self.checksum = checksum
        return True

    def get_checksum(self) -> int:
        return self.checksum

    def set_cost(self, cost: float) -> bool:
        if cost < 0:
            return False
        self.cost = cost
        return True

    def get_cost(self) -> float:
        return self.cost

    def is_registered(self) -> bool:
        return self.id != File.NOT_REGISTERED

    def set_master_copy(self, masterCopy: bool) -> None:
        self.masterCopy = masterCopy

    def is_master_copy(self) -> bool:
        return self.masterCopy

    def set_read_only(self, readOnly: bool) -> None:
        self.readOnly = readOnly

    def is_read_only(self) -> bool:
        return self.readOnly

    def set_name(self, name: str) -> None:
        self.name = name

    def get_name(self) -> str:
        return self.name