from __future__ import annotations


class VmStateHistoryEntry:
    time: float = 0.0
    allocatedMips: float = 0.0
    requestedMips: float = 0.0
    inMigration: bool = False

    def __init__(self, time: float, allocateMips: float, requestedMips: float, isInMigration: bool):
        self.time = time
        self.allocateMips = allocateMips
        self.requestedMips = requestedMips
        self.inMigration = isInMigration


    def set_time(self, time: float):
        self.time = time


    def get_time(self) -> float:
        return self.time
    

    def set_allocated_mips(self, allocatedMips: float):
        self.allocatedMips = allocatedMips


    def get_allocated_mips(self) -> float:
        return self.allocatedMips
    

    def set_requested_mips(self, requestedMips: float):
        self.requestedMips = requestedMips


    def get_requested_mips(self) -> float:
        return self.requestedMips
    

    def set_in_migration(self, inMigration: bool):
        self.inMigration = inMigration


    def is_in_migration(self) -> bool:
        return self.inMigration