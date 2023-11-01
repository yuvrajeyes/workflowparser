from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List
from cloudsim.Cloudlet import Cloudlet
from cloudsim.ResCloudlet import ResCloudlet


class CloudletScheduler(ABC):
    def __init__(self):
        self.previousTime: float = 0.0
        self.currentMipsShare: List[float] = []


    @abstractmethod
    def update_vm_processing(self, currentTime: float, mipsShare: List[float]) -> float:
        pass


    @abstractmethod
    def cloudlet_submit(self, gl: Cloudlet, fileTransferTime: float=None) -> float:
        pass


    @abstractmethod
    def cloudlet_cancel(self, clId: int) -> Cloudlet:
        pass


    @abstractmethod
    def cloudlet_pause(self, clId: int) -> bool:
        pass


    @abstractmethod
    def cloudlet_resume(self, clId: int) -> float:
        pass


    @abstractmethod
    def cloudlet_finish(self, rcl: ResCloudlet) -> None:
        pass


    @abstractmethod
    def get_cloudlet_status(self, clId: int) -> int:
        pass


    @abstractmethod
    def is_finished_cloudlets(self) -> bool:
        pass


    @abstractmethod
    def get_next_finished_cloudlet(self) -> Cloudlet:
        pass


    @abstractmethod
    def running_cloudlets(self) -> int:
        pass


    @abstractmethod
    def migrate_cloudlet(self) -> Cloudlet:
        pass


    @abstractmethod
    def get_total_utilization_of_cpu(self, time: float) -> float:
        pass


    @abstractmethod
    def get_current_requested_mips(self) -> List[float]:
        pass


    @abstractmethod
    def get_total_current_available_mips_for_cloudlet(self, rcl: ResCloudlet, mipsShare: List[float]) -> float:
        pass


    @abstractmethod
    def get_total_current_requested_mips_for_cloudlet(self, rcl: ResCloudlet, time: float) -> float:
        pass


    @abstractmethod
    def get_total_current_allocated_mips_for_cloudlet(self, rcl: ResCloudlet, time: float) -> float:
        pass


    @abstractmethod
    def get_current_requested_utilization_of_ram(self) -> float:
        pass


    @abstractmethod
    def get_current_requested_utilization_of_bw(self) -> float:
        pass


    def get_previous_time(self) -> float:
        return self.previousTime
    

    def set_previous_time(self, previousTime: float) -> None:
        self.previousTime = previousTime


    def set_current_mips_share(self, currentMipsShare: List[float]) -> None:
        self.currentMipsShare = currentMipsShare


    def get_current_mips_share(self) -> List[float]:
        return self.currentMipsShare