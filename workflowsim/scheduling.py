from __future__ import annotations
import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")

from typing import List, Dict
from abc import ABC, abstractmethod
from cloudsim.Cloudlet import Cloudlet 
import cloudsim.Vm as Vm
from cloudsim.Log import Log
from workflowsim.CustomVM import CustomVM
from workflowsim.WorkflowSimTags import WorkflowSimTags


class BaseSchedulingAlgorithm(ABC):
    def __init__(self):
        self.cloudletList: List[Cloudlet] = []
        self.vmList: List[Vm.Vm] = []
        self.scheduledList: List[Cloudlet] = []


    def set_cloudlet_list(self, cloudletList: List[Cloudlet]) -> None:
        self.cloudletList = cloudletList


    def set_vm_list(self, vmList: List[Vm.Vm]) -> None:
        self.vmList = vmList


    def get_cloudlet_list(self) -> List:
        return self.cloudletList


    def get_vm_list(self) -> List:
        return self.vmList
    

    @abstractmethod
    def run(self):
        pass


    def get_scheduled_list(self) -> List:
        return self.scheduledList


class StaticSchedulingAlgorithm(BaseSchedulingAlgorithm):
    def __init__(self):
        super().__init__()


    def run(self) -> None:
        mId2Vm: Dict[int, CustomVM] = {}
        for i in range(len(self.get_vm_list())):
            vm: CustomVM = self.get_vm_list()[i]
            if vm is not None:
                mId2Vm[vm.get_id()] = vm
        size: int = len(self.get_cloudlet_list())
        for i in range(size):
            cloudlet: Cloudlet = self.get_cloudlet_list()[i]
            if cloudlet.get_vm_id() < 0 or cloudlet.get_vm_id() not in mId2Vm:
                Log.print_line(f"Cloudlet {cloudlet.get_cloudlet_id()} is not matched. It is possible a stage-in job")
                cloudlet.set_vm_id(0)
            vm: CustomVM = mId2Vm[cloudlet.get_vm_id()]
            if vm.get_state() == WorkflowSimTags.VM_STATUS_IDLE:
                vm.set_state(WorkflowSimTags.VM_STATUS_BUSY)
                self.get_scheduled_list().append(cloudlet)
                Log.print_line(f"Schedules {cloudlet.get_cloudlet_id()} with {cloudlet.get_cloudlet_length()} to VM {cloudlet.get_vm_id()}")