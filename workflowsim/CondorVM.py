from __future__ import annotations

from typing import List
import cloudsim.Vm as Vm
from cloudsim.CloudletScheduler import CloudletScheduler
from workflowsim.WorkflowSimTags import WorkflowSimTags


class CondorVM(Vm.Vm):
    cost: float = 0.0
    costPerMem: float = 0.0
    costPerStorage: float = 0.0
    costPerBw: float = 0.0
    
    def __init__(self, id: int, userId: int, mips: float, number_of_pes: int, ram: int, bw: int, size: int, vmm: str, cloudlet_scheduler: CloudletScheduler, 
                 cost: float=0.0, costPerMem: float=0.0, costPerStorage: float=0.0, costPerBw: float=0.0):
        super().__init__(id, userId, mips, number_of_pes, ram, bw, size, vmm, cloudlet_scheduler)
        self.cost: float = cost
        self.costPerMem: float = costPerMem
        self.costPerStorage: float = costPerStorage
        self.costPerBw: float = costPerBw
        self.state: int = WorkflowSimTags.VM_STATUS_IDLE  # Initialize VM state as IDLE at the beginning

    
    def get_cost(self) -> float:
        return self.cost
    

    def get_cost_per_bw(self) -> float:
        return self.costPerBw
    

    def get_cost_per_storage(self) -> float:
        return self.costPerStorage
    

    def get_cost_per_mem(self) -> float:
        return self.costPerMem
    

    def set_state(self, tag: int):
        self.state = tag


    def get_state(self) -> int:
        return self.state