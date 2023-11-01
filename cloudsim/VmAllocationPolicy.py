from __future__ import annotations

from typing import List, Dict, Union
from abc import ABC, abstractmethod
import cloudsim.Vm as Vm
from cloudsim.core import CloudSim
from cloudsim.Log import Log



class VmAllocationPolicy(ABC):
    def __init__(self, hostList: List[Vm.Host]):
        self.hostList: List[Vm.Host] = hostList


    @abstractmethod
    def allocate_host_for_vm(self, vm: Vm.Vm, host: Vm.Host) -> bool:
        pass


    @abstractmethod
    def optimize_allocation(self, vmList: List[Vm.Vm], utilization_bound: float, time: float) -> List[Dict[str, Union[str, float]]]:
        pass


    @abstractmethod
    def optimize_allocation(self, vmList: List[Vm.Vm]) -> List[Dict[str, Union[str, float]]]:
        pass


    @abstractmethod
    def deallocate_host_for_vm(self, vm: Vm.Vm) -> None:
        pass


    @abstractmethod
    def get_host(self, vm: Union[int, Vm.Vm], userId: int) -> Vm.Host:
        pass


    def set_host_list(self, hostList: List[Vm.Host]) -> None:
        self.hostList = hostList


    def get_host_list(self) -> List[Vm.Host]:
        return self.hostList



class VmAllocationPolicySimple(VmAllocationPolicy):
    def __init__(self, hostList: List[Vm.Host]):
        super().__init__(hostList)
        self.vmTable: Dict[str, Vm.Host] = {}
        self.usedPes: Dict[str, int] = {}
        self.freePes: List[int] = []
        for host in hostList:
            self.freePes.append(host.get_number_of_pes())


    def allocate_host_for_vm(self, vm: Vm.Vm, host: Vm.Host=None) -> bool:
        if host is not None:
            if host.vm_create(vm):  # if vm has been successfully created in the host
                self.vmTable[vm.get_UID()] = host
                requiredPes: int = vm.get_number_of_pes()
                idx: int = self.get_host_list().index(host)
                self.usedPes[vm.get_UID()] = requiredPes
                self.freePes[idx] -= requiredPes
                Log.format_line("%.2f: VM #%d has been allocated to the host #%d",
                    CloudSim.clock(), vm.get_id(), host.get_id())
                return True
            return False
        else:
            requiredPes: int = vm.get_number_of_pes()
            result: bool = False
            tries: int = 0
            freePesTmp: List[int] = self.freePes.copy()
            if vm.get_UID() not in self.vmTable:  # if this vm was not created
                while not result and tries < len(self.freePes):
                    more_free: int = min(freePesTmp)
                    idx: int = freePesTmp.index(more_free)
                    host: Vm.Host = self.get_host_list()[idx]
                    result = host.vm_create(vm)
                    if result:  # if vm was successfully created in the host
                        self.vmTable[vm.get_UID()] = host
                        self.usedPes[vm.get_UID()] = requiredPes
                        self.freePes[idx] -= requiredPes
                        break
                    else:
                        freePesTmp[idx] = float('-inf')
                    tries += 1
            return result


    def deallocate_host_for_vm(self, vm: Vm.Vm) -> None:
        host: Vm.Host = self.vmTable.pop(vm.get_UID(), None)
        idx: int = self.get_host_list().index(host) if host else -1
        pes: int = self.usedPes.pop(vm.get_UID())
        if host:
            host.vm_destroy(vm)
            if idx >= 0:
                self.freePes[idx] += pes


    def get_host(self, vm: Union[int, Vm.Vm], userId: int=None) -> Vm.Host:
        if (isinstance(vm, Vm.Vm)):
            return self.vmTable.get(vm.get_UID())
        elif (isinstance(vm, int)):
            if userId is None:
                raise ValueError("userId can't be empty")
            return self.vmTable.get(Vm.Vm.get_uid(userId, vm))


    def get_vm_table(self) -> Dict[str, Vm.Host]:
        return self.vmTable


    def set_vm_table(self, vmTable: Dict[str, Vm.Host]) -> None:
        self.vmTable = vmTable


    def get_used_pes(self) -> Dict[str, int]:
        return self.usedPes


    def set_used_pes(self, usedPes: Dict[str, int]) -> None:
        self.usedPes = usedPes


    def get_free_pes(self) -> List[int]:
        return self.freePes


    def set_free_pes(self, freePes: List[int]) -> None:
        self.freePes = freePes


    def optimize_allocation(self, vmList: List[Vm.Vm]) -> List[Dict[str, object]]:
        return []