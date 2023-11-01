from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Dict, Union
import cloudsim.Vm as Vm


class BwProvisioner(ABC):
    def __init__(self, bw: int):
        self.bw: int = bw
        self.availableBw: int = bw

    @abstractmethod
    def allocate_bw_for_vm(self, vm: Vm.Vm, bw: int) -> bool:
        pass

    @abstractmethod
    def get_allocated_bw_for_vm(self, vm: Vm.Vm) -> int:
        pass

    @abstractmethod
    def deallocate_bw_for_vm(self, vm: Vm.Vm) -> None:
        pass

    def deallocate_bw_for_all_vms(self) -> None:
        self.availableBw = self.bw

    @abstractmethod
    def is_suitable_for_vm(self, vm: Vm.Vm, bw: int) -> bool:
        pass

    def get_bw(self) -> int:
        return self.bw

    def set_bw(self, bw: int) -> None:
        self.bw = bw

    def get_available_bw(self) -> int:
        return self.availableBw

    def get_used_bw(self) -> int:
        return self.bw - self.availableBw

    def set_available_bw(self, availableBw: int) -> None:
        self.availableBw = availableBw


class BwProvisionerSimple(BwProvisioner):
    def __init__(self, bw: int):
        super().__init__(bw)
        self.bwTable: Dict[str, int] = {}


    def allocate_bw_for_vm(self, vm: Vm.Vm, bw: int) -> bool:
        self.deallocate_bw_for_vm(vm)
    
        if self.get_available_bw() >= bw:
            self.set_available_bw(self.get_available_bw() - bw)
            self.bwTable[vm.get_UID()] = bw
            vm.set_current_allocated_bw(self.get_allocated_bw_for_vm(vm))
            return True

        vm.set_current_allocated_bw(self.get_allocated_bw_for_vm(vm))
        return False

    def get_allocated_bw_for_vm(self, vm: Vm.Vm) -> int:
        return self.bwTable.get(vm.get_UID(), 0)

    def deallocate_bw_for_vm(self, vm: Vm.Vm) -> None:
        if vm.get_UID() in self.bwTable:
            amountFreed = self.bwTable.pop(vm.get_UID())
            self.set_available_bw(self.get_available_bw() + amountFreed)
            vm.set_current_allocated_bw(0)

    def deallocate_bw_for_all_vms(self) -> None:
        super().deallocate_bw_for_all_vms()
        self.bwTable.clear()

    def is_suitable_for_vm(self, vm: Vm.Vm, bw: int) -> bool:
        allocatedBw = self.get_allocated_bw_for_vm(vm)
        result = self.allocate_bw_for_vm(vm, bw)
        self.deallocate_bw_for_vm(vm)
        if allocatedBw > 0:
            self.allocate_bw_for_vm(vm, allocatedBw)
        return result


class RamProvisioner(ABC):
    def __init__(self, ram: int):
        self.ram: int = ram
        self.availableRam: int = ram

    @abstractmethod
    def allocate_ram_for_vm(self, vm:Vm.Vm, ram: int) -> bool:
        pass

    @abstractmethod
    def get_allocated_ram_for_vm(self, vm: Vm.Vm) -> int:
        pass

    @abstractmethod
    def deallocate_ram_for_vm(self, vm: Vm.Vm) -> None:
        pass

    def deallocate_ram_for_all_vms(self) -> None:
        self.availableRam = self.ram

    @abstractmethod
    def is_suitable_for_vm(self, vm: Vm.Vm, ram: int) -> bool:
        pass

    def get_ram(self) -> int:
        return self.ram

    def set_ram(self, ram: int) -> None:
        self.ram = ram

    def get_used_ram(self) -> int:
        return self.ram - self.availableRam

    def get_available_ram(self) -> int:
        return self.availableRam

    def set_available_ram(self, availableRam: int) -> None:
        self.availableRam = availableRam


class RamProvisionerSimple(RamProvisioner):
    def __init__(self, availableRam: int):
        super().__init__(availableRam)
        self.ramTable: Dict[str, int] = {}


    def allocate_ram_for_vm(self, vm: Vm.Vm, ram: int) -> bool:
        self.ramTable = {}
        self.availableRam = 2048
        maxRam: int = vm.get_ram()
        if ram >= maxRam:
            ram = maxRam
        self.deallocate_ram_for_vm(vm)
        if self.get_available_ram() >= ram:
            self.availableRam -= ram
            self.ramTable[vm.get_UID()] = ram
            vm.set_current_allocated_ram(self.get_allocated_ram_for_vm(vm))
            return True
        vm.set_current_allocated_ram(self.get_allocated_ram_for_vm(vm))
        return False
    

    def get_allocated_ram_for_vm(self, vm: Vm.Vm) -> int:
        if vm.get_UID() in self.get_ram_table():
            return self.get_ram_table()[vm.get_UID()]
        return 0


    def deallocate_ram_for_vm(self, vm: Vm.Vm) -> None:
        if vm.get_UID() in self.get_ram_table().keys():
            amountFreed: int = self.ramTable.pop(vm.get_UID())
            self.set_available_ram(self.get_available_ram() + amountFreed)
            vm.set_current_allocated_ram(0)


    def deallocate_ram_for_all_vms(self) -> None:
        super().deallocate_ram_for_all_vms()
        self.ramTable.clear()


    def is_suitable_for_vm(self, vm: Vm.Vm, ram: int) -> bool:
        allocatedRam: int = self.get_allocated_ram_for_vm(vm)
        result: bool = self.allocate_ram_for_vm(vm, ram)
        self.deallocate_ram_for_vm(vm)
        if allocatedRam > 0:
            self.allocate_ram_for_vm(vm, allocatedRam)
        return result
    
    
    def get_ram_table(self) -> Dict[str, int]:
        return self.ramTable
    
    
    def set_ram_table(self, ramTable: Dict[str, int]) -> None:
        self.ramTable = ramTable



class PeProvisioner(ABC):
    def __init__(self, mips: float):
        self.mips: float = mips
        self.availableMips: float = mips


    @abstractmethod
    def allocate_mips_for_vm(self, vm: Vm.Vm, mips: float) -> bool:
        pass


    @abstractmethod
    def allocate_mips_for_vm(self, vm_uid: str, mips: float) -> bool:
        pass


    @abstractmethod
    def allocate_mips_for_vm(self, vm: Vm.Vm, mips_list: List[float]) -> bool:
        pass


    @abstractmethod
    def get_allocated_mips_for_vm(self, vm: Vm.Vm) -> List[float]:
        pass


    @abstractmethod
    def get_total_allocated_mips_for_vm(self, vm: Vm.Vm) -> float:
        pass


    @abstractmethod
    def get_allocated_mips_for_vm_by_virtual_pe_id(self, vm: Vm.Vm, pe_id: int) -> float:
        pass


    @abstractmethod
    def deallocate_mips_for_vm(self, vm: Vm.Vm) -> None:
        pass


    def deallocate_mips_for_all_vms(self) -> None:
        self.availableMips = self.mips


    def get_mips(self) -> float:
        return self.mips
    

    def set_mips(self, mips: float) -> None:
        self.mips = mips


    def get_available_mips(self) -> float:
        return self.availableMips
    
    
    def set_available_mips(self, availableMips: float) -> float:
        self.availableMips = availableMips


    def get_total_allocated_mips(self) -> float:
        total_allocated_mips = self.mips - self.availableMips
        return max(total_allocated_mips, 0)
    

    def get_utilization(self) -> float:
        return self.get_total_allocated_mips() / self.mips
    


class PeProvisionerSimple(PeProvisioner):
    def __init__(self, availableMips: float):
        super().__init__(availableMips)
        self.peTable: Dict[str, List[float]] = {}


    def allocate_mips_for_vm(self, vm_uid: Union[Vm.Vm, str], mips: float) -> bool:
        if (isinstance(vm_uid, Vm.Vm)):
            vm_uid: str = vm_uid.get_UID()
        if self.get_available_mips() < mips:
            return False
        allocated_mips = self.peTable.get(vm_uid, [])
        allocated_mips.append(mips)
        self.set_available_mips(self.get_available_mips() - mips)
        self.peTable[vm_uid] = allocated_mips
        return True
    

    def allocate_mips_for_vm_list(self, vm: Vm.Vm, mips_list: List[float]) -> bool:
        total_mips_to_allocate = sum(mips_list)
        if self.get_available_mips() + self.get_total_allocated_mips_for_vm(vm) < total_mips_to_allocate:
            return False
        self.set_available_mips(self.get_available_mips() + self.get_total_allocated_mips_for_vm(vm) - total_mips_to_allocate)
        self.peTable[vm.get_UID()] = mips_list
        return True
    

    def deallocate_mips_for_all_vms(self) -> None:
        super().deallocate_mips_for_all_vms()
        self.peTable.clear()


    def get_allocated_mips_for_vm_by_virtual_pe_id(self, vm: Vm.Vm, pe_id: int) -> float:
        vm_uid = vm.get_UID()
        if vm_uid in self.peTable and pe_id < len(self.peTable[vm_uid]):
            return self.peTable[vm_uid][pe_id]
        return 0
    

    def get_allocated_mips_for_vm(self, vm: Vm.Vm) -> List[float]:
        return self.peTable.get(vm.get_UID(), [])
    

    def get_total_allocated_mips_for_vm(self, vm: Vm.Vm) -> float:
        vm_uid = vm.get_UID()
        if vm_uid in self.peTable:
            return sum(self.peTable[vm_uid])
        return 0
    

    def deallocate_mips_for_vm(self, vm: Vm.Vm) -> None:
        vm_uid = vm.get_UID()
        if vm_uid in self.peTable:
            allocated_mips = self.peTable.pop(vm_uid)
            self.set_available_mips(self.get_available_mips() + sum(allocated_mips))