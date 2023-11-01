from __future__ import annotations

from typing import List, Dict, cast, Union
from abc import ABC, abstractmethod
import cloudsim.Vm as Vm
from cloudsim.Pe import Pe
import cloudsim.lists as lists
import cloudsim.provisioners as provisioners
from cloudsim.Log import Log



class VmScheduler(ABC):
    def __init__(self, pelist: List[Pe]):
        self.peList: List[Pe] = pelist
        self.peMap: Dict[str, List[Pe]] = {}
        self.mipsMap: Dict[str, List[float]] = {}
        self.availableMips: float = lists.PeList.get_total_mips(pelist)
        self.vmsMigratingIn: List[str] = []
        self.vmsMigratingOut: List[str] = []


    @abstractmethod
    def allocate_pes_for_vm(self, vm: Union[Vm.Vm, str], mipsShare: List[float]) -> bool:
        pass


    @abstractmethod
    def deallocate_pes_for_vm(self, vm: Vm.Vm) -> None:
        pass


    def deallocate_pes_for_all_vms(self) -> None:
        self.mipsMap.clear()
        self.availableMips = lists.PeList.get_total_mips(self.peList)
        for pe in self.peList:
            pe.get_pe_provisioner().deallocate_mips_for_all_vms()


    def get_pes_allocated_for_vm(self, vm: Vm.Vm) -> List[Pe]:
        return self.peMap.get(vm.get_UID())


    def get_allocated_mips_for_vm(self, vm: Vm.Vm) -> List[float]:
        return self.mipsMap.get(vm.get_UID())


    def get_total_allocated_mips_for_vm(self, vm: Vm.Vm) -> float:
        allocated: float = 0
        mipsMap: List[float] = self.get_allocated_mips_for_vm(vm)
        if mipsMap:
            allocated = sum(mipsMap)
        return allocated


    def get_max_available_mips(self) -> float:
        if not self.peList:
            Log.print_line("Pe list is empty")
            return 0.0
        maxMips: float = 0.0
        for pe in self.peList:
            tmp: float = pe.get_pe_provisioner().get_available_mips()
            if tmp > maxMips:
                maxMips = tmp
        return maxMips
    

    def get_pe_capacity(self) -> float:
        if not self.peList:
            Log.print_line("Pe list is empty")
            return 0.0
        return self.peList[0].get_mips()
    

    def get_pe_list(self) -> List[Pe]:
        return cast(List[Pe], self.peList)
    

    def set_pe_list(self, peList: List[Pe]) -> None:
        self.peList = peList


    def get_mips_map(self) -> Dict[str, List[float]]:
        return self.mipsMap
    

    def set_mips_map(self, mipsMap: Dict[str, List[float]]) -> None:
        self.mipsMap = mipsMap


    def get_available_mips(self) -> float:
        return self.availableMips
    

    def set_available_mips(self, availableMips: float) -> None:
        self.availableMips = availableMips


    def get_vms_migrating_out(self) -> List[str]:
        return self.vmsMigratingOut
    

    def set_vms_migrating_out(self, vmsMigratingOut: List[str]) -> None:
        self.vmsMigratingOut = vmsMigratingOut


    def get_vms_migrating_in(self) -> List[str]:
        return self.vmsMigratingIn


    def set_vms_migrating_in(self, vmsMigratingIn: List[str]) -> None:
        self.vmsMigratingIn = vmsMigratingIn


    def get_pe_map(self) -> Dict[str, List[Pe]]:
        return self.peMap
    

    def set_pe_map(self, peMap: Dict[str, List[Pe]]) -> None:
        self.peMap = peMap



class VmSchedulerTimeShared(VmScheduler):
    def __init__(self, pelist: List[Pe]):
        super().__init__(pelist)
        self.mipsMapRequested: Dict[str, List[float]] = {}
        self.pesInUse: int = 0


    def allocate_pes_for_vm(self, vm: Vm.Vm, mipsShareRequested: List[float]) -> bool:
        if (isinstance(vm, Vm.Vm)):
            if vm.is_in_migration():
                if vm.get_UID() not in self.get_vms_migrating_in() and vm.get_UID() not in self.get_vms_migrating_out():
                    self.get_vms_migrating_out().append(vm.get_UID())
            else:
                if vm.get_UID() in self.get_vms_migrating_out():
                    self.get_vms_migrating_out().remove(vm.get_UID())
            result = self.allocate_pes_for_vm(vm.get_UID(), mipsShareRequested)
            self.update_pe_provisioning()
            return result
        else:
            vmUid: str = vm
            totalRequestedMips: float = 0
            peMips: float = self.get_pe_capacity()
            for mips in mipsShareRequested:
                if mips > peMips:
                    return False
                totalRequestedMips += mips
            if self.get_available_mips() < totalRequestedMips:
                return False
            self.get_mips_map_requested()[vmUid] = mipsShareRequested
            self.pesInUse += len(mipsShareRequested)
            if vmUid in self.get_vms_migrating_in():
                totalRequestedMips *= 0.1
            mipsShareAllocated: List[float] = []
            for mipsRequested in mipsShareRequested:
                if vmUid in self.get_vms_migrating_out():
                    mipsRequested *= 0.9
                elif vmUid in self.get_vms_migrating_in():
                    mipsRequested *= 0.1
                mipsShareAllocated.append(mipsRequested)
            self.get_mips_map()[vmUid] = mipsShareAllocated
            self.set_available_mips(self.get_available_mips() - totalRequestedMips)
            return True


    def update_pe_provisioning(self) -> None:
        self.get_pe_map().clear()
        for pe in self.get_pe_list():
            pe.get_pe_provisioner().deallocate_mips_for_all_vms()

        peIterator = iter(self.get_pe_list())
        pe: Pe = next(peIterator)
        peProvisioner: provisioners.PeProvisioner = pe.get_pe_provisioner()
        availableMips: float = peProvisioner.get_available_mips()

        for vmUid, mipsShare in self.get_mips_map().items():
            self.get_pe_map()[vmUid] = []

            for mips in mipsShare:
                while mips >= 0.1:
                    if availableMips >= mips:
                        peProvisioner.allocate_mips_for_vm(vmUid, mips)
                        self.get_pe_map()[vmUid].append(pe)
                        availableMips -= mips
                        break
                    else:
                        peProvisioner.allocate_mips_for_vm(vmUid, availableMips)
                        self.get_pe_map()[vmUid].append(pe)
                        mips -= availableMips
                        if mips <= 0.1:
                            break
                        if not peIterator:
                            Log.print_line("There is no enough MIPS ({}) to accommodate VM {}".format(mips, vmUid))
                            # sys.exit(0)
                        pe: Pe = next(peIterator)
                        peProvisioner = pe.get_pe_provisioner()
                        availableMips = peProvisioner.get_available_mips()


    def deallocate_pes_for_vm(self, vm: Vm.Vm) -> None:
        self.get_mips_map_requested().pop(vm.get_UID(), None)
        self.pesInUse = 0
        self.get_mips_map().clear()
        self.set_available_mips(lists.PeList.get_total_mips(self.get_pe_list()))
        for pe in self.get_pe_list():
            pe.get_pe_provisioner().deallocate_mips_for_vm(vm)
        for vmUid, mipsShareRequested in self.get_mips_map_requested().items():
            self.allocate_pes_for_vm(vmUid, mipsShareRequested)
        self.update_pe_provisioning()


    def deallocate_pes_for_all_vms(self) -> None:
        super().deallocate_pes_for_all_vms()
        self.get_mips_map_requested().clear()
        self.pesInUse = 0


    def get_max_Available_mips(self) -> float:
        return self.get_available_mips()
    

    def set_pes_in_use(self, pesInUse: int) -> None:
        self.pesInUse = pesInUse


    def get_pes_in_use(self) -> int:
        return self.pesInUse
    

    def set_mips_map_requested(self, mipsMapRequested: Dict[str, List[float]]) -> None:
        self.mipsMapRequested = mipsMapRequested


    def get_mips_map_requested(self) -> Dict[str, List[float]]:
        return self.mipsMapRequested