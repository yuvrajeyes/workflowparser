from __future__ import annotations

from typing import List, Dict
from abc import ABC
import cloudsim.provisioners as provisioners
from cloudsim.core import CloudSim
from cloudsim.CloudletScheduler import CloudletScheduler
from cloudsim.Pe import Pe
import cloudsim.DataCenter as Datacenter
import cloudsim.lists as lists
from cloudsim.Log import Log
from cloudsim.VmScheduler import VmScheduler
from cloudsim.VmStateHistoryEntry import VmStateHistoryEntry


class Vm:
    def __init__(self, id: int, userId: int, mips: float, numberOfPes: int, ram: int, bw: int, size: int, vmm: str, cloudlet_scheduler: CloudletScheduler):
        self.id: int = id
        self.userId: int = userId
        self.uid: str = self.get_uid(userId, id)
        self.size: int = size
        self.mips: float = mips
        self.numberOfPes: int = numberOfPes
        self.ram: int = ram
        self.bw: int = bw
        self.vmm: str = vmm
        self.cloudlet_scheduler: CloudletScheduler = cloudlet_scheduler
        self.host: Host = None
        self.inMigration: bool = False
        self.currentAllocatedSize: int = 0
        self.currentAllocatedRam: int = 0
        self.currentAllocatedBw: int = 0
        self.currentAllocatedMips: List[float] = None
        self.beingInstantiated: bool = True
        self.stateHistory: List[VmStateHistoryEntry] = []

    def update_vm_processing(self, current_time: float, mips_share: List[float]) -> float:
        if mips_share is not None:
            return self.cloudlet_scheduler.update_vm_processing(current_time, mips_share)
        return 0.0

    def get_current_requested_mips(self) -> List[float]:
        current_requested_mips: List[float] = self.cloudlet_scheduler.get_current_requested_mips()
        if self.beingInstantiated:
            current_requested_mips = [self.mips] * self.numberOfPes
        return current_requested_mips

    def get_current_requested_total_mips(self) -> float:
        return sum(self.get_current_requested_mips())

    def get_current_requested_max_mips(self) -> float:
        return max(self.get_current_requested_mips())

    def get_current_requested_bw(self) -> int:
        if self.beingInstantiated:
            return self.bw
        return int(self.cloudlet_scheduler.get_current_requested_utilization_of_bw() * self.bw)

    def get_current_requested_ram(self) -> int:
        if self.beingInstantiated:
            return self.ram
        return int(self.cloudlet_scheduler.get_current_requested_utilization_of_ram() * self.ram)

    def get_total_utilization_of_cpu(self, current_time: float) -> float:
        return self.cloudlet_scheduler.get_total_utilization_of_cpu(current_time)

    def get_total_utilization_of_cpu_mips(self, current_time: float) -> float:
        return self.get_total_utilization_of_cpu(current_time) * self.mips

    def set_uid(self, uid: str):
        self.uid = uid

    @staticmethod
    def get_uid(userId: int, vm_id: int) -> str:
        return f"{userId}-{vm_id}"
    
    def get_UID(self) -> str:
        return self.uid

    def get_id(self) -> int:
        return self.id

    def set_id(self, id: int):
        self.id = id

    def set_user_id(self, userId: int):
        self.userId = userId

    def get_user_id(self) -> int:
        return self.userId

    def get_mips(self) -> float:
        return self.mips

    def set_mips(self, mips: float):
        self.mips = mips

    def get_number_of_pes(self) -> int:
        return self.numberOfPes

    def set_number_of_pes(self, numberOfPes: int):
        self.numberOfPes = numberOfPes

    def get_ram(self) -> int:
        return self.ram

    def set_ram(self, ram: int):
        self.ram = ram

    def get_bw(self) -> int:
        return self.bw

    def set_bw(self, bw: int):
        self.bw = bw

    def get_size(self) -> int:
        return self.size

    def set_size(self, size: int):
        self.size = size

    def get_vmm(self) -> str:
        return self.vmm

    def set_vmm(self, vmm: str):
        self.vmm = vmm

    def set_host(self, host: 'Host'):
        self.host = host

    def get_host(self) -> 'Host':
        return self.host

    def get_cloudlet_scheduler(self) -> CloudletScheduler:
        return self.cloudlet_scheduler

    def set_cloudlet_scheduler(self, cloudlet_scheduler: CloudletScheduler):
        self.cloudlet_scheduler = cloudlet_scheduler

    def is_in_migration(self) -> bool:
        return self.inMigration

    def set_in_migration(self, inMigration: bool):
        self.inMigration = inMigration

    def get_current_allocated_size(self) -> int:
        return self.currentAllocatedSize

    def set_current_allocated_size(self, currentAllocatedSize: int):
        self.currentAllocatedSize = currentAllocatedSize

    def get_current_allocated_ram(self) -> int:
        return self.currentAllocatedRam

    def set_current_allocated_ram(self, currentAllocatedRam: int):
        self.currentAllocatedRam = currentAllocatedRam

    def get_current_allocated_bw(self) -> int:
        return self.currentAllocatedBw

    def set_current_allocated_bw(self, currentAllocatedBw: int):
        self.currentAllocatedBw = currentAllocatedBw

    def get_current_allocated_mips(self) -> List[float]:
        return self.currentAllocatedMips

    def set_current_allocated_mips(self, currentAllocatedMips: List[float]):
        self.currentAllocatedMips = currentAllocatedMips

    def is_being_instantiated(self) -> bool:
        return self.beingInstantiated

    def set_being_instantiated(self, beingInstantiated: bool):
        self.beingInstantiated = beingInstantiated

    def get_state_history(self) -> List[VmStateHistoryEntry]:
        return self.stateHistory

    def add_state_history_entry(self, time: float, allocated_mips: float, requested_mips: float, inMigration: bool):
        new_state: VmStateHistoryEntry = VmStateHistoryEntry(time, allocated_mips, requested_mips, inMigration)
        if self.stateHistory and self.stateHistory[-1].time == time:
            self.stateHistory[-1] = new_state
        else:
            self.stateHistory.append(new_state)



class Host:
    def __init__(self, id: int, ramProvisioner: provisioners.RamProvisioner, bwProvisioner: provisioners.BwProvisioner, storage: int, peList: List[Pe], vmScheduler: VmScheduler):
        self.id: int = id
        self.ramProvisioner: provisioners.RamProvisioner = ramProvisioner
        self.bwProvisioner: provisioners.BwProvisioner = bwProvisioner
        self.storage: int = storage
        self.peList: List[Pe] = peList
        self.vmScheduler: VmScheduler = vmScheduler
        self.vmList: List[Vm] = []
        self.failed: bool = False
        self.vmsMigratingIn: List[Vm] = []
        self.datacenter: Datacenter = None

    def update_vms_processing(self, current_time: float) -> float:
        smaller_time: float = float("inf")
        for vm in self.vmList:
            time: float = vm.update_vm_processing(current_time, self.vmScheduler.get_allocated_mips_for_vm(vm))
            if time > 0.0 and time < smaller_time:
                smaller_time = time
        return smaller_time

    def add_migrating_in_vm(self, vm: Vm) -> None:
        vm.set_in_migration(True)

        if vm not in self.vmsMigratingIn:
            if self.storage < vm.get_size():
                Log.print_line(f"[VmScheduler.addMigratingInVm] Allocation of VM #{vm.get_id()} to Host #{self.id} failed by storage")
                exit(0)
            if not self.ramProvisioner.allocate_ram_for_vm(vm, vm.get_current_requested_ram()):
                Log.print_line(f"[VmScheduler.addMigratingInVm] Allocation of VM #{vm.get_id()} to Host #{self.id} failed by RAM")
                exit(0)
            if not self.bwProvisioner.allocate_bw_for_vm(vm, vm.get_current_requested_bw()):
                Log.print_line(f"[VmScheduler.addMigratingInVm] Allocation of VM #{vm.get_id()} to Host #{self.id} failed by BW")
                exit(0)
            self.vmScheduler.get_vms_migrating_in().append(vm.get_id())
            if not self.vmScheduler.allocate_pes_for_vm(vm, vm.get_current_requested_mips()):
                Log.print_line(f"[VmScheduler.addMigratingInVm] Allocation of VM #{vm.get_id()} to Host #{self.id} failed by MIPS")
                exit(0)

            self.storage -= vm.get_size()
            self.vmsMigratingIn.append(vm)
            self.vmList.append(vm)
            self.update_vms_processing(CloudSim.clock())
            vm.get_host().update_vms_processing(CloudSim.clock())

    def remove_migrating_in_vm(self, vm: Vm) -> None:
        self.vm_deallocate(vm)
        self.vmsMigratingIn.remove(vm)
        self.vmList.remove(vm)
        self.vmScheduler.get_vms_migrating_in().remove(vm.get_id())
        vm.set_in_migration(False)

    def reallocate_migrating_in_vms(self) -> None:
        for vm in self.vmsMigratingIn:
            if vm not in self.vmList:
                self.vmList.append(vm)
            if vm.get_id() not in self.vmScheduler.get_vms_migrating_in():
                self.vmScheduler.get_vms_migrating_in().append(vm.get_id())
            self.ramProvisioner.allocate_ram_for_vm(vm, vm.get_current_requested_ram())
            self.bwProvisioner.allocate_bw_for_vm(vm, vm.get_current_requested_bw())
            self.vmScheduler.allocate_pes_for_vm(vm, vm.get_current_requested_mips())
            self.storage -= vm.get_size()

    def is_suitable_for_vm(self, vm: Vm) -> bool:
        return (
            self.vmScheduler.get_pe_capacity() >= vm.get_current_requested_max_mips()
            and self.vmScheduler.get_available_mips() >= vm.get_current_requested_total_mips()
            and self.ramProvisioner.is_suitable_for_vm(vm, vm.get_current_requested_ram())
            and self.bwProvisioner.is_suitable_for_vm(vm, vm.get_current_requested_bw())
        )

    def vm_create(self, vm: Vm) -> bool:
        if self.storage < vm.get_size():
            Log.print_line(f"[VmScheduler.vmCreate] Allocation of VM #{vm.get_id()} to Host #{self.id} failed by storage")
            return False

        if not self.ramProvisioner.allocate_ram_for_vm(vm, vm.get_current_requested_ram()):
            Log.print_line(f"[VmScheduler.vmCreate] Allocation of VM #{vm.get_id()} to Host #{self.id} failed by RAM")
            return False

        if not self.bwProvisioner.allocate_bw_for_vm(vm, vm.get_current_requested_bw()):
            Log.print_line(f"[VmScheduler.vmCreate] Allocation of VM #{vm.get_id()} to Host #{self.id} failed by BW")
            self.ramProvisioner.deallocate_ram_for_vm(vm)
            return False
        # Error present
        if not self.vmScheduler.allocate_pes_for_vm(vm, vm.get_current_requested_mips()):
            Log.print_line(f"[VmScheduler.vmCreate] Allocation of VM #{vm.get_id()} to Host #{self.id} failed by MIPS")
            self.ramProvisioner.deallocate_ram_for_vm(vm)
            self.bwProvisioner.deallocate_bw_for_vm(vm)
            return False

        self.storage -= vm.get_size()
        self.vmList.append(vm)
        vm.set_host(self)
        return True

    def vm_destroy(self, vm: Vm) -> None:
        if vm is not None:
            self.vm_deallocate(vm)
            self.vmList.remove(vm)
            vm.set_host(None)

    def vm_destroy_all(self) -> None:
        self.vm_deallocate_all()
        for vm in self.vmList:
            vm.set_host(None)
            self.storage += vm.get_size()
        self.vmList.clear()

    def vm_deallocate(self, vm: Vm) -> None:
        self.ramProvisioner.deallocate_ram_for_vm(vm)
        self.bwProvisioner.deallocate_bw_for_vm(vm)
        self.vmScheduler.deallocate_pes_for_vm(vm)
        self.storage += vm.get_size()

    def vm_deallocate_all(self) -> None:
        self.ramProvisioner.deallocate_ram_for_all_vms()
        self.bwProvisioner.deallocate_bw_for_all_vms()
        self.vmScheduler.deallocate_pes_for_all_vms()

    def get_vm(self, vm_id: int, userId: int) -> Vm:
        for vm in self.vmList:
            if vm.get_id() == vm_id and vm.get_user_id() == userId:
                return vm
        return None

    def get_number_of_pes(self) -> int:
        return len(self.peList)

    def get_number_of_free_pes(self) -> int:
        return lists.PeList.get_number_of_free_pes(self.peList)

    def get_total_mips(self) -> int:
        return lists.PeList.get_total_mips(self.peList)

    def allocate_pes_for_vm(self, vm: Vm, mips_share: List[float]) -> bool:
        return self.vmScheduler.allocate_pes_for_vm(vm, mips_share)

    def deallocate_pes_for_vm(self, vm: Vm) -> None:
        self.vmScheduler.deallocate_pes_for_vm(vm)

    def get_allocated_mips_for_vm(self, vm: Vm) -> List[float]:
        return self.vmScheduler.get_allocated_mips_for_vm(vm)

    def get_total_allocated_mips_for_vm(self, vm: Vm) -> float:
        return self.vmScheduler.get_total_allocated_mips_for_vm(vm)

    def get_max_available_mips(self) -> float:
        return self.vmScheduler.get_max_available_mips()

    def get_available_mips(self) -> float:
        return self.vmScheduler.get_available_mips()

    def get_bw(self) -> int:
        return self.bwProvisioner.get_bw()

    def get_ram(self) -> int:
        return self.ramProvisioner.get_ram()
    
    def get_storage(self) -> int:
        return self.storage
    
    def set_storage(self, storage: int) -> None:
        self.storage = storage

    def get_id(self) -> int:
        return self.id
    
    def set_id(self, id: int) -> None:
        self.id = id

    def get_ram_provisioner(self) -> provisioners.RamProvisioner:
        return self.ramProvisioner
    
    def set_ram_provisioner(self, ramProvisioner: provisioners.RamProvisioner) -> None:
        self.ramProvisioner = ramProvisioner
    
    def get_bw_provisioner(self) -> provisioners.BwProvisioner:
        return self.bwProvisioner
    
    def set_bw_provisioner(self, bwProvisioner: provisioners.BwProvisioner) -> None:
        self.bwProvisioner = bwProvisioner

    def get_vm_scheduler(self) -> VmScheduler:
        return self.vmScheduler
    
    def set_vm_scheduler(self, vmScheduler: VmScheduler) -> None:
        self.vmScheduler = vmScheduler

    def get_pe_list(self) -> List[Pe]:
        return self.peList
    
    def set_pe_list(self, peList: List[Pe]):
        self.peList = peList

    def get_vm_list(self) -> List[Vm]:
        return self.vmList
    
    def is_failed(self) -> bool:
        return self.failed

    def set_failed(self, resName: str, failed: bool) -> bool:
        self.failed = failed
        lists.PeList.set_status_failed(self.peList, resName, self.id, failed)
        return True
    
    def set_failed(self, failed: bool) -> bool:
        self.failed = failed
        lists.PeList.set_status_failed(self.peList, self.id, failed)
        return True

    def set_pe_status(self, pe_id: int, status: int) -> bool:
        return lists.PeList.set_pe_status(self.peList, pe_id, status)

    def get_vms_migrating_in(self) -> List[Vm]:
        return self.vmsMigratingIn

    def get_datacenter(self) -> Datacenter:
        return self.datacenter

    def set_datacenter(self, datacenter: Datacenter) -> None:
        self.datacenter = datacenter