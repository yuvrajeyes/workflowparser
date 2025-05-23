from __future__ import annotations
import sys

import math
from typing import List, Final, Tuple, Union, Dict, Any, Iterator, cast
from cloudsim.Cloudlet import Cloudlet
from cloudsim.VmAllocationPolicy import VmAllocationPolicy
from cloudsim.core import SimEvent, SimEntity, CloudSimTags, CloudSim
import cloudsim.Vm as Vm  # Host, Vm, PeList
import cloudsim.lists as lists
from cloudsim.Log import Log
from cloudsim.lists import HostList
from cloudsim.CloudletScheduler import CloudletScheduler
from cloudsim.Storage import Storage
from cloudsim.InfoPacket import InfoPacket
from cloudsim.File import File



class DataCloudTags:
    # to prevent a conflict with the existing CloudSimTags values
    BASE: Final[int] = 400         # for other general tags
    RM_BASE: Final[int] = 500      # for Replica Manager tags
    CTLG_BASE: Final[int] = 600    # for catalogue tags

    # Default Maximum Transmission Unit (MTU) of a link in bytes
    DEFAUT_MTU: Final[int] = 1500

    # The default packet size (in byte) for sending events to other entity
    PKT_SIZE: Final[int] = DEFAUT_MTU * 100  # in bytes

    # The default storage size (10 GByte)
    DEFAULT_STORAGE_SIZE: Final[int] = 10000000  # 10 GB in bytes

    # Constants for General Operations
    REGISTER_REPLICA_CTLG: Final[int] = BASE + 1
    INQUIRY_LOCAL_RC_LIST: Final[int] = BASE + 2
    INQUIRY_GLOBAL_RC_LIST: Final[int] = BASE + 3
    INQUIRY_RC_LIST: Final[int] = BASE + 4
    INQUIRY_RC_RESULT: Final[int] = BASE + 5
    DATA_CLOUDLET_SUBMIT: Final[int] = BASE + 6


    # Constants for File Requests and Operations
    FILE_REQUEST: Final[int] = RM_BASE + 1
    FILE_DELIVERY: Final[int] = RM_BASE + 2

    FILE_ADD_MASTER: Final[int] = RM_BASE + 10
    FILE_ADD_MASTER_RESULT: Final[int] = RM_BASE + 11
    FILE_ADD_REPLICA: Final[int] = RM_BASE + 12
    FILE_ADD_REPLICA_RESULT: Final[int] = RM_BASE + 13

    FILE_ADD_SUCCESSFUL: Final[int] = RM_BASE + 20
    FILE_ADD_ERROR_STORAGE_FULL: Final[int] = RM_BASE + 21
    FILE_ADD_ERROR_EMPTY: Final[int] = RM_BASE + 22
    FILE_ADD_ERROR_EXIST_READ_ONLY: Final[int] = RM_BASE + 23
    FILE_ADD_ERROR: Final[int] = RM_BASE + 24
    FILE_ADD_ERROR_ACCESS_DENIED: Final[int] = RM_BASE + 25

    FILE_DELETE_MASTER: Final[int] = RM_BASE + 30
    FILE_DELETE_MASTER_RESULT: Final[int] = RM_BASE + 31
    FILE_DELETE_REPLICA: Final[int] = RM_BASE + 32
    FILE_DELETE_REPLICA_RESULT: Final[int] = RM_BASE + 33

    FILE_DELETE_SUCCESSFUL: Final[int] = RM_BASE + 40
    FILE_DELETE_ERROR: Final[int] = RM_BASE + 41
    FILE_DELETE_ERROR_READ_ONLY: Final[int] = RM_BASE + 42
    FILE_DELETE_ERROR_DOESNT_EXIST: Final[int] = RM_BASE + 43
    FILE_DELETE_ERROR_IN_USE: Final[int] = RM_BASE + 44
    FILE_DELETE_ERROR_ACCESS_DENIED: Final[int] = RM_BASE + 45

    FILE_MODIFY: Final[int] = RM_BASE + 50
    FILE_MODIFY_RESULT: Final[int] = RM_BASE + 51

    FILE_MODIFY_SUCCESSFUL: Final[int] = RM_BASE + 60
    FILE_MODIFY_ERROR: Final[int] = RM_BASE + 61
    FILE_MODIFY_ERROR_READ_ONLY: Final[int] = RM_BASE + 62
    FILE_MODIFY_ERROR_DOESNT_EXIST: Final[int] = RM_BASE + 63
    FILE_MODIFY_ERROR_IN_USE: Final[int] = RM_BASE + 64
    FILE_MODIFY_ERROR_ACCESS_DENIED: Final[int] = RM_BASE + 65


    # Constants for Replica Catalogue Operations
    CTLG_GET_REPLICA: Final[int] = CTLG_BASE + 1
    CTLG_REPLICA_DELIVERY: Final[int] = CTLG_BASE + 2
    CTLG_GET_REPLICA_LIST: Final[int] = CTLG_BASE + 3
    CTLG_REPLICA_LIST_DELIVERY: Final[int] = CTLG_BASE + 4
    CTLG_GET_FILE_ATTR: Final[int] = CTLG_BASE + 5
    CTLG_FILE_ATTR_DELIVERY: Final[int] = CTLG_BASE + 6
    CTLG_FILTER: Final[int] = CTLG_BASE + 7
    CTLG_FILTER_DELIVERY: Final[int] = CTLG_BASE + 8

    CTLG_ADD_MASTER: Final[int] = CTLG_BASE + 10
    CTLG_ADD_MASTER_RESULT: Final[int] = CTLG_BASE + 11
    CTLG_ADD_MASTER_SUCCESSFUL: Final[int] = CTLG_BASE + 12
    CTLG_ADD_MASTER_ERROR: Final[int] = CTLG_BASE + 13
    CTLG_ADD_MASTER_ERROR_FULL: Final[int] = CTLG_BASE + 14

    CTLG_DELETE_MASTER: Final[int] = CTLG_BASE + 20
    CTLG_DELETE_MASTER_RESULT: Final[int] = CTLG_BASE + 21
    CTLG_DELETE_MASTER_SUCCESSFUL: Final[int] = CTLG_BASE + 22
    CTLG_DELETE_MASTER_ERROR: Final[int] = CTLG_BASE + 23
    CTLG_DELETE_MASTER_DOESNT_EXIST: Final[int] = CTLG_BASE + 24
    CTLG_DELETE_MASTER_REPLICAS_EXIST: Final[int] = CTLG_BASE + 25

    CTLG_ADD_REPLICA: Final[int] = CTLG_BASE + 30
    CTLG_ADD_REPLICA_RESULT: Final[int] = CTLG_BASE + 31
    CTLG_ADD_REPLICA_SUCCESSFUL: Final[int] = CTLG_BASE + 32
    CTLG_ADD_REPLICA_ERROR: Final[int] = CTLG_BASE + 33
    CTLG_ADD_REPLICA_DOESNT_EXIST: Final[int] = CTLG_BASE + 34
    CTLG_ADD_REPLICA_ERROR_FULL: Final[int] = CTLG_BASE + 35

    CTLG_DELETE_REPLICA: Final[int] = CTLG_BASE + 40
    CTLG_DELETE_REPLICA_RESULT: Final[int] = CTLG_BASE + 41
    CTLG_DELETE_REPLICA_SUCCESSFUL: Final[int] = CTLG_BASE + 42
    CTLG_DELETE_REPLICA_ERROR: Final[int] = CTLG_BASE + 43
    CTLG_DELETE_REPLICA_DOESNT_EXIST: Final[int] = CTLG_BASE + 44

    CTLG_MODIFY_MASTER: Final[int] = CTLG_BASE + 50
    CTLG_MODIFY_MASTER_RESULT: Final[int] = CTLG_BASE + 51
    CTLG_MODIFY_MASTER_SUCCESSFUL: Final[int] = CTLG_BASE + 52
    CTLG_MODIFY_MASTER_ERROR: Final[int] = CTLG_BASE + 53
    CTLG_MODIFY_MASTER_ERROR_DOESNT_EXIST: Final[int] = CTLG_BASE + 54
    CTLG_MODIFY_MASTER_ERROR_READ_ONLY: Final[int] = CTLG_BASE + 55

    def __init__():
        raise NotImplementedError("DataCloudTags cannot be instantiated")


class DatacenterCharacteristics:
    TIME_SHARED: int = 0
    SPACE_SHARED: int = 1
    OTHER_POLICY_SAME_RATING: int = 2
    OTHER_POLICY_DIFFERENT_RATING: int = 3
    ADVANCE_RESERVATION: int = 4

    def __init__(self, architecture: str, os: str, vmm: str, hostList: List[Vm.Host], timeZone: float, cost_per_sec: float, costPerMem: float, costPerStorage: float, costPerBw: float) -> None:
        self.id: int = -1
        self.architecture: str = architecture
        self.os: str = os
        self.hostList: List[Vm.Host] = hostList
        self.timeZone: float = timeZone
        self.costPerSecond: float = cost_per_sec
        self.allocationAolicy: int = None
        self.vmm: str = vmm
        self.costPerMem: float = costPerMem
        self.costPerStorage: float = costPerStorage
        self.costPerBw: float = costPerBw


    def get_resource_name(self) -> str:
        return str(self.id)
    

    def get_host_with_free_pe(self, pe_number: int = None) -> Vm.Host:
        if pe_number is None:
            return HostList.get_host_with_free_pe(self.hostList)
        else:
            return HostList.get_host_with_free_pe(self.hostList, pe_number)


    def get_mips_of_one_pe(self, host_id: int = None, pe_id: int = None) -> int:
        if not self.hostList:
            return -1
        
        if host_id is None and pe_id is None:
            return lists.PeList.get_mips(self.hostList[0].get_pe_list(), 0)
        elif host_id is not None and pe_id is not None:
            host = HostList.get_by_id(self.hostList, host_id)
            return lists.PeList.get_mips(host.get_pe_list(), pe_id)
        else:
            return -1


    def get_mips(self) -> int:
        mips = 0
        if not self.hostList:
            return mips

        if self.allocationAolicy == DatacenterCharacteristics.TIME_SHARED or \
           self.allocationAolicy == DatacenterCharacteristics.OTHER_POLICY_SAME_RATING:
            mips = self.get_mips_of_one_pe() * HostList.get_number_of_pes(self.hostList)
        elif self.allocationAolicy == DatacenterCharacteristics.SPACE_SHARED or \
             self.allocationAolicy == DatacenterCharacteristics.OTHER_POLICY_DIFFERENT_RATING:
            for host in self.hostList:
                mips += host.get_total_mips()
        
        return mips


    def get_cpu_time(self, cloudlet_length: float, load: float) -> float:
        cpu_time = 0.0
        if self.allocationAolicy == DatacenterCharacteristics.TIME_SHARED:
            cpu_time = cloudlet_length / (self.get_mips_of_one_pe() * (1.0 - load))
        return cpu_time


    def get_number_of_pes(self) -> int:
        return HostList.get_number_of_pes(self.hostList)


    def get_number_of_free_pes(self) -> int:
        return HostList.get_number_of_free_pes(self.hostList)


    def get_number_of_busy_pes(self) -> int:
        return HostList.get_number_of_busy_pes(self.hostList)


    def set_pe_status(self, status: int, host_id: int, pe_id: int) -> bool:
        return HostList.set_pe_status(self.hostList, status, host_id, pe_id)


    def get_cost_per_mi(self) -> float:
        return self.costPerSecond / self.get_mips_of_one_pe()


    def get_number_of_hosts(self) -> int:
        return len(self.hostList)


    def get_number_of_failed_hosts(self) -> int:
        return sum(1 for host in self.hostList if host.is_failed())


    def is_working(self) -> bool:
        return self.get_number_of_failed_hosts() == 0


    def get_cost_per_mem(self) -> float:
        return self.costPerMem


    def set_cost_per_mem(self, costPerMem: float) -> None:
        self.costPerMem = costPerMem


    def get_cost_per_storage(self) -> float:
        return self.costPerStorage


    def set_cost_per_storage(self, costPerStorage: float) -> None:
        self.costPerStorage = costPerStorage


    def get_cost_per_bw(self) -> float:
        return self.costPerBw


    def set_cost_per_bw(self, costPerBw: float) -> None:
        self.costPerBw = costPerBw


    def get_vmm(self) -> str:
        return self.vmm


    def get_id(self) -> int:
        return self.id


    def set_id(self, id: int) -> None:
        self.id = id


    def get_architecture(self) -> str:
        return self.architecture


    def set_architecture(self, architecture: str) -> None:
        self.architecture = architecture


    def get_os(self) -> str:
        return self.os


    def set_os(self, os: str) -> None:
        self.os = os


    def get_host_list(self) -> List[Vm.Host]:
        return self.hostList


    def set_host_list(self, hostList: List[Vm.Host]) -> None:
        self.hostList = hostList


    def get_time_zone(self) -> float:
        return self.timeZone


    def set_time_zone(self, timeZone: float) -> None:
        self.timeZone = timeZone


    def get_cost_per_second(self) -> float:
        return self.costPerSecond


    def set_cost_per_second(self, costPerSecond: float) -> None:
        self.costPerSecond = costPerSecond


    def get_allocation_policy(self) -> int:
        return self.allocationAolicy


    def set_allocation_policy(self, allocationAolicy: int) -> None:
        self.allocationAolicy = allocationAolicy


    def set_vmm(self, vmm: str) -> None:
        self.vmm = vmm



class Datacenter(SimEntity):
    regionalCisName: str = None
    def __init__(self, name: str, characteristics: DatacenterCharacteristics, 
                 vmAllocationPolicy: VmAllocationPolicy, storage_list: List[Storage], 
                 scheduling_interval: float) -> None:
        super().__init__(name)
        self.characteristics: DatacenterCharacteristics = characteristics
        self.vmAllocationPolicy: VmAllocationPolicy = vmAllocationPolicy
        self.lastProcessTime: float = 0.0
        self.storageList: List[Storage] = storage_list
        self.vmList: List[Vm.Vm] = []
        self.schedulingInterval: float = scheduling_interval

        for host in self.get_characteristics().get_host_list():
            host.set_datacenter(self)

        # If this resource doesn't have any PEs then no useful at all 
        if self.get_characteristics().get_number_of_pes() == 0:
            raise Exception(f"{super().get_name()}: Error - this entity has no PEs. Therefore, can't process any Cloudlets.")
        
        # stores id of this class
        self.get_characteristics().set_id(super().get_id())
    
    def register_other_entity(self) -> None:
        pass

    
    def process_event(self, ev: SimEvent) -> None:
        src_id: int = -1
        tag: int = ev.get_tag()
        data: Any = ev.get_data()

        # Resource characteristics inquiry
        if tag == CloudSimTags.RESOURCE_CHARACTERISTICS:
            src_id = int(data)
            self.send_now(src_id, tag, self.get_characteristics())

        # Resource dynamic info inquiry
        elif tag == CloudSimTags.RESOURCE_DYNAMICS:
            src_id = int(data)
            self.send_now(src_id, tag, 0)

        elif tag == CloudSimTags.RESOURCE_NUM_PE:
            src_id = int(data)
            num_pe: int = self.get_characteristics().get_number_of_pes()
            self.send_now(src_id, tag, num_pe)

        elif tag == CloudSimTags.RESOURCE_NUM_FREE_PE:
            src_id = int(data)
            free_pes_number: int = self.get_characteristics().get_number_of_free_pes()
            self.send_now(src_id, tag, free_pes_number)

        elif tag == CloudSimTags.CLOUDLET_SUBMIT:
            self.process_cloudlet_submit(ev, False)

        elif tag == CloudSimTags.CLOUDLET_SUBMIT_ACK:
            self.process_cloudlet_submit(ev, True)

        elif tag == CloudSimTags.CLOUDLET_CANCEL:
            self.process_cloudlet(ev, CloudSimTags.CLOUDLET_CANCEL)

        elif tag == CloudSimTags.CLOUDLET_PAUSE:
            self.process_cloudlet(ev, CloudSimTags.CLOUDLET_PAUSE)

        elif tag == CloudSimTags.CLOUDLET_PAUSE_ACK:
            self.process_cloudlet(ev, CloudSimTags.CLOUDLET_PAUSE_ACK)

        elif tag == CloudSimTags.CLOUDLET_RESUME:
            self.process_cloudlet(ev, CloudSimTags.CLOUDLET_RESUME)

        elif tag == CloudSimTags.CLOUDLET_RESUME_ACK:
            self.process_cloudlet(ev, CloudSimTags.CLOUDLET_RESUME_ACK)

        elif tag == CloudSimTags.CLOUDLET_MOVE:
            self.process_cloudlet_move(data, CloudSimTags.CLOUDLET_MOVE)

        elif tag == CloudSimTags.CLOUDLET_MOVE_ACK:
            self.process_cloudlet_move(data, CloudSimTags.CLOUDLET_MOVE_ACK)
        
        elif tag == CloudSimTags.CLOUDLET_STATUS:
            self.process_cloudlet_status(ev)

        elif tag == CloudSimTags.INFOPKT_SUBMIT:
            self.process_ping_request(ev)

        elif tag == CloudSimTags.VM_CREATE:
            self.process_vm_create(ev, False)

        elif tag == CloudSimTags.VM_CREATE_ACK:
            self.process_vm_create(ev, True)

        elif tag == CloudSimTags.VM_DESTROY:
            self.process_vm_destroy(ev, False)

        elif tag == CloudSimTags.VM_DESTROY_ACK:
            self.process_vm_destroy(ev, True)

        elif tag == CloudSimTags.VM_MIGRATE:
            self.process_vm_migrate(ev, False)

        elif tag == CloudSimTags.VM_MIGRATE_ACK:
            self.process_vm_migrate(ev, True)

        elif tag == CloudSimTags.VM_DATA_ADD:
            self.process_data_add(ev, False)

        elif tag == CloudSimTags.VM_DATA_ADD_ACK:
            self.process_data_add(ev, True)

        elif tag == CloudSimTags.VM_DATA_DEL:
            self.process_data_delete(ev, False)

        elif tag == CloudSimTags.VM_DATA_DEL_ACK:
            self.process_data_delete(ev, True)

        elif tag == CloudSimTags.VM_DATACENTER_EVENT:
            self.update_cloudlet_processing()
            self.check_cloudlet_completion()

        else:
            self.process_other_event(ev)


    def process_data_delete(self, ev: SimEvent, ack: bool) -> None:
        if ev is None:
            return
        data: Union[None, Tuple[str, int]] = ev.get_data()
        if data is None:
            return
        filename: str = data[0]
        req_source: int = data[1]
        tag: int = -1

        msg: int = self.delete_file_from_storage(filename)

        if msg == DataCloudTags.FILE_DELETE_SUCCESSFUL:
            tag = DataCloudTags.CTLG_DELETE_MASTER
        else:
            tag = DataCloudTags.FILE_DELETE_MASTER_RESULT

        if ack:
            pack: Tuple[str, int] = (filename, msg)
            self.send_now(req_source, tag, pack)


    def process_data_add(self, ev: SimEvent, ack: bool) -> None:
        if ev is None:
            return
        pack: Union[None, Tuple[File, int]] = ev.get_data()
        if pack is None:
            return
        file: File = pack[0]
        file.set_master_copy(True)
        sent_from: int = pack[1]

        if ack:
            data: Tuple[str, int, int] = [file.get_name(), -1, self.add_file(file)]
            self.send_now(sent_from, DataCloudTags.FILE_ADD_MASTER_RESULT, data)


    def process_ping_request(self, ev: SimEvent) -> None:
        pkt: InfoPacket = ev.get_data()
        pkt.set_tag(CloudSimTags.INFOPKT_RETURN)
        pkt.set_dest_id(pkt.get_src_id())
        # sends back to the sender
        self.send_now(pkt.get_src_id(), CloudSimTags.INFOPKT_RETURN, pkt)

    def process_cloudlet_status(self, ev: SimEvent) -> None:
        cloudlet_id: int = 0
        userId: int = 0
        vm_id: int = 0
        status: int = -1

        try:
            data: Tuple[int, int, int] = ev.get_data()
            cloudlet_id = data[0]
            userId = data[1]
            vm_id = data[2]
            status = self.get_vm_allocation_policy().get_host(vm_id, userId).get_vm(vm_id, userId) \
                .get_cloudlet_scheduler().get_cloudlet_status(cloudlet_id)
        except Exception as c:
            try:
                cl: Cloudlet = cast(Cloudlet, ev.get_data())
                cloudlet_id = cl.get_cloudlet_id()
                userId = cl.get_user_id()
            
                status = self.get_vm_allocation_policy().get_host(vm_id, userId).get_vm(vm_id, userId) \
                    .get_cloudlet_scheduler().get_cloudlet_status(cloudlet_id)
            except Exception as e:
                Log.print_line(f"Error in processing CloudletStatus with ID {cloudlet_id}: {e}")
                Log.print_line(str(e))
                return
            
        array: Tuple[int, int, int] = [self.get_id(), cloudlet_id, status]
        tag: int = CloudSimTags.CLOUDLET_STATUS
        self.send_now(userId, tag, array)


    def process_other_event(self, ev: SimEvent) -> None:
        if ev is None:
            Log.print_line(f"{self.get_name()}.process_other_event(): Error - an event is null.")


    def process_vm_create(self, ev: SimEvent, ack: bool) -> None:
        vm: Vm.Vm = cast(Vm.Vm, ev.get_data())
        result: bool = self.get_vm_allocation_policy().allocate_host_for_vm(vm)
        if ack:
            data = [self.get_id(), vm.get_id()]
            if result:
                data.append(CloudSimTags.TRUE)
            else:
                data.append(CloudSimTags.FALSE)
            self.send(vm.get_user_id(), CloudSim.get_min_time_between_events(), CloudSimTags.VM_CREATE_ACK, data)

        if result:
            self.get_vm_list().append(vm)

            if vm.is_being_instantiated():
                vm.set_being_instantiated(False)
            
            host: Vm.Host = cast(Vm.Host, self.get_vm_allocation_policy().get_host(vm))
            vm.update_vm_processing(CloudSim.clock(), host.get_vm_scheduler()
                                    .get_allocated_mips_for_vm(vm))


    def process_vm_destroy(self, ev: SimEvent, ack: bool) -> None:
        vm: Vm.Vm = cast(Vm.Vm, ev.get_data())
        self.get_vm_allocation_policy().deallocate_host_for_vm(vm)
        if ack:
            data = [self.get_id(), vm.get_id(), CloudSimTags.TRUE]
            self.send_now(vm.get_user_id(), CloudSimTags.VM_DESTROY_ACK, data)
        self.get_vm_list().remove(vm)


    def process_vm_migrate(self, ev: SimEvent, ack: bool) -> None:
        tmp: object = ev.get_data()
    
        if not isinstance(tmp, dict):
            raise TypeError("The data object must be a dictionary")

        migrate: Dict[str, object] = cast(Dict[str, object], tmp)
        
        vm: Vm.Vm = cast(Vm.Vm, migrate["vm"])
        host: Vm.Host = cast(Vm.Host, migrate["host"])

        self.get_vm_allocation_policy().deallocate_host_for_vm(vm)
        host.remove_migrating_in_vm(vm)
        result: bool = self.get_vm_allocation_policy().allocate_host_for_vm(vm, host)
        
        if not result:
            Log.print_line("[Datacenter.processVmMigrate] VM allocation to the destination host failed")
            sys.exit(0)

        if ack:
            data: List[int] = [self.get_id(), vm.get_id()]

            if result:
                data.append(CloudSimTags.TRUE)
            else:
                data.append(CloudSimTags.FALSE)
            
            self.send_now(ev.get_source(), CloudSimTags.VM_CREATE_ACK, data)

        Log.format_line(f"{CloudSim.clock():.2f}: Migration of VM #{vm.get_id()} to Host #{host.get_id()} is completed")
        vm.set_in_migration(False)


    def process_cloudlet(self, ev: SimEvent, type_: int) -> None:
        cloudlet_id: int = 0
        userId: int = 0
        vm_id: int = 0

        try:  # if the sender is using cloudletXXX() methods
            data: List[int] = cast(List[int], ev.get_data())
            cloudlet_id = data[0]
            userId = data[1]
            vm_id = data[2]

        except (TypeError, IndexError):  # if the sender is using normal send() methods
            try:
                cl: Cloudlet = cast(Cloudlet, ev.get_data())
                cloudlet_id = cl.get_cloudlet_id()
                userId = cl.get_user_id()
                vm_id = cl.get_vm_id()
            except Exception as e:
                Log.print_line(self.get_name() + ": Error in processing Cloudlet")
                Log.print_line(str(e))
                return
        except Exception as e:
            Log.print_line(self.get_name() + ": Error in processing Cloudlet")
            Log.print_line(str(e))
            return

        # begins executing ....
        if type_ == CloudSimTags.CLOUDLET_CANCEL:
            self.process_cloudlet_cancel(cloudlet_id, userId, vm_id)
        elif type_ == CloudSimTags.CLOUDLET_PAUSE:
            self.process_cloudlet_pause(cloudlet_id, userId, vm_id, False)
        elif type_ == CloudSimTags.CLOUDLET_PAUSE_ACK:
            self.process_cloudlet_pause(cloudlet_id, userId, vm_id, True)
        elif type_ == CloudSimTags.CLOUDLET_RESUME:
            self.process_cloudlet_resume(cloudlet_id, userId, vm_id, False)
        elif type_ == CloudSimTags.CLOUDLET_RESUME_ACK:
            self.process_cloudlet_resume(cloudlet_id, userId, vm_id, True)


    def process_cloudlet_move(self, received_data: List[int], type_: int) -> None:
        self.update_cloudlet_processing()

        array: List[int] = received_data
        cloudlet_id: int = array[0]
        userId: int = array[1]
        vm_id: int = array[2]
        vm_dest_id: int = array[3]
        dest_id: int = array[4]

        # get the cloudlet
        host: Vm.Host = cast(Vm.Host, self.get_vm_allocation_policy().get_host(vm_id, userId))
        cl: Cloudlet = host.get_vm(vm_id, userId).get_cloudlet_scheduler().cloudlet_cancel(cloudlet_id)

        failed: bool = False
        if cl is None:  # cloudlet doesn't exist
            failed = True
        else:
            # has the cloudlet already finished?
            if cl.get_cloudlet_status() == Cloudlet.SUCCESS:  # if yes, send it back to the user
                data: List[int] = [self.get_id(), cloudlet_id, 0]
                self.send_now(cl.get_user_id(), CloudSimTags.CLOUDLET_SUBMIT_ACK, data)
                self.send_now(cl.get_user_id(), CloudSimTags.CLOUDLET_RETURN, cl)

            # prepare cloudlet for migration
            cl.set_vm_id(vm_dest_id)

            # the cloudlet will migrate from one VM to another, does the destination VM exist?
            if dest_id == self.get_id():
                vm: Vm.Vm = self.get_vm_allocation_policy().get_host(vm_dest_id, userId).get_vm(vm_dest_id, userId)
                if vm is None:
                    failed = True
                else:
                    # time to transfer the files
                    file_transfer_time: float = self.predict_file_transfer_time(cl.get_required_files())
                    vm.get_cloudlet_scheduler().cloudlet_submit(cl, file_transfer_time)
            else:  # the cloudlet will migrate from one resource to another
                tag: int = CloudSimTags.CLOUDLET_SUBMIT_ACK if type_ == CloudSimTags.CLOUDLET_MOVE_ACK else CloudSimTags.CLOUDLET_SUBMIT
                self.send_now(dest_id, tag, cl)

        if type_ == CloudSimTags.CLOUDLET_MOVE_ACK:  # send ACK if requested
            data: List[int] = [self.get_id(), cloudlet_id, 0] if failed else [self.get_id(), cloudlet_id, 1]
            self.send_now(cl.get_user_id(), CloudSimTags.CLOUDLET_SUBMIT_ACK, data)


    def process_cloudlet_submit(self, ev: SimEvent, ack: bool) -> None:
        self.update_cloudlet_processing()

        try:
            # gets the Cloudlet object
            cl: Cloudlet = ev.get_data()

            # checks whether this Cloudlet has finished or not
            if cl.is_finished():
                name: str = CloudSim.get_entity_name(cl.get_user_id())
                Log.print_line(self.get_name() + ": Warning - Cloudlet #" + str(cl.get_cloudlet_id()) +
                               " owned by " + name + " is already completed/finished.")
                Log.print_line("Therefore, it is not being executed again")
                Log.print_line()

                if ack:
                    data: List[int] = [self.get_id(), cl.get_cloudlet_id(), CloudSimTags.FALSE]

                    # unique tag = operation tag
                    tag: int = CloudSimTags.CLOUDLET_SUBMIT_ACK
                    self.send_now(cl.get_user_id(), tag, data)

                self.send_now(cl.get_user_id(), CloudSimTags.CLOUDLET_RETURN, cl)

                return

            # process this Cloudlet to this CloudResource
            cl.set_resource_parameter(self.get_id(), self.get_characteristics().get_cost_per_second(),
                                      self.get_characteristics().get_cost_per_bw())

            userId: int = cl.get_user_id()
            vm_id: int = cl.get_vm_id()

            # time to transfer the files
            file_transfer_time: float = self.predict_file_transfer_time(cl.get_required_files())

            host: Vm.Host = cast(Vm.Host, self.get_vm_allocation_policy().get_host(vm_id, userId))
            vm: Vm.Vm = host.get_vm(vm_id, userId)
            scheduler: CloudletScheduler = vm.get_cloudlet_scheduler()
            estimated_finish_time: float = scheduler.cloudlet_submit(cl, file_transfer_time)

            # if this cloudlet is in the exec queue
            if estimated_finish_time > 0.0 and not math.isinf(estimated_finish_time):
                estimated_finish_time += file_transfer_time
                self.send(self.get_id(), estimated_finish_time, CloudSimTags.VM_DATACENTER_EVENT)

            if ack:
                data: List[int] = [self.get_id(), cl.get_cloudlet_id(), CloudSimTags.TRUE]

                # unique tag = operation tag
                tag: int = CloudSimTags.CLOUDLET_SUBMIT_ACK
                self.send_now(cl.get_user_id(), tag, data)

        except Exception as e:
            Log.print_line(self.get_name() + ".process_cloudlet_submit(): " + "Exception error.")
            Log.print_line(str(e))

        self.check_cloudlet_completion()


    def predict_file_transfer_time(self, required_files: List[str]) -> float:
        time: float = 0.0
        files: Iterator[str] = iter(required_files)
        for file_name in files:
            for i in range(len(self.get_storage_list())):
                temp_storage: Storage = self.get_storage_list()[i]
                temp_file: File = temp_storage.get_file(file_name)
                if temp_file is not None:
                    time += temp_file.get_size() / temp_storage.get_max_transfer_rate()
                    break
        return time
    
    
    def process_cloudlet_resume(self, cloudlet_id: int, userId: int, vm_id: int, ack: bool) -> None:
        host: Vm.Host = cast(Vm.Host, self.get_vm_allocation_policy().get_host(vm_id, userId))
        event_time: float = host.get_vm(vm_id, userId) \
            .get_cloudlet_scheduler().cloudlet_resume(cloudlet_id)

        status: bool = False
        if event_time > 0.0:  # if this cloudlet is in the exec queue
            status = True
            if event_time > CloudSim.clock():
                self.schedule(self.get_id(), event_time, CloudSimTags.VM_DATACENTER_EVENT)

        if ack:
            data: List[int] = [self.get_id(), cloudlet_id]
            if status:
                data.append(CloudSimTags.TRUE)
            else:
                data.append(CloudSimTags.FALSE)
            self.send_now(userId, CloudSimTags.CLOUDLET_RESUME_ACK, data)


    def process_cloudlet_pause(self, cloudlet_id: int, userId: int, vm_id: int, ack: bool) -> None:
        host: Vm.Host = cast(Vm.Host, self.get_vm_allocation_policy().get_host(vm_id, userId))
        status: bool = host.get_vm(vm_id, userId).get_cloudlet_scheduler().cloudlet_pause(cloudlet_id)
        
        if ack:
            data: List[int] = [self.get_id(), cloudlet_id]
            if status:
                data.append(CloudSimTags.TRUE)
            else:
                data.append(CloudSimTags.FALSE)
            self.send_now(userId, CloudSimTags.CLOUDLET_PAUSE_ACK, data)


    def process_cloudlet_cancel(self, cloudlet_id: int, userId: int, vm_id: int) -> None:
        host: Vm.Host = cast(Vm.Host, self.get_vm_allocation_policy().get_host(vm_id, userId))
        cl: Cloudlet = host.get_vm(vm_id, userId).get_cloudlet_scheduler().cloudlet_cancel(cloudlet_id)
        self.send_now(userId, CloudSimTags.CLOUDLET_CANCEL, cl)


    def update_cloudlet_processing(self) -> None:
        if CloudSim.clock() < 0.111 or CloudSim.clock() > self.get_last_process_time() + CloudSim.get_min_time_between_events():
            hostList: List[Vm.Host] = self.get_vm_allocation_policy().get_host_list()
            smaller_time = float('inf')
            # For each host...
            for host in hostList:
                # Inform VMs to update processing
                time = host.update_vms_processing(CloudSim.clock())
                # Update the smallest time
                if time < smaller_time:
                    smaller_time = time
            # Ensure a minimal interval before scheduling the event
            if smaller_time < CloudSim.clock() + CloudSim.get_min_time_between_events() + 0.01:
                smaller_time = CloudSim.clock() + CloudSim.get_min_time_between_events() + 0.01
            if smaller_time != float('inf'):
                self.schedule(self.get_id(), (smaller_time - CloudSim.clock()), CloudSimTags.VM_DATACENTER_EVENT)
            self.set_last_process_time(CloudSim.clock())


    def check_cloudlet_completion(self) -> None:
        hostList: List[Vm.Host] = self.get_vm_allocation_policy().get_host_list()
        for host in hostList:
            for vm in host.get_vm_list():
                while vm.get_cloudlet_scheduler().is_finished_cloudlets():
                    cl = vm.get_cloudlet_scheduler().get_next_finished_cloudlet()
                    if cl is not None:
                        self.send_now(cl.get_user_id(), CloudSimTags.CLOUDLET_RETURN, cl)


    def add_file(self, file: File) -> int:
        if file is None:
            return DataCloudTags.FILE_ADD_ERROR_EMPTY

        if self.contains(file.get_name()):
            return DataCloudTags.FILE_ADD_ERROR_EXIST_READ_ONLY

        # Check storage space first
        if len(self.get_storage_list()) <= 0:
            return DataCloudTags.FILE_ADD_ERROR_STORAGE_FULL

        temp_storage: Storage = None
        msg: int = DataCloudTags.FILE_ADD_ERROR_STORAGE_FULL

        for storage in self.get_storage_list():
            temp_storage = storage
            if temp_storage.get_available_space() >= file.get_size():
                temp_storage.add_file(file)
                msg = DataCloudTags.FILE_ADD_SUCCESSFUL
                break

        return msg
    
    
    def contains(self, file_name: Union[File, str]) -> bool:
        if isinstance(file_name, File):
            if file_name is None:
                return False
            file_name = file_name.get_name()

        if file_name is None or len(file_name) == 0:
            return False
        
        for storage in self.get_storage_list():
            if storage.contains(file_name):
                return True
        return False
    
    
    def delete_file_from_storage(self, file_name: str) -> int:
        temp_storage: Storage = None
        temp_file: File = None
        msg: int = DataCloudTags.FILE_DELETE_ERROR

        for i in range(len(self.get_storage_list())):
            temp_storage = self.get_storage_list()[i]
            temp_file = temp_storage.get_file(file_name)
            temp_storage.delete_file(file_name, temp_file)
            msg = DataCloudTags.FILE_DELETE_SUCCESSFUL
        return msg
    
    
    def shutdown_entity(self) -> None:
        Log.print_line(f"{self.get_name()} is shutting down...")
    

    def start_entity(self):
        Log.print_line(f"{self.get_name()} is starting...")

        gis_id: int = CloudSim.get_entity_id(self.regionalCisName)
        if gis_id == -1:
            gis_id = CloudSim.get_cloud_info_service_entity_id()
        # Send the registration to GIS
        self.send_now(gis_id, CloudSimTags.REGISTER_RESOURCE, self.get_id())
        # Below method is for a child class to override
        self.register_other_entity()


    def get_host_list(self) -> List[Vm.Host]:
        return (self.get_characteristics().get_host_list())
    

    def get_characteristics(self) -> DatacenterCharacteristics:
        return self.characteristics
    

    def set_characteristics(self, characteristics: DatacenterCharacteristics):
        self.characteristics = characteristics


    def get_regional_cis_name(self) -> str:
        return self.regionalCisName
    

    def set_regional_cis_name(self, regionalCisName: str):
        self.regionalCisName = regionalCisName

    def get_vm_allocation_policy(self) -> VmAllocationPolicy:
        return self.vmAllocationPolicy
    

    def set_vm_allocation_policy(self, vmAllocationPolicy: VmAllocationPolicy):
        self.vmAllocationPolicy = vmAllocationPolicy


    def get_last_process_time(self) -> float:
        return self.lastProcessTime
    

    def set_last_process_time(self, lastProcessTime: float):
        self.lastProcessTime = lastProcessTime


    def get_storage_list(self) -> List[Storage]:
        return self.storageList
    

    def set_storage_list(self, storageList: List[Storage]):
        self.storageList = storageList


    def get_vm_list(self) -> List[Vm.Vm]:
        return (self.vmList)
    

    def set_vm_list(self, vmList: List[Vm.Vm]):
        self.vmList = vmList


    def set_scheduling_interval(self, schedulingInterval: float):
        self.schedulingInterval = schedulingInterval

    
    def process_ping_reply(self, ev: SimEvent) -> None:
        pkt: InfoPacket = ev.get_data()
        pkt.set_tag(CloudSimTags.INFOPKT_RETURN)
        pkt.set_dest_id(pkt.get_src_id())
        self.send_now(pkt.get_src_id(), CloudSimTags.INFOPKT_RETURN, pkt)
