from __future__ import annotations

from typing import List, Dict, cast
from cloudsim.Cloudlet import Cloudlet
import cloudsim.Vm as Vm
from cloudsim.DataCenter import DatacenterCharacteristics
from cloudsim.core import CloudSim, SimEntity, SimEvent, CloudSimTags
from cloudsim.lists import CloudletList, VmList
from cloudsim.Log import Log


class DatacenterBroker(SimEntity):
    def __init__(self, name: str):
        super().__init__(name)
        self.vmList: List[Vm.Vm] = []
        self.vmsCreatedList: List[Vm.Vm] = []
        self.cloudletList: List[Cloudlet] = []
        self.cloudletSubmittedList: List[Cloudlet] = []
        self.cloudletReceivedList: List[Cloudlet] = []

        self.cloudletsSubmitted: int = 0
        self.vmsRequested: int = 0
        self.vmsAcks: int = 0
        self.vmsDestroyed: int = 0

        self.datacenterIdsList: List[int] = []
        self.datacenterRequestedIdsList: List[int] = []
        self.vmsToDatacentersMap: Dict[int, int] = {}
        self.datacenterCharacteristicsList: Dict[int, DatacenterCharacteristics] = {}

    def submit_vm_list(self, vm_list: List[Vm.Vm]) -> None:
        self.vmList.extend(vm_list)

    def submit_cloudlet_list(self, cloudlet_list: List[Cloudlet]) -> None:
        self.cloudletList.extend(cloudlet_list)

    def bind_cloudlet_to_vm(self, cloudlet_id: int, vm_id: int) -> None:
        CloudletList.get_by_id(self.get_cloudlet_list)
        for cloudlet in self.cloudletList:
            if cloudlet.get_cloudlet_id() == cloudlet_id:
                cloudlet.set_vm_id(vm_id)
                break

    def process_event(self, ev: SimEvent) -> None:
        tag: int = ev.get_tag()

        if tag == CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
            self.process_resource_characteristics_request(ev)
        elif tag == CloudSimTags.RESOURCE_CHARACTERISTICS:
            self.process_resource_characteristics(ev)
        elif tag == CloudSimTags.VM_CREATE_ACK:
            self.process_vm_create(ev)
        elif tag == CloudSimTags.CLOUDLET_RETURN:
            self.process_cloudlet_return(ev)
        elif tag == CloudSimTags.END_OF_SIMULATION:
            self.shutdown_entity()
        else:
            self.process_other_event(ev)

    def process_resource_characteristics(self, ev: SimEvent) -> None:
        characteristics: DatacenterCharacteristics = cast(DatacenterCharacteristics, ev.get_data())
        self.datacenterCharacteristicsList[characteristics.get_id()] = characteristics

        if len(self.datacenterCharacteristicsList) == len(self.datacenterIdsList):
            self.datacenterRequestedIdsList = []
            self.create_vms_in_datacenter(self.datacenterIdsList[0])

    def process_resource_characteristics_request(self, ev: SimEvent) -> None:
        self.datacenterIdsList = CloudSim.get_cloud_resource_list()
        self.datacenterCharacteristicsList = {}
        Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: Cloud Resource List received with {len(self.datacenterIdsList)} resource(s)")
        for datacenterId in self.datacenterIdsList:
            self.send_now(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, self.get_id())

    def process_vm_create(self, ev: SimEvent) -> None:
        data: List[int] = cast(List[int], ev.get_data())
        datacenterId: int = data[0]
        vmId: int = data[1]
        result: int = data[2]

        if result == CloudSimTags.TRUE:
            self.vmsToDatacentersMap[vmId] = datacenterId
            self.vmsCreatedList.append(VmList.get_by_id(self.vmList, vmId))
            Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: VM #{vmId} has been created in Datacenter #{datacenterId}, Host #\
                  {VmList.get_by_id(self.vmsCreatedList, vmId).get_host().get_id()}")
        else:
            Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: Creation of VM #{vmId} failed in Datacenter #{datacenterId}")

        self.increment_vms_acks()

        if len(self.vmsCreatedList) == len(self.vmList) - self.vmsDestroyed:
            self.submit_cloudlets()
        else:
            if self.vmsRequested == self.vmsAcks:
                for nextDatacenterId in self.datacenterIdsList:
                    if nextDatacenterId not in self.datacenterRequestedIdsList:
                        self.create_vms_in_datacenter(nextDatacenterId)
                        return

                if len(self.vmsCreatedList) > 0:
                    self.submit_cloudlets()
                else:
                    Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: none of the required VMs could be created. Aborting")
                    self.finish_execution()

    def process_cloudlet_return(self, ev: SimEvent) -> None:
        cloudlet: Cloudlet = cast(Cloudlet, ev.get_data())
        self.cloudletReceivedList.append(cloudlet)
        Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: Cloudlet {cloudlet.get_cloudlet_id()} received")
        self.cloudletsSubmitted -= 1

        if len(self.cloudletList) == 0 and self.cloudletsSubmitted == 0:
            Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: All Cloudlets executed. Finishing...")
            self.clear_datacenters()
            self.finish_execution()
        else:
            if len(self.cloudletList) > 0 and self.cloudletsSubmitted == 0:
                self.clear_datacenters()
                self.create_vms_in_datacenter(0)

    def process_other_event(self, ev: SimEvent) -> None:
        if ev is None:
            Log.print_line(f"{self.get_name()}.processOtherEvent(): Error - an event is null.")
            return

        Log.print_line(f"{self.get_name()}.processOtherEvent(): Error - event unknown by this DatacenterBroker.")

    def create_vms_in_datacenter(self, datacenterId: int) -> None:
        requestedVms: int = 0
        datacenterName: str = CloudSim.get_entity_name(datacenterId)
        for vm in self.vmList:
            if vm.get_id() not in self.vmsToDatacentersMap:
                Log.print_line(f"{CloudSim.clock()}: {self.get_name()} Trying to Create VM #{vm.get_id()} in {datacenterName}")
                self.send_now(datacenterId, CloudSimTags.VM_CREATE_ACK, vm)
                requestedVms += 1

        self.datacenterRequestedIdsList.append(datacenterId)
        self.set_vms_requested(requestedVms)
        self.set_vms_acks(0)

    def submit_cloudlets(self) -> None:
        vmIndex: int = 0
        for cloudlet in self.cloudletList:
            if cloudlet.get_vm_id() == -1:
                vm = self.vmsCreatedList[vmIndex]
            else:
                vm = VmList.get_by_id(self.vmsCreatedList, cloudlet.get_vm_id())
                if vm is None:
                    Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: Postponing execution of cloudlet \
                          {cloudlet.get_cloudlet_id()}: bound VM not available")
                    continue

            Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: Sending cloudlet {cloudlet.get_cloudlet_id()} to VM #{vm.get_id()}")
            cloudlet.set_vm_id(vm.get_id())
            self.send_now(self.vmsToDatacentersMap[vm.get_id()], CloudSimTags.CLOUDLET_SUBMIT, cloudlet)
            self.cloudletsSubmitted += 1
            vmIndex = (vmIndex + 1) % len(self.vmsCreatedList)
            self.cloudletSubmittedList.append(cloudlet)
        self.cloudletList = [cloudlet for cloudlet in self.cloudletList if cloudlet not in self.cloudletSubmittedList]

    def clear_datacenters(self) -> None:
        for vm in self.vmsCreatedList:
            Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: Destroying VM #{vm.get_id()}")
            self.send_now(self.vmsToDatacentersMap[vm.get_id()], CloudSimTags.VM_DESTROY, vm)
        self.vmsCreatedList.clear()

    def finish_execution(self) -> None:
        self.send_now(self.get_id(), CloudSimTags.END_OF_SIMULATION)

    def shutdown_entity(self) -> None:
        Log.print_line(f"{self.get_name()} is shutting down...")

    def start_entity(self) -> None:
        Log.print_line(f"{self.get_name()} is starting...")
        self.schedule(self.get_id(), 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST)

    def get_vm_list(self) -> List[Vm.Vm]:
        return cast(List[Vm.Vm], self.vmList)
    
    def set_vm_list(self, vmList: List[Vm.Vm]) -> None:
        self.vmList = vmList

    def get_cloudlet_list(self) -> List[Cloudlet]:
        return cast(List[Cloudlet], self.cloudletList)
    
    def set_cloudlet_list(self, cloudletList: List[Cloudlet]) -> None:
        self.cloudletList = cloudletList

    def get_cloudlet_submitted_list(self) -> List[Cloudlet]:
        return cast(List[Cloudlet], self.cloudletSubmittedList)

    def set_cloudlet_submitted_list(self, cloudletSubmittedList: List[Cloudlet]) -> None:
        self.cloudletSubmittedList = cloudletSubmittedList

    def get_cloudlet_received_list(self) -> List[Cloudlet]:
        return cast(List[Cloudlet], self.cloudletReceivedList)

    def set_cloudlet_received_list(self, cloudletReceivedList: List[Cloudlet]) -> None:
        self.cloudletReceivedList = cloudletReceivedList

    def get_vms_created_list(self) -> List[Vm.Vm]:
        return cast(List[Vm.Vm], self.vmsCreatedList)

    def set_vms_created_list(self, vmsCreatedList: List[Vm.Vm]) -> None:
        self.vmsCreatedList = vmsCreatedList

    def get_vms_requested(self) -> int:
        return self.vmsRequested

    def set_vms_requested(self, vmsRequested: int) -> None:
        self.vmsRequested = vmsRequested

    def get_vms_acks(self) -> int:
        return self.vmsAcks

    def set_vms_acks(self, vmsAcks: int):
        self.vmsAcks = vmsAcks

    def increment_vms_acks(self) -> None:
        self.vmsAcks += 1

    def get_vms_destroyed(self) -> int:
        return self.vmsDestroyed

    def set_vms_destroyed(self, vmsDestroyed: int) -> None:
        self.vmsDestroyed = vmsDestroyed

    def get_datacenter_ids_list(self) -> List[int]:
        return self.datacenterIdsList

    def set_datacenter_ids_list(self, datacenterIdsList: List[int]) -> None:
        self.datacenterIdsList = datacenterIdsList

    def get_vms_to_datacenters_map(self) -> Dict[int, int]:
        return self.vmsToDatacentersMap

    def set_vms_to_datacenters_map(self, vmsToDatacentersMap: Dict[int, int]) -> None:
        self.vmsToDatacentersMap = vmsToDatacentersMap

    def get_datacenter_characteristics_list(self) -> Dict[int, DatacenterCharacteristics]:
        return self.datacenterCharacteristicsList

    def set_datacenter_characteristics_list(self, datacenterCharacteristicsList: Dict[int, DatacenterCharacteristics]) -> None:
        self.datacenterCharacteristicsList = datacenterCharacteristicsList

    def get_datacenter_requested_ids_list(self) -> List[int]:
        return self.datacenterRequestedIdsList

    def set_datacenter_requested_ids_list(self, datacenterRequestedIdsList: List[int]) -> None:
        self.datacenterRequestedIdsList = datacenterRequestedIdsList
