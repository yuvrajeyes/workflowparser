from __future__ import annotations
import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")

from typing import List, cast

from cloudsim.Cloudlet import Cloudlet
from cloudsim.DatacenterBroker import DatacenterBroker
from cloudsim.core import CloudSim, CloudSimTags, SimEvent
from cloudsim.Log import Log
import cloudsim.lists as lists
from workflowsim.failure import FailureGenerator
from workflowsim.scheduling import BaseSchedulingAlgorithm, StaticSchedulingAlgorithm
from workflowsim.utils.Parameters import Parameters, SchedulingAlgorithm
from workflowsim.utils.OverheadParameters import OverheadParameters
from workflowsim.WorkflowSimTags import WorkflowSimTags
from workflowsim.Job import Job
from workflowsim.CustomVM import CustomVM


class WorkflowScheduler(DatacenterBroker):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.workflowEngineId: int = 0
        self.processCloudletSubmitHasShown: bool = False


    def bind_scheduler_datacenter(self, datacenterId: int) -> None:
        if datacenterId <= 0:
            Log.print_line("Error in data center id")
            return
        self.datacenterIdsList.append(datacenterId)


    def set_workflow_engine_id(self, workflowEngineId: int) -> None:
        self.workflowEngineId = workflowEngineId


    def process_event(self, ev: SimEvent) -> None:
        tag = ev.get_tag()
        if tag == CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST:
            self.process_resource_characteristics_request(ev)
        elif tag == CloudSimTags.RESOURCE_CHARACTERISTICS:
            self.process_resource_characteristics(ev)
        elif tag == CloudSimTags.VM_CREATE_ACK:
            self.process_vm_create(ev)
        elif tag == WorkflowSimTags.CLOUDLET_CHECK:
            self.process_cloudlet_return(ev)
        elif tag == CloudSimTags.CLOUDLET_RETURN:
            self.process_cloudlet_return(ev)
        elif tag == CloudSimTags.END_OF_SIMULATION:
            self.shutdown_entity()
        elif tag == CloudSimTags.CLOUDLET_SUBMIT:
            self.process_cloudlet_submit(ev)
        elif tag == WorkflowSimTags.CLOUDLET_UPDATE:
            self.process_cloudlet_update(ev)
        else:
            self.process_other_event(ev)


    def get_scheduler(self, name: SchedulingAlgorithm) -> BaseSchedulingAlgorithm:
        if name == SchedulingAlgorithm.STATIC:
            algorithm: BaseSchedulingAlgorithm = StaticSchedulingAlgorithm()
        else:
            algorithm: BaseSchedulingAlgorithm = StaticSchedulingAlgorithm()
        return algorithm


    def process_vm_create(self, ev: SimEvent) -> None:
        data: List[int] = cast(List[int], ev.get_data())
        datacenter_id, vm_id, result = data[0], data[1], data[2]
        if result == CloudSimTags.TRUE:
            self.get_vms_to_datacenters_map()[vm_id] = datacenter_id
            if lists.VmList.get_by_id(self.get_vm_list(), vm_id) is not None:
                self.get_vms_created_list().append(lists.VmList.get_by_id(self.get_vm_list(), vm_id))
                Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: VM #{vm_id} has been created in Datacenter #{datacenter_id}, Host #{lists.VmList.get_by_id(self.vmsCreatedList, vm_id).get_host().get_id()}")
        else:
            Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: Creation of VM #{vm_id} failed in Datacenter #{datacenter_id}")
        self.increment_vms_acks()
        if len(self.get_vms_created_list()) == len(self.get_vm_list()) - self.get_vms_destroyed():
            self.submit_cloudlets()
        else:
            if self.get_vms_requested() == self.get_vms_acks():
                for next_datacenter_id in self.get_datacenter_ids_list():
                    if next_datacenter_id not in self.get_datacenter_requested_ids_list():
                        self.create_vms_in_datacenter(next_datacenter_id)
                        return  
                if len(self.get_vms_created_list()) > 0:
                    self.submit_cloudlets()
                else:
                    Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: None of the required VMs could be created. Aborting")
                    self.finish_execution()


    def process_cloudlet_update(self, ev: SimEvent) -> None:
        scheduler: BaseSchedulingAlgorithm = self.get_scheduler(Parameters.getSchedulingAlgorithm())
        scheduler.set_cloudlet_list(self.get_cloudlet_list())
        scheduler.set_vm_list(self.get_vms_created_list())
        
        scheduler.run()

        scheduledList: List[Cloudlet] = scheduler.get_scheduled_list()
        for cloudlet in scheduledList:
            vm_id: int = cloudlet.get_vm_id()
            delay: float = 0.0
            op: OverheadParameters = Parameters.getOverheadParams()
            if op.get_queue_delay() is not None:
                delay = op.get_queue_delay(cloudlet)
            self.schedule(self.get_vms_to_datacenters_map()[vm_id], delay, CloudSimTags.CLOUDLET_SUBMIT, cloudlet)
        self.set_cloudlet_list([cloudlet for cloudlet in self.get_cloudlet_list() if cloudlet not in scheduledList])
        self.get_cloudlet_submitted_list().extend(scheduledList)
        self.cloudletsSubmitted += len(scheduledList)


    def process_cloudlet_return(self, ev: SimEvent) -> None:
        cloudlet: Cloudlet = cast(Cloudlet, ev.get_data())
        job: Job = cast(Job, cloudlet)
        # Generate a failure if the failure rate is not zero
        FailureGenerator.generate(job)
        self.get_cloudlet_received_list().append(cloudlet)
        self.get_cloudlet_submitted_list().remove(cloudlet)
        vm: CustomVM = cast(CustomVM, self.get_vms_created_list()[cloudlet.get_vm_id()])
        vm.set_state(WorkflowSimTags.VM_STATUS_IDLE)
        delay: float = 0.0
        op: OverheadParameters = Parameters.getOverheadParams()
        if op.get_post_delay() is not None:
            delay = op.get_post_delay(job)
        self.schedule(self.workflowEngineId, delay, CloudSimTags.CLOUDLET_RETURN, cloudlet)
        self.cloudletsSubmitted -= 1
        self.schedule(self.get_id(), 0.0, WorkflowSimTags.CLOUDLET_UPDATE)


    def start_entity(self) -> None:
        Log.print_line(f"{self.get_name()} is starting...")
        gis_id: int = CloudSim.get_cloud_info_service_entity_id()
        # Send the registration to GIS
        self.send_now(gis_id, CloudSimTags.REGISTER_RESOURCE, self.get_id())


    def shutdown_entity(self) -> None:
        self.clear_datacenters()
        Log.print_line(f"{self.get_name()} is shutting down...")


    def submit_cloudlets(self) -> None:
        self.send_now(self.workflowEngineId, CloudSimTags.CLOUDLET_SUBMIT, None)
    

    def process_cloudlet_submit(self, ev: SimEvent) -> None:
        lst: List[Job] = cast(List, ev.get_data())
        self.get_cloudlet_list().extend(lst)
        self.send_now(self.get_id(), WorkflowSimTags.CLOUDLET_UPDATE)
        if (not self.processCloudletSubmitHasShown):
            self.processCloudletSubmitHasShown = True


    def process_resource_characteristics_request(self, ev: SimEvent) -> None:
        self.set_datacenter_characteristics_list(dict())
        Log.print_line(f"{CloudSim.clock()}: {self.get_name()}: Cloud Resource List received with {len(self.get_datacenter_ids_list())} resource(s).")
        for datacenterId in self.get_datacenter_ids_list():
            self.send_now(datacenterId, CloudSimTags.RESOURCE_CHARACTERISTICS, self.get_id())