from __future__ import annotations

from typing import List, Tuple, cast
import math

from cloudsim.Cloudlet import Cloudlet
from cloudsim.CloudletScheduler import CloudletScheduler
from cloudsim.Consts import Consts
from cloudsim.DataCenter import Datacenter, DatacenterCharacteristics
from cloudsim.Vm import Vm, Host
from cloudsim.VmAllocationPolicy import VmAllocationPolicy
from cloudsim.Storage import Storage
from cloudsim.core import CloudSim, CloudSimTags, SimEvent
from cloudsim.Log import Log
from workflowsim.utils.ReplicaCatalog import ReplicaCatalog
from workflowsim.utils.Parameters import Parameters, CostModel, ClassType, FileType
from workflowsim.Job import Job
from workflowsim.Task import Task
from workflowsim.CustomVM import CustomVM
from workflowsim.FileItem import FileItem


class WorkflowDatacenter(Datacenter):
    def __init__(self, name: str, characteristics: DatacenterCharacteristics, vmAllocationPolicy: VmAllocationPolicy,
                 storageList: List[Storage], schedulingInterval: float) -> None:
        super().__init__(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval)


    def process_cloudlet_submit(self, ev: SimEvent, ack: float) -> None:
        self.update_cloudlet_processing()
        # cl is actually a job but it is not necessary to cast it to a job
        job: Job = cast(Job, ev.get_data())
        if (job.is_finished()):
            name: str = CloudSim.get_entity_name(job.get_user_id())
            Log.print_line(f"""{self.get_name()}: Warning - Cloudlet #{job.get_cloudlet_id()} 
                    owned by {name} is already completed/finished""")
            Log.print_line("Therefore, it is not being executed again.\n")
            if (ack):
                data: Tuple[int, int, int] = [self.get_id(), job.get_cloudlet_id(), CloudSimTags.FALSE]
                # unique tag = operation tag
                self.send_now(job.get_user_id(), CloudSimTags.CLOUDLET_SUBMIT_ACK, data)
            self.send_now(job.get_user_id(), CloudSimTags.CLOUDLET_RETURN, job)
            return
        userId: int = job.get_user_id()
        vmId: int = job.get_vm_id()
        host: Host = self.get_vm_allocation_policy().get_host(vmId, userId)
        vm: CustomVM = cast(CustomVM, host.get_vm(vmId, userId))
        cost_model: CostModel = Parameters.getCostModel()
        if cost_model == CostModel.DATACENTER:
            job.set_resource_parameter(self.get_id(), self.get_characteristics().get_cost_per_second(),
                                    self.get_characteristics().get_cost_per_bw())
        elif cost_model == CostModel.VM:
            job.set_resource_parameter(self.get_id(), vm.get_cost(), vm.get_cost_per_bw())
        else:
            pass  # Default case
        # Stage-in file && Shared based on the file.system
        if (job.get_class_type()==ClassType.STAGE_IN):
            self.stage_in_file2filesystem(job)
        # Add data transfer time (communication cost)
        fileTransferTime: float = 0.0
        if (job.get_class_type()==ClassType.COMPUTE):
            fileTransferTime = self.process_data_stage_in_for_compute_job(job.get_fileList(), job)
        scheduler: CloudletScheduler = vm.get_cloudlet_scheduler()
        estimatedFinishTime: float = scheduler.cloudlet_submit(job, fileTransferTime)
        self.update_task_exec_time(job, vm)
        # if this cloudlet is in the exec queue
        if (estimatedFinishTime > 0.0 and not math.isinf(estimatedFinishTime)):
            self.send(self.get_id(), estimatedFinishTime, CloudSimTags.VM_DATACENTER_EVENT)
        else:
            Log.print_line("Warning: You schedule cloudlet to a busy VM.")
        if (ack):
            data: Tuple[int, int, int] = [self.get_id(), job.get_cloudlet_length(), CloudSimTags.TRUE]
            self.send_now(job.get_user_id(), CloudSimTags.CLOUDLET_SUBMIT_ACK, data)
        self.check_cloudlet_completion()


    def update_task_exec_time(self, job: Job, vm: Vm) -> None:
        startTime: float = job.get_exec_start_time()
        for task in job.get_task_list():
            task.set_exec_start_time(startTime)
            taskRuntime: float = task.get_cloudlet_length() / vm.get_mips()
            startTime += taskRuntime
            # Because CloudSim would not let us update end time here
            task.set_taskFinishTime(startTime)


    def stage_in_file2filesystem(self, job: Job) -> None:
        fList: List[FileItem] = job.get_fileList()
        for file in fList:
            fileSysyem: ReplicaCatalog.FileSystem = ReplicaCatalog.get_file_system()
            if (fileSysyem == ReplicaCatalog.FileSystem.LOCAL):
                ReplicaCatalog.add_file_to_storage(file.get_name(), self.get_name())
            elif (fileSysyem == ReplicaCatalog.FileSystem.SHARED):
                ReplicaCatalog.add_file_to_storage(file.get_name(), self.get_name())
            else:
                print("PASS")
                pass


    def process_data_stage_in_for_compute_job(self, requiredFiles: List[FileItem], job: Job) -> float:
        time: float = 0.0
        for file in requiredFiles:
            # The input file is not an output File
            if (file.is_real_input_file(requiredFiles)):
                maxBwth: float = 0.0
                siteList: List[str] = ReplicaCatalog.get_storage_list(file.get_name())
                if (len(siteList) == 0):
                    raise Exception(file.get_name() + " does not exist")
                file_system = ReplicaCatalog.get_file_system()
                if file_system == ReplicaCatalog.FileSystem.SHARED:
                    maxRate: float = float('-inf')
                    for storage in self.get_storage_list():
                        rate: float = storage.get_max_transfer_rate()
                        maxRate = max(rate, maxRate)
                    time += file.get_size() / float(Consts.MILLION) / maxRate
                elif file_system == ReplicaCatalog.FileSystem.LOCAL:
                    vmId: int = job.get_vm_id()
                    userId: int = job.get_user_id()
                    host: Host = self.get_vm_allocation_policy().get_host(vmId, userId)
                    vm: Vm = host.get_vm(vmId, userId)
                    requiredFileStagein: bool = True
                    for site in siteList:
                        # /site is where one replica of this data is located at
                        if site == self.get_name():
                            continue
                        if site == str(vmId):
                            # This file is already in the local vm and thus it is no need to transfer
                            requiredFileStagein = False
                            break
                        bwth: float = 0.0
                        if site == Parameters.SOURCE:
                            # transfers from the source to the VM is limited to the VM bw only
                            bwth = vm.get_bw()
                        else:
                            # transfers between two VMs is limited to both VMs
                            bwth = min(vm.get_bw(), self.get_vm_allocation_policy().get_host(int(site), userId).get_vm(int(site), userId).get_bw())
                        maxBwth = max(bwth, maxBwth)
                    if requiredFileStagein and maxBwth > 0.0:
                        time += file.size / float(Consts.MILLION) / maxBwth
                    #  For the case when storage is too small it is not handled here
                    ReplicaCatalog.add_file_to_storage(file.name, str(vmId))
        return time
    

    def update_cloudlet_processing(self) -> None:
        if CloudSim.clock() < 0.111 or CloudSim.clock() > self.get_last_process_time() + 0.01:
            hostList: List[Host] = self.get_vm_allocation_policy().get_host_list()
            smaller_time = float('inf')
            # For each host...
            for host in hostList:
                # Inform VMs to update processing
                time = host.update_vms_processing(CloudSim.clock())
                # What time do we expect that the next cloudlet will finish?
                smaller_time = min(time, smaller_time)
            # Ensure a minimal interval before scheduling the event
            if smaller_time < CloudSim.clock() + 0.11:
                smaller_time = CloudSim.clock() + 0.11
            if smaller_time != float('inf'):
                self.schedule(self.get_id(), smaller_time - CloudSim.clock(), CloudSimTags.VM_DATACENTER_EVENT)
            self.lastProcessTime = CloudSim.clock()


    def check_cloudlet_completion(self) -> None:
        hostList: List[Host] = self.get_vm_allocation_policy().get_host_list()
        for host in hostList:
            for vm in host.get_vm_list():
                while vm.get_cloudlet_scheduler().is_finished_cloudlets():
                    cl: Cloudlet = vm.get_cloudlet_scheduler().get_next_finished_cloudlet()
                    if cl is not None:
                        self.send_now(cl.get_user_id(), CloudSimTags.CLOUDLET_RETURN, cl)
                        self.register(cl)


    def register(self, cl: Cloudlet) -> None:
        tl: Task = cast(Task, cl)
        fList: List[FileItem] = tl.get_fileList()
        for file in fList:
            if file.get_type().value == FileType.OUTPUT:  # Check if it's an output file
                file_system = ReplicaCatalog.get_file_system()
                if file_system == ReplicaCatalog.FileSystem.SHARED:
                    ReplicaCatalog.add_file_to_storage(file.get_name(), self.get_name())
                elif file_system == ReplicaCatalog.FileSystem.LOCAL:
                    vmId = cl.get_vm_id()
                    ReplicaCatalog.add_file_to_storage(file.get_name(), str(vmId))