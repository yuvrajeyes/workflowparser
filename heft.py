from __future__ import annotations

from typing import List
import random
import time
import os
from datetime import datetime
from tabulate import tabulate

from cloudsim.core import CloudSim
from cloudsim.Cloudlet import Cloudlet
from cloudsim.CloudletSchedulerSpaceShared import CloudletSchedulerSpaceShared
import cloudsim.Vm as Vm  # Vm, Host
from cloudsim.VmScheduler import VmSchedulerTimeShared
from cloudsim.HarddriveStorage import HarddriveStorage
from cloudsim.DataCenter import DatacenterCharacteristics
from cloudsim.provisioners import RamProvisionerSimple, BwProvisionerSimple, PeProvisionerSimple
from cloudsim.Pe import Pe
from cloudsim.Storage import Storage
from cloudsim.VmAllocationPolicy import VmAllocationPolicySimple

from workflowsim.CondorVM import CondorVM
from workflowsim.CustomVM import CustomVM
from workflowsim.WorkflowDatacenter import WorkflowDatacenter
from workflowsim.Job import Job
from workflowsim.WorkflowEngine import WorkflowEngine
from workflowsim.WorkflowPlanner import WorkflowPlanner
from workflowsim.utils.ClusteringParameters import ClusteringParameters
from workflowsim.utils.OverheadParameters import OverheadParameters
from workflowsim.utils.Parameters import Parameters, SchedulingAlgorithm, PlanningAlgorithm, ClassType
from workflowsim.utils.ReplicaCatalog import ReplicaCatalog
from workflowsim.CustomVMGenerator import CustomVMGenerator
from workflowsim.utils.metrices import Metrics




class HEFTExample():
    @staticmethod
    def create_VM(userId: int, vms: int) -> List[CondorVM]:
        lst: List[CondorVM] = []
        # VM Parameters
        size: int = 10000  # 10000
        ram: int = 512   # vm memory (MB)
        mips: int = 1000
        bw: int = 1000
        pesNumber: int = 1  # number of cpus
        vmm: str = "Xen"    # VMM name

        # create VMs
        vm: List[CondorVM] = [None]*vms
        current_time_millis = int(time.time() * 1000)  # Get current time in milliseconds
        bw_random: random.Random = random.Random(current_time_millis)

        for i in range(vms):
            ratio: float = bw_random.random()  # Generate a random float between 0.0 and 1.0
            vm[i] = CondorVM(i, userId, mips * ratio, pesNumber, ram, int(bw * ratio), size, vmm, CloudletSchedulerSpaceShared())
            lst.append(vm[i])

        return lst
    
    @staticmethod
    def create_datacenter(name):
        # Create a list to store one or more Hosts
        hostList: List[Vm.Host] = []
        # Create Hosts and add them to the list
        for i in range(1, 21):
            peList1: List[Pe] = []
            mips: int = 20000
            # Create PEs and add them to the list
            peList1.append(Pe(0, PeProvisionerSimple(mips)))
            peList1.append(Pe(1, PeProvisionerSimple(mips)))
            host_id: int = 0
            ram: int = 2048  # host memory (MB)
            storage: int = 1000000  # host storage
            bw: int = 10000
            hostList.append(Vm.Host(host_id, RamProvisionerSimple(ram), BwProvisionerSimple(bw),
                    storage, peList1, VmSchedulerTimeShared(peList1)))

        # Create DatacenterCharacteristics object
        arch: str = "x86"
        os: str = "Windows"
        vmm: str = "Xen"
        timeZone: float = 10.0
        cost: float = 3.0
        costPerMem: float = 0.05
        costPerStorage: float = 0.1
        costPerBw: float = 0.1
        storageList: List[Storage] = []
        datacenter: WorkflowDatacenter = None

        characteristics: DatacenterCharacteristics = DatacenterCharacteristics(arch,os,vmm,hostList,timeZone,cost,costPerMem,costPerStorage,costPerBw)

        # Create a storage object
        maxTransferRate: int = 15
        # try:
        s1: HarddriveStorage = HarddriveStorage(name, 1e12)
        s1.set_max_transfer_rate(maxTransferRate)
        storageList.append(s1)
        datacenter = WorkflowDatacenter(name,characteristics,VmAllocationPolicySimple(hostList),storageList,0)
        # except Exception as e:
        #     print(e)

        return datacenter
    

    def print_job_list(job_list: List[Job]):
        table = []

        for job in job_list:
            job_id = job.get_cloudlet_id()
            task_ids = ", ".join(str(task.get_cloudlet_id()) for task in job.get_task_list())
            status = "Stage-in" if job.get_class_type() == ClassType.STAGE_IN else "SUCCESS"
            data_center_id = job.get_resource_id()
            vm_id = job.get_vm_id()
            time = f"{job.get_actual_cpu_time():.2f}"
            start_time = f"{job.get_exec_start_time():.2f}"
            finish_time = f"{job.get_finish_time():.2f}"
            depth = job.depth

            table.append([job_id, task_ids, status, data_center_id, vm_id, time, start_time, finish_time, depth])

        headers = ["Job ID", "Task ID", "STATUS", "Data center ID", "VM ID", "Time", "Start Time", "Finish Time", "Depth"]
        table_str = tabulate(table, headers, tablefmt="grid")

        print("\n========== OUTPUT ==========")
        print(table_str)


    @staticmethod
    def main() -> None:
        vmNum: int = 5  # number of vms

        daxPath: str = "data\Montage_25.xml"
        daxFile = os.path.abspath(daxPath)
        if not os.path.exists(daxFile):
            print("Warning: Please replace dax_path with the physical path in your working environment!")
            return

        # Since we are using HEFT planning algorithm, the scheduling
        # algorithm should be static such that the scheduler would not
        # override the result of the planner
        schMethod: SchedulingAlgorithm = SchedulingAlgorithm.STATIC
        plnMethod: PlanningAlgorithm = PlanningAlgorithm.HEFT
        fileSystem: ReplicaCatalog.FileSystem = ReplicaCatalog.FileSystem.LOCAL

        # No overheads
        op: OverheadParameters = OverheadParameters(0, None, None, None, None, 0)
        # No Clustering
        method: ClusteringParameters.ClusteringMethod = ClusteringParameters.ClusteringMethod.NONE
        cp: ClusteringParameters = ClusteringParameters(0, 0, method, None)
        
        # Initailize static parameters
        Parameters.init(vm=vmNum, dax=daxPath, runtime=None, datasize=None, op=op, cp=cp, scheduler=schMethod, planner=plnMethod, rMethod=None, dl=0)
        ReplicaCatalog.init(fs=fileSystem)
        # before creating any entities
        num_user: int = 1  # number of grid users
        clndr: datetime = datetime.now()
        trace_flag: bool = False

        # Initialize Cloudsim library
        CloudSim.init(num_user=num_user, cal=clndr, traceFlag=trace_flag)
        
        datacenter0: WorkflowDatacenter = HEFTExample.create_datacenter("Datacentor_0")
        
        # Create a WorkflowPlanner with one schedulers 
        wfPlanner: WorkflowPlanner = WorkflowPlanner("planner_0", 1)
        
        # # Create a WorkflowEngine
        wfEngine: WorkflowEngine = wfPlanner.get_workflow_engine()

        # Create a list of VMs.The userId of a vm is basically the id of the scheduler that controls this vm.
        vmlist0: List[CustomVM] = CustomVMGenerator.create_custom_vms(wfEngine.get_scheduler_id(0), Parameters.vmNum)
        
        # Submits this list of vms to this WorkflowEngine.
        wfEngine.submit_vm_list(vmlist0, 0)
        
        # Binds the data centers with the scheduler.
        wfEngine.bind_scheduler_datacenter(datacenter0.get_id(), 0)
    
        CloudSim.start_simulation()
        outputList0: List[Job] = wfEngine.get_jobs_received_list()
        CloudSim.stop_simulation()
        HEFTExample.print_job_list(outputList0)
        Metrics.print_matrices(outputList0, vmlist0)
        # except:
        #     pass

if __name__ == "__main__":
    HEFTExample.main()
