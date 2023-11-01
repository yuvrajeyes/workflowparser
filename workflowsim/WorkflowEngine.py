from __future__ import annotations
import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")

from typing import List, Dict
from cloudsim.Cloudlet import Cloudlet
from cloudsim.core import SimEntity, SimEvent, CloudSimTags
import cloudsim.Vm as Vm
from cloudsim.Log import Log
from workflowsim.utils.Parameters import Parameters
from workflowsim.utils.OverheadParameters import OverheadParameters
from workflowsim.WorkflowScheduler import WorkflowScheduler
from workflowsim.Job import Job
from workflowsim.Task import Task
from workflowsim.ReclusteringEngine import ReclusteringEngine
from workflowsim.WorkflowSimTags import WorkflowSimTags


class WorkflowEngine(SimEntity):
    def __init__(self, name: str, schedulers: int = 1):
        super().__init__(name)
        self.jobsList: List[Cloudlet] = []
        self.jobsSubmittedList: List[Cloudlet] = [] 
        self.jobsReceivedList: List[Cloudlet] = []
        self.jobsSubmitted = 0
        self.vmList: List[Vm.Vm] = []
        self.schedulerId: List[int] = []
        self.scheduler: List[WorkflowScheduler] = []

        for i in range(schedulers):
            wfs: WorkflowScheduler = WorkflowScheduler(f"{name}_Scheduler_{i}")
            self.scheduler.append(wfs)
            self.schedulerId.append(wfs.id)
            wfs.workflowEngineId = self.id


    def submit_vm_list(self, vmList: List[Vm.Vm], schedulerId: int=None):
        if schedulerId is not None:
            self.scheduler[0].submit_vm_list(vmList)
            self.vmList = vmList
        else:
            self.get_scheduler(schedulerId).submit_vm_list(vmList)


    def get_all_vm_list(self) -> List[Vm.Vm]:
        if self.vmList and len(self.vmList) > 0:
            return self.vmList
        else:
            vmList: List = []
            for i in range(len(self.scheduler)):
                vmList.extend(self.get_scheduler(i).get_vm_list())
            return vmList
        

    def submit_cloudlet_list(self, cloudlet_list: List[Cloudlet]):
        self.jobsList.extend(cloudlet_list)


    def process_event(self, ev):
        tag: int = ev.get_tag()
        # Resource characteristics request
        if (tag==CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST):
            self.process_resource_characteristics_request(ev)
        # this call is from workflow scheduler when all vms are created
        elif (tag==CloudSimTags.CLOUDLET_SUBMIT):
            self.submit_jobs()
        elif (tag==CloudSimTags.CLOUDLET_RETURN):
            self.process_job_return(ev)
        elif (tag==CloudSimTags.END_OF_SIMULATION):
            self.shutdown_entity()
        elif (tag==WorkflowSimTags.JOB_SUBMIT):
            self.process_job_submit(ev)
        else:
            self.process_other_event(ev)


    def process_resource_characteristics_request(self, ev: SimEvent) -> None:
        for i in range(len(self.schedulerId)):
            self.schedule(self.schedulerId[i], 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST)

    def bind_scheduler_datacenter(self, datacenterId: int, schedulerId: int=0) -> None:
        self.get_scheduler(schedulerId).bind_scheduler_datacenter(datacenterId)


    def process_job_submit(self, ev: SimEvent) -> None:
        jobList: List[Cloudlet] = ev.get_data()
        self.set_jobs_list(jobList)


    def process_job_return(self, ev: SimEvent):
        job: Job = ev.get_data()
        if job.get_cloudlet_status() == Cloudlet.FAILED:
            newId: int = len(self.jobsList) + len(self.jobsSubmittedList)
            self.jobsList.extend(ReclusteringEngine.process(job, newId))
        self.jobsReceivedList.append(job)
        self.jobsSubmitted -= 1
        if len(self.jobsList)==0 and self.jobsSubmitted==0:
            for i in range(len(self.get_scheduler_ids())):
                self.send_now(self.schedulerId[i], CloudSimTags.END_OF_SIMULATION, None)
        else:
            self.send_now(self.id, CloudSimTags.CLOUDLET_SUBMIT, None)


    def process_other_event(self, ev: SimEvent):
        if ev is None:
            Log.print_line(f"{self.name}.process_other_event(): Error - an event is null")
            return
        Log.print_line(f"{self.name}.process_other_event(): Error - event unknown by this DatacenterBroker.")


    def has_job_list_contains_id(self, jobList: List[Job], cloudlet_id: int):
        for job in jobList:
            if job.get_cloudlet_id() == cloudlet_id:
                return True
        return False
    

    def submit_jobs(self):
        jobList: List[Job] = self.get_jobs_list()
        allocationList: Dict[int, List[Job]] = {}
        for i in range(len(self.scheduler)):
            submittedList: List[Job] = []
            allocationList[self.schedulerId[i]] = submittedList
        num = len(jobList)
        i = 0
        while i < num:
            job: Job = jobList[i]
            if not self.has_job_list_contains_id(self.jobsReceivedList, job.get_cloudlet_id()):
                parentList: List[Job] = job.get_parentList()
                flag: bool = True
                for parent in parentList:
                    if not self.has_job_list_contains_id(self.jobsReceivedList, parent.get_cloudlet_id()):
                        flag = False
                        break
                if flag:
                    submittedList: List[Job] = allocationList[job.get_user_id()]
                    submittedList.append(job)
                    self.jobsSubmitted += 1
                    self.jobsSubmittedList.append(job)
                    jobList.remove(job)
                    i -= 1
                    num -= 1
            i += 1
        for i in range(len(self.scheduler)):
            submittedList: List = allocationList[self.schedulerId[i]]
            op: OverheadParameters = Parameters.getOverheadParams()
            interval: int = op.get_wed_interval()
            delay: float = 0.0
            if op.get_wed_delay() is not None:
                delay = op.get_wed_delay(submittedList)
            delay_base: float = delay
            size: int = len(submittedList)
            if interval > 0 and interval <= size:
                index: int = 0
                sub_list: List = []
                while index < size:
                    sub_list.append(submittedList[index])
                    index += 1
                    if index % interval == 0:
                        self.schedule(self.schedulerId[i], delay, CloudSimTags.CLOUDLET_SUBMIT, sub_list)
                        delay += delay_base
                        sub_list = []
                if len(sub_list)!=0:
                    self.schedule(self.schedulerId[i], delay, CloudSimTags.CLOUDLET_SUBMIT, sub_list)
            elif len(submittedList)!=0:
                self.send_now(self.schedulerId[i], CloudSimTags.CLOUDLET_SUBMIT, submittedList)


    def shutdown_entity(self):
        Log.print_line(f"{self.name} is shutting down...")


    def start_entity(self):
        Log.print_line(f"{self.name} is starting...")
        self.schedule(self.id, 0, CloudSimTags.RESOURCE_CHARACTERISTICS_REQUEST)


    def get_jobs_list(self) -> list:
        return self.jobsList


    def set_jobs_list(self, jobsList: list):
        self.jobsList = jobsList


    def get_jobs_submitted_list(self) -> list:
        return self.jobsSubmittedList


    def set_jobs_submitted_list(self, jobsSubmittedList: list):
        self.jobsSubmittedList = jobsSubmittedList


    def get_jobs_received_list(self) -> list:
        return self.jobsReceivedList


    def set_jobs_received_list(self, jobsReceivedList: list) -> None:
        self.jobsReceivedList = jobsReceivedList


    def get_vm_list(self) -> list:
        return self.vmList


    def set_vm_list(self, vmList: list) -> None:
        self.vmList = vmList


    def set_schedulers(self, lst: List[int]) -> None:
        self.schedulerId = lst


    def get_schedulers(self) -> List[WorkflowScheduler]:
        return self.scheduler


    def set_scheduler_ids(self, lst: List[int]) -> None:
        self.schedulerId = lst


    def get_scheduler_ids(self) -> List[int]:
        return self.schedulerId


    def get_scheduler_id(self, index: int) -> int:
        if self.schedulerId:
            return self.schedulerId[index]
        return 0
    

    def get_scheduler(self, schedulerId: int) -> WorkflowScheduler:
        if self.scheduler:
            return self.scheduler[schedulerId]
        return None