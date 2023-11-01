from __future__ import annotations

from typing import List, cast
from cloudsim.core import CloudSimTags, SimEntity, SimEvent
from cloudsim.Log import Log
from workflowsim.clustering import BasicClustering
from workflowsim.utils.Parameters import Parameters, ClusteringParameters, ClassType
from workflowsim.utils.ReplicaCatalog import ReplicaCatalog
from workflowsim.Task import Task
from workflowsim.Job import Job
from workflowsim.WorkflowEngine import WorkflowEngine
from workflowsim.FileItem import FileItem
from workflowsim.WorkflowSimTags import WorkflowSimTags


class ClusteringEngine(SimEntity):
    def __init__(self, name: str, schedulers: int) -> None:
        super().__init__(name)
        self.jobList: List[Job] = []
        self.taskList: List[Task] = []
        self.taskSubmittedList: List[Task] = []
        self.taskReceivedList: List[Task] = []
        self.cloudletsSubmitted: int = 0
        self.engine: BasicClustering = BasicClustering()
        self.workflowEngine: WorkflowEngine = WorkflowEngine(f"{name}_Engine_0", schedulers)
        self.workflowEngineId: int = self.workflowEngine.id


    def get_workflow_engine_id(self) -> int:
        return self.workflowEngineId
    

    def get_workflow_engine(self) -> WorkflowEngine:
        return self.workflowEngine

    def submit_task_list(self, taskList: List[Task]) -> None:
        self.taskList.extend(taskList)


    def process_clustering(self) -> None:
        self.engine.set_taskList(self.taskList)
        self.engine.run()
        self.set_job_list(self.engine.jobList)


    def process_data_staging(self) -> None:
        lst: List[FileItem] = self.engine.get_task_files()
        job: Job = Job(len(self.jobList), 110)
        file_list: List[FileItem] = []
        for file in lst:
            if file.is_real_input_file(lst):
                ReplicaCatalog.add_file_to_storage(file.name, Parameters.SOURCE)
                file_list.append(file)
        job.set_fileList(file_list)
        job.set_class_type(ClassType.STAGE_IN)
        job.set_depth(0)
        job.set_priority(0)
        job.set_user_id(self.workflowEngine.get_scheduler_id(0))
        for c_job in self.get_job_list():
            if len(c_job.get_parent_list())==0:
                c_job.add_parent(job)
                job.add_child(c_job)
        self.get_job_list().append(job)


    def process_event(self, ev: SimEvent) -> None:
        tag: int = ev.get_tag()
        if tag == WorkflowSimTags.START_SIMULATION:
            return
        elif tag == WorkflowSimTags.JOB_SUBMIT:
            taskList: List[Task] = cast(List[Task], ev.get_data())
            self.set_task_list(taskList)
            self.process_clustering()
            self.process_data_staging()
            self.send_now(self.workflowEngineId, WorkflowSimTags.JOB_SUBMIT, self.jobList)
        elif tag == CloudSimTags.END_OF_SIMULATION:
            self.shutdown_entity()
        else:
            self.process_other_event(ev)


    def process_other_event(self, ev: SimEvent) -> None:
        if ev is None:
            Log.print_line(f"{self.get_name()}.process_other_event(): Error - an event is null.")
            return
        Log.print_line(f"{self.get_name()}.process_other_event(): Error - event unknown by this DatacenterBroker.")


    def finish_execution(self) -> None:
        pass

    def shutdown_entity(self) -> None:
        Log.print_line(f"{self.get_name()} is shutting down...")

    def start_entity(self) -> None:
        Log.print_line(f"{self.get_name()} is starting...")
        self.schedule(self.get_id(), 0, WorkflowSimTags.START_SIMULATION)


    def get_task_list(self) -> List[Task]:
        return cast(List[Task], self.taskList)


    def get_job_list(self) -> List[Job]:
        return self.jobList


    def set_task_list(self, taskList: List[Task]) -> None:
        self.taskList = taskList


    def set_job_list(self, jobList: List[Job]) -> None:
        self.jobList = jobList


    def get_task_submitted_list(self) -> List[Task]:
        return cast(List[Task], self.taskSubmittedList)


    def set_task_submitted_list(self, taskSubmittedList: List[Task]) -> None:
        self.taskSubmittedList = taskSubmittedList


    def get_task_received_list(self) -> List[Task]:
        return cast(List[Task], self.taskReceivedList)


    def set_task_received_list(self, taskReceivedList: List[Task]) -> None:
        self.taskReceivedList = taskReceivedList