from __future__ import annotations
import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")


from typing import List
from cloudsim.Log import Log
from cloudsim.core import SimEntity, SimEvent, CloudSimTags
from workflowsim.Task import Task
from workflowsim.planning import BasePlanningAlgorithm, HEFTPlanningAlgorithm
from workflowsim.utils.Parameters import Parameters
from workflowsim.utils.Parameters import PlanningAlgorithm
from workflowsim.WorkflowParser import WorkflowParser
from workflowsim.WorkflowEngine import WorkflowEngine
from workflowsim.WorkflowSimTags import WorkflowSimTags
from workflowsim.ClusteringEngine import ClusteringEngine


class WorkflowPlanner(SimEntity):
    def __init__(self, name, schedulers: int=1):
        super().__init__(name)
        self.taskList: List[Task] = []
        self.clusteringEngine: ClusteringEngine = ClusteringEngine(name + "_Merger_", schedulers)
        self.clusteringEngineId: int = self.clusteringEngine.get_id()
        self.parser = WorkflowParser(self.clusteringEngine.get_workflow_engine().get_scheduler_id(0))


    def get_clustering_engine_id(self) -> int:
        return self.clusteringEngineId


    def get_clustering_engine(self) -> ClusteringEngine:
        return self.clusteringEngine


    def get_workflow_parser(self) -> WorkflowParser:
        return self.parser


    def get_workflow_engine_id(self) -> int:
        return self.clusteringEngine.get_workflow_engine_id()


    def get_workflow_engine(self) -> WorkflowEngine:
        return self.clusteringEngine.get_workflow_engine()


    def process_event(self, ev: SimEvent) -> None:
        tag = ev.get_tag()
        if tag == WorkflowSimTags.START_SIMULATION:
            self.get_workflow_parser().parse()
            self.set_task_list(self.get_workflow_parser().get_taskList())
            self.process_planning()
            self.process_impact_factors(self.get_task_list())
            self.send_now(self.get_clustering_engine_id(), WorkflowSimTags.JOB_SUBMIT, self.get_task_list())
        elif tag == CloudSimTags.END_OF_SIMULATION:
            self.shutdown_entity()
        else:
            self.process_other_event(ev)


    def process_planning(self) -> None:
        if Parameters.getPlanningAlgorithm() == PlanningAlgorithm.INVALID:
            return
        planner: BasePlanningAlgorithm = self.get_planning_algorithm(Parameters.getPlanningAlgorithm())
        planner.set_task_list(self.get_task_list())
        planner.set_vm_list(self.get_workflow_engine().get_all_vm_list())
        planner.run()


    def get_planning_algorithm(self, name: PlanningAlgorithm) -> BasePlanningAlgorithm:
        planner: BasePlanningAlgorithm = None
        if name == PlanningAlgorithm.INVALID:
            planner = None
        elif name == PlanningAlgorithm.HEFT:
            planner = HEFTPlanningAlgorithm()
        return planner


    def process_impact_factors(self, taskList: List[Task]) -> None:
        exits: List[Task] = []
        for task in taskList:
            if len(task.get_childList()) == 0:
                exits.append(task)
        avg: float = 1.0 / len(exits)
        for task in exits:
            self.add_impact(task, avg)


    def add_impact(self, task: Task, impact) -> None:
        task.set_impact(task.get_impact() + impact)
        size: int = len(task.get_parentList())
        if size > 0:
            avg = impact / size
            for parent in task.get_parentList():
                self.add_impact(parent, avg)


    def process_other_event(self, ev: SimEvent) -> None:
        if (ev is None):
            Log.print_line(f"{self.get_name()}.process_other_event(): Error - an event is null.")
            return
        Log.print_line(f"{self.get_name()}.process_other_event(): Error - event unknown by this DatacenterBroker.")


    def finish_execution(self) -> None:
        pass


    def shutdown_entity(self) -> None:
        Log.print_line(self.get_name() + " is shutting down...")


    def start_entity(self) -> None:
        Log.print_line(f"Starting WorkflowSim {Parameters.getVersion()}")
        Log.print_line(self.get_name() + " is starting...")
        self.schedule(self.get_id(), 0, WorkflowSimTags.START_SIMULATION)


    def get_task_list(self) -> List[Task]:
        return self.taskList


    def set_task_list(self, taskList) -> None:
        self.taskList = taskList