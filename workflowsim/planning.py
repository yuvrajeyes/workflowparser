from __future__ import annotations
import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")

from typing import List, Dict, cast
from abc import ABC, abstractmethod
from cloudsim.DataCenter import Datacenter
from cloudsim.Consts import Consts
import cloudsim.Vm as Vm
from cloudsim.Log import Log

from workflowsim.Task import Task
from workflowsim.CustomVM import CustomVM
from workflowsim.utils.Parameters import FileType
from workflowsim.FileItem import FileItem



class BasePlanningAlgorithm(ABC):
    def __init__(self):
        self.taskList: List[Task] = []
        self.vmList: List[Vm.Vm] = []
        self.datacenterList: List[Datacenter] = []

    def set_task_list(self, taskList: List[Task]):
        self.taskList = taskList

    def set_vm_list(self, vmList: List[Vm.Vm]):
        self.vmList = list(vmList)

    def get_task_list(self) -> List[Task]:
        return self.taskList

    def get_vm_list(self) -> List[Vm.Vm]:
        return self.vmList

    def get_datacenter_list(self) -> List[Datacenter]:
        return self.datacenterList

    def set_datacenter_list(self, datacenterList: List[Datacenter]):
        self.datacenterList = list(datacenterList)

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError("Subclasses must implement the 'run' method")


def custom_sort_key(task_rank: HEFTPlanningAlgorithm.TaskRank):
    return task_rank.rank


class HEFTPlanningAlgorithm(BasePlanningAlgorithm):
    class Event:
        def __init__(self, start: float, finish: float):
            self.start: float = start
            self.finish: float = finish

    class TaskRank:
        def __init__(self, task: str, rank: float):
            self.task = task 
            self.rank = rank

    computationCosts: Dict[Task, Dict[CustomVM, float]] = dict()
    transferCosts: Dict[Task, Dict[Task, float]] = dict()
    averageBandwidth: float = 0.0
    rank: Dict[Task, float] = dict()
    earliestFinishTimes: Dict[Task, float] = dict()
    schedules : Dict[CustomVM, List[HEFTPlanningAlgorithm.Event]] = dict()


    def run(self) -> None:
        Log.print_line(f"HEFT planner running with {len(self.get_task_list())} tasks.")

        self.averageBandwidth = self.calculate_average_bandwidth()
        for vmObject in self.get_vm_list():
            vm: CustomVM = cast(CustomVM, vmObject)
            self.schedules[vm] = []

        # Prioritization phase
        self.calculate_computation_costs()
        self.calculate_transfer_costs()
        self.calculate_ranks()
        # Selection phase
        self.allocate_tasks()


    def calculate_average_bandwidth(self) -> float:
        avg = 0.0
        for vmObject in self.get_vm_list():
            vm: CustomVM = cast(CustomVM, vmObject)
            avg += vm.get_bw()
        return avg / len(self.get_vm_list())


    def calculate_computation_costs(self) -> None:
        for task in self.get_task_list():
            costsVm: Dict[CustomVM, float] = {}
            for vmObject in self.get_vm_list():
                vm: CustomVM = cast(CustomVM, vmObject)
                if vm.get_number_of_pes() < task.get_number_of_pes():
                    costsVm[vm] = float('inf')
                else:
                    costsVm[vm] = task.get_cloudlet_total_length() / vm.get_mips()
            HEFTPlanningAlgorithm.computationCosts[task] = costsVm

    
    def calculate_transfer_costs(self):
        # Initializing the matrix
        for task1 in self.get_task_list():
            task_transfer_costs = {}
            for task2 in self.get_task_list():
                task_transfer_costs[task2] = 0.0
            self.transferCosts[task1] = task_transfer_costs
        # Calculating the actual values
        for parent in self.get_task_list():
            for child in parent.get_childList():
                self.transferCosts[parent][child] = self.calculate_transfer_cost(parent, child)


    def calculate_transfer_cost(self, parent: Task, child: Task) -> float:
        parent_files: List[FileItem] = parent.get_fileList()
        child_files: List[FileItem] = child.get_fileList()
        acc: float = 0.0
        for parent_file in parent_files:
            if parent_file.get_type() != FileType.OUTPUT:
                continue
            for child_file in child_files:
                if child_file.get_type() == FileType.INPUT and child_file.get_name() == parent_file.get_name():
                    acc += child_file.get_size()
                    break
        # File size is in bytes, acc in MB
        acc /= Consts.MILLION
        # acc in MB, average_bandwidth in Mb/s
        return (acc * 8) / self.averageBandwidth


    def calculate_ranks(self):
        for task in self.get_task_list():
            self.calculate_rank(task)

    
    def calculate_rank(self, task: Task):
        if task in self.rank:
            return self.rank[task]
        average_computation_cost = 0.0
        for cost in self.computationCosts[task].values():
            average_computation_cost += cost
        average_computation_cost /= len(self.computationCosts[task])
        max_cost = 0.0
        for child in task.get_childList():
            child_cost = self.transferCosts[task][child] + self.calculate_rank(child)
            max_cost = max(max_cost, child_cost)
        self.rank[task] = average_computation_cost + max_cost
        return self.rank[task]
    

    def allocate_tasks(self) -> None:
        taskRank: List[HEFTPlanningAlgorithm.TaskRank] = []
        for task in self.rank.keys():
            taskRank.append(HEFTPlanningAlgorithm.TaskRank(task, self.rank[task]))
        # Sorting in non-ascending order of rank
        taskRank = sorted(taskRank, key=custom_sort_key, reverse=True)
        for rankItem in taskRank:
            self.allocate_task(rankItem.task)


    def allocate_task(self, task: 'Task'):
        chosen_vm: CustomVM = None
        earliest_finish_time: float = float('inf')
        best_ready_time: float = 0.0
        finish_time: float = 0
        for vmObject in self.get_vm_list():
            vm: CustomVM = cast(CustomVM, vmObject)
            min_ready_time: float = 0.0
            for parent in task.get_parentList():
                ready_time: float = self.earliestFinishTimes[parent]
                if parent.get_vm_id() != vm.get_id():
                    ready_time += self.transferCosts[parent][task]
                min_ready_time = max(min_ready_time, ready_time)

            finish_time = self.find_finish_time(task, vm, min_ready_time, False)
            if finish_time < earliest_finish_time:
                best_ready_time = min_ready_time
                earliest_finish_time = finish_time
                chosen_vm = vm
        self.find_finish_time(task, chosen_vm, best_ready_time, True) 
        self.earliestFinishTimes[task] = earliest_finish_time 
        task.set_vm_id(chosen_vm.get_id())


    def find_finish_time(self, task: 'Task', vm: CustomVM, ready_time: float, occupy_slot: bool) -> float:
        sched: List[HEFTPlanningAlgorithm.Event] = self.schedules[vm]
        computation_cost: float = self.computationCosts[task][vm]
        start: float = 0.0
        finish: float = 0.0
        pos: int = -1
        if not sched:
            if occupy_slot:
                sched.append(HEFTPlanningAlgorithm.Event(ready_time, ready_time + computation_cost))
            return ready_time + computation_cost
        if len(sched) == 1:
            if ready_time >= sched[0].finish:
                pos = 1
                start = ready_time
            elif ready_time + computation_cost <= sched[0].start:
                pos = 0
                start = ready_time
            else:
                pos = 1
                start = sched[0].finish

            if occupy_slot:
                sched.insert(pos, HEFTPlanningAlgorithm.Event(start, start + computation_cost))
            return start + computation_cost
        # Trivial case: Start after the latest task scheduled
        start = max(ready_time, sched[-1].finish)
        finish = start + computation_cost
        i = len(sched) - 1
        j = len(sched) - 2
        pos = i + 1
        while j >= 0:
            current: HEFTPlanningAlgorithm.Event = sched[i]
            previous: HEFTPlanningAlgorithm.Event = sched[j]
            if ready_time > previous.finish:
                if ready_time + computation_cost <= current.start:
                    start = ready_time
                    finish = ready_time + computation_cost
                break
            if previous.finish + computation_cost <= current.start:
                start = previous.finish
                finish = previous.finish + computation_cost
                pos = i
            i -= 1
            j -= 1
        if ready_time + computation_cost <= sched[0].start:
            pos = 0
            start = ready_time
            if occupy_slot:
                sched.insert(pos, HEFTPlanningAlgorithm.Event(start, start + computation_cost))
            return start + computation_cost
        if occupy_slot:
            sched.insert(pos, HEFTPlanningAlgorithm.Event(start, finish))
        return finish