from __future__ import annotations
import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")

import math
from typing import List, Dict, cast
from cloudsim.Cloudlet import Cloudlet
from cloudsim.Log import Log
from workflowsim.Job import Job
from workflowsim.Task import Task
from workflowsim.failure import FailureMonitor, FailureRecord, FailureParameters
from workflowsim.utils.Parameters import Parameters
from workflowsim.utils.OverheadParameters import OverheadParameters


class ClusteringSizeEstimator:
    # return the makespan
    @staticmethod
    def f(clustering_size: float, task_runtime: float, system_overhead: float, theta: float, phi_gamma: float, phi_ts: float) -> float:
        d: float = (clustering_size * task_runtime + system_overhead) * (phi_ts - 1)
        return d/clustering_size * math.exp(math.pow(d/theta, phi_gamma))
    

    # return the prime of makespan
    @staticmethod
    def fprime(clustering_size: float, task_runtime: float, system_overhead: float, theta: float, phi: float) -> float:
        d: float = math.pow((clustering_size * task_runtime + system_overhead) / theta, phi)
        return math.exp(d) + task_runtime * phi / clustering_size * d - system_overhead / (clustering_size**2)


    # return the optimal K
    @staticmethod
    def estimateK(task_runtime: float, system_overhead: float, theta: float, phi_gamma: float, phi_ts: float) -> int:
        optimalK: int = 0
        minM: float = float("inf")
        for k in range(1, 200):
            M: float = ClusteringSizeEstimator.f(k, task_runtime, system_overhead, theta, phi_gamma, phi_ts)
            if (M < minM):
                minM = M
                optimalK = k
        return optimalK
    


class ReclusteringEngine:
    @staticmethod
    def create_job(id: int, job: Job, length: int, taskList: List, updateDep: bool) -> Job:
        newJob: Job = Job(id, length)
        newJob.set_user_id(job.get_user_id())
        newJob.set_vm_id(-1)
        newJob.set_cloudlet_status(Cloudlet.CREATED)
        newJob.set_task_list(taskList)
        newJob.set_depth(job.get_depth())
        if (updateDep):
            newJob.set_childList(job.get_childList())
            newJob.set_parentList(job.get_parent_list())
            for cJob in job.get_childList():
                cJob.add_parent(newJob)
        return newJob

    
    @staticmethod
    def process(job: Job, id: int) -> List[Job]:
        jobList: List = list()
        algorithm = FailureParameters.get_ft_clustering_algorithm()
        if algorithm == FailureParameters.get_ft_clustering_algorithm().FTCLUSTERING_NOOP:
            jobList.append(ReclusteringEngine.create_job(id, job, job.get_cloudlet_length(), job.get_task_list(), True))
        # Dynamic clustering.
        elif algorithm in [FailureParameters.get_ft_clustering_algorithm().FTCLUSTERING_DC, FailureParameters.get_ft_clustering_algorithm().FTCLUSTERING_DR]:
            jobList = ReclusteringEngine.dc_reclustering(jobList, job, id, job.get_task_list())
        # Selective reclustering
        elif algorithm == FailureParameters.get_ft_clustering_algorithm().FTCLUSTERING_SR:
            jobList = ReclusteringEngine.sr_reclustering(jobList, job, id)
        # Block reclustering
        elif algorithm == FailureParameters.get_ft_clustering_algorithm().FTCLUSTERING_BLOCK:
            jobList = ReclusteringEngine.block_reclustering(jobList, job, id)
        # Binary reclustering
        elif algorithm == FailureParameters.get_ft_clustering_algorithm().FTCLUSTERING_VERTICAL:
            jobList = ReclusteringEngine.vertical_reclustering(jobList, job, id)
        else:
            pass
        return jobList
    
    
    @staticmethod
    def get_depth_map(taskList: List[Task]) -> Dict[int, List[Task]]:
        map: Dict[int, List[Task]] = {}
        for task in taskList:
            depth: int = task.get_depth()
            if depth not in map:
                map[depth] = []
            dl: List[Task] = map[depth]
            if task not in dl:
                dl.append(task)
        return map
    

    @staticmethod
    def get_min(map: Dict[int, List[Task]]) -> int:
        if map:
            min_value: int = min(map.keys())
            return min_value
        return -1
    
    
    @staticmethod
    def check_failed(taskList: List[Task]) -> bool:
        for task in taskList:
            if task.get_cloudlet_status() == Cloudlet.FAILED:
                return True
        return False
    

    @staticmethod
    def vertical_reclustering(jobList: List[Job], job: Job, id: int) -> List[Job]:
        map: Dict[int, List[Task]] = ReclusteringEngine.get_depth_map(job.get_task_list())
        if len(map) == 1:
            jobList = ReclusteringEngine.dc_reclustering(jobList, job, id, job.get_task_list())
            return jobList
        min_depth: int = ReclusteringEngine.get_min(map)
        max_depth: int = min_depth + len(map) - 1
        mid_depth: int = (min_depth + max_depth) // 2
        listUp: List = []
        listDown: List = []
        for i in range(min_depth, min_depth + len(map)):
            taskList: List[Task] = map.get(i, [])
            if i <= mid_depth:
                listUp.extend(taskList)
            else:
                listDown.extend(taskList)
        newUpList: List[Job] = ReclusteringEngine.dc_reclustering([], job, id, listUp)
        id += len(newUpList)
        jobList.extend(newUpList)
        jobList.extend(ReclusteringEngine.dc_reclustering([], job, id, listDown))
        return jobList
    
    
    @staticmethod
    def block_reclustering(jobList: List[Job], job: Job, id: int) -> List[Job]:
        map: Dict = ReclusteringEngine.get_depth_map(job.get_task_list())
        if len(map) == 1:
            jobList = ReclusteringEngine.dr_reclustering(jobList, job, id, job.get_task_list())
            return jobList
        min_depth: int = ReclusteringEngine.get_min(map)
        for i in range(min_depth, min_depth + len(map)):
            taskList: List = cast(List, map.get(i))
            if ReclusteringEngine.check_failed(taskList):
                new_list: List = ReclusteringEngine.dr_reclustering([], job, id, taskList)
                id += len(new_list)
                jobList.extend(new_list)
        return jobList
    
    
    @staticmethod
    def dc_reclustering(jobList: List[Job], job: Job, id: int, allTaskList: List[Task]) -> List[Job]:
        firstTask: Task = allTaskList[0]
        taskLength: float = float(firstTask.get_cloudlet_length()) / 1000  # Replace with actual conversion factor
        record: FailureRecord = FailureRecord(taskLength, 0, job.get_depth(), len(allTaskList), 0, 0, job.get_user_id())
        record.delayLength = ReclusteringEngine.get_cumulative_delay(job.depth)
        suggestedK: int = FailureMonitor.get_clustering_factor(record)
        if suggestedK == 0:
            jobList.append(ReclusteringEngine.create_job(id, job, job.get_cloudlet_length(), allTaskList, True))
        else:
            actualK: int = 0
            taskList: List[Task] = []
            retryJobs: List[Job] = []
            length: int = 0
            newJob: Job = ReclusteringEngine.create_job(id, job, 0, None, False)
            for task in allTaskList:
                if actualK < suggestedK:
                    actualK += 1
                    taskList.append(task)
                    length += task.get_cloudlet_length()
                else:
                    newJob.set_task_list(taskList)
                    taskList: List[Task] = []
                    newJob.set_cloudlet_length(length)
                    length = 0
                    retryJobs.append(newJob)
                    id += 1
                    newJob = ReclusteringEngine.create_job(id, job, 0, None, False)
                    actualK = 0
            if taskList:
                newJob.set_task_list(taskList)
                newJob.set_cloudlet_length(length)
                retryJobs.append(newJob)
            ReclusteringEngine.update_dependencies(job, retryJobs)
            jobList.extend(retryJobs)
        return jobList
    
    
    @staticmethod
    def sr_reclustering(jobList: List[Job], job: Job, id: int) -> List[Job]:
        newTaskList: List[Task] = []
        length: int = 0
        for task in job.get_task_list():
            if task.get_cloudlet_status() == Cloudlet.FAILED:
                newTaskList.append(task)
                length += task.get_cloudlet_length()
        jobList.append(ReclusteringEngine.create_job(id, job, length, newTaskList, True))
        return jobList
    

    @staticmethod
    def get_dividend(depth: int) -> int:
        dividend: int = 1
        if depth == 1:
            dividend = 78
        elif depth == 2:
            dividend = 229
        elif depth == 5:
            dividend = 64
        else:
            Log.print_line("Error")
        return dividend
    
    
    @staticmethod
    def get_cumulative_delay(depth: int) -> float:
        delay: float = 0.0
        op: OverheadParameters = Parameters.getOverheadParams()
        if op.get_queue_delay() and depth in op.get_queue_delay():
            delay += op.get_queue_delay()[depth].get_mle_mean()
        if op.get_wed_delay() and depth in op.get_wed_delay():
            delay += op.get_wed_delay()[depth].get_mle_mean()
        if op.get_post_delay() and depth in op.get_post_delay():
            delay += op.get_post_delay()[depth].get_mle_mean()
        return delay
    

    @staticmethod
    def get_overhead_likelihood_prior(depth: int) -> float:
        prior: float = 0.0
        op: OverheadParameters = Parameters.getOverheadParams()
        if op.get_queue_delay() and depth in op.get_queue_delay():
            prior = op.get_queue_delay()[depth].get_likelihood_prior()
        elif op.get_wed_delay() and depth in op.get_wed_delay():
            prior = op.get_wed_delay()[depth].get_mle_mean()
        elif op.get_post_delay() and depth in op.get_post_delay():
            prior = op.get_post_delay()[depth].get_mle_mean()
        return prior
    
    
    @staticmethod
    def dr_reclustering(jobList: List[Job], job: Job, id: int, allTaskList: List[Task]) -> List:
        pass


    @staticmethod
    def update_dependencies(job: Job, jobList: List[Job]) -> None:
        pass
