from __future__ import annotations

import numpy as np
from scipy.stats import lognorm, weibull_min, gamma, norm
from typing import List, Dict, Union, Final
from enum import Enum, auto
from cloudsim.Cloudlet import Cloudlet
from cloudsim.Log import Log
from workflowsim.utils.DistributionGenerator import DistributionGenerator
from workflowsim.Task import Task
from workflowsim.Job import Job


class FailureRecord:
    def __init__(self, length: float, failedTasksNum: int, depth: int, allTaskNum: int, vmId: int, jobId: int, workflowId: int, delayLength: float):
        self.length: float = length
        self.failedTasksNum: int = failedTasksNum
        self.depth: int= depth
        self.allTaskNum: int = allTaskNum
        self.vmId: int = vmId
        self.jobId: int = jobId
        self.workflowId: int = workflowId
        self.delayLength: float = delayLength

    def __str__(self):
        return f"FailureRecord: length={self.length}, failedTasksNum={self.failedTasksNum}, \
            depth={self.depth}, allTaskNum={self.allTaskNum}, vmId={self.vmId}, jobId={self.jobId}, \
            workflowId={self.workflowId}, delayLength={self.delayLength}"


class FailureMonitor:
    vm2record: Dict[int, List[FailureRecord]]
    type2record: Dict[int, List[FailureRecord]]
    jobid2record: Dict[int, FailureRecord]
    recordList: List[FailureRecord]
    index2job: Dict

    @staticmethod
    def init():
        FailureMonitor.vm2record = {}
        FailureMonitor.type2record = {}
        FailureMonitor.jobid2record = {}
        FailureMonitor.recordList = []

    @staticmethod
    def get_K(d: float, a: float, t: float) -> float:
        return (-d + (d * d - 4 * d / (1 - a)) ** 0.5) / (2 * t)

    @staticmethod
    def get_clustering_factor(record: FailureRecord) -> int:
        d: float = record.delayLength
        t: float = record.length
        a: float = 0.0
        if FailureParameters.get_monitor_mode() == FailureParameters.FTCMonitor.MONITOR_JOB:
            a = FailureMonitor.analyze(record.depth)
        elif FailureParameters.get_monitor_mode() == FailureParameters.FTCMonitor.MONITOR_ALL:
            a = FailureMonitor.analyze(record.depth)

        if a <= 0.0:
            return record.allTaskNum
        else:
            k = FailureMonitor.get_K(d, a, t)
            if k <= 1:
                k = 1  # minimal
            return int(k)

    @staticmethod
    def post_failure_record(record: FailureRecord):
        if record.workflowId < 0 or record.jobId < 0 or record.vmId < 0:
            Log.print_line("Error in receiving failure record")
            return

        if FailureParameters.get_monitor_mode() == FailureParameters.FTCMonitor.MONITOR_VM:
            if record.vmId not in FailureMonitor.vm2record:
                FailureMonitor.vm2record[record.vmId] = []
            FailureMonitor.vm2record[record.vmId].append(record)
        elif FailureParameters.get_monitor_mode() == FailureParameters.FTCMonitor.MONITOR_JOB:
            if record.depth not in FailureMonitor.type2record:
                FailureMonitor.type2record[record.depth] = []
            FailureMonitor.type2record[record.depth].append(record)
        elif FailureParameters.get_monitor_mode() == FailureParameters.FTCMonitor.MONITOR_NONE:
            pass

        FailureMonitor.recordList.append(record)

    @staticmethod
    def analyze(type: int) -> float:
        sumFailures = 0
        sumJobs = 0

        if FailureParameters.get_monitor_mode() == FailureParameters.FTCMonitor.MONITOR_ALL:
            for record in FailureMonitor.recordList:
                sumFailures += record.failedTasksNum
                sumJobs += record.allTaskNum
        elif FailureParameters.get_monitor_mode() == FailureParameters.FTCMonitor.MONITOR_JOB:
            if type in FailureMonitor.type2record:
                for record in FailureMonitor.type2record[type]:
                    sumFailures += record.failedTasksNum
                    sumJobs += record.allTaskNum
        elif FailureParameters.get_monitor_mode() == FailureParameters.FTCMonitor.MONITOR_VM:
            if type in FailureMonitor.vm2record:
                for record in FailureMonitor.vm2record[type]:
                    sumFailures += record.failedTasksNum
                    sumJobs += record.allTaskNum

        if sumFailures == 0:
            return 0
        alpha = float(sumFailures) / float(sumJobs)
        return alpha



class FailureParameters:
    class FTCluteringAlgorithm(Enum):
        FTCLUSTERING_DC = auto()
        FTCLUSTERING_SR = auto()
        FTCLUSTERING_DR = auto()
        FTCLUSTERING_NOOP = auto()
        FTCLUSTERING_BLOCK = auto()
        FTCLUSTERING_VERTICAL = auto()


    class FTCMonitor(Enum):
        MONITOR_NONE = auto()
        MONITOR_ALL = auto()
        MONITOR_VM = auto()
        MONITOR_JOB = auto()
        MONITOR_VM_JOB = auto()


    class FTCFailure(Enum):
        FAILURE_NONE = auto()
        FAILURE_ALL = auto()
        FAILURE_VM = auto()
        FAILURE_JOB = auto()
        FAILURE_VM_JOB = auto()

    generators: List[List[DistributionGenerator]] = []
    FTClusteringAlgorithm: FTCluteringAlgorithm = FTCluteringAlgorithm.FTCLUSTERING_NOOP
    monitorMode: FTCMonitor = FTCMonitor.MONITOR_NONE
    failureMode: FTCFailure = FTCFailure.FAILURE_NONE
    distribution: DistributionGenerator.DistributionFamily = DistributionGenerator.DistributionFamily.WEIBULL
    INVALID: int = -1

    @staticmethod
    def init(fMethod: FTCluteringAlgorithm, monitor: FTCMonitor, failure: FTCFailure,
        failureGenerators: List[List[DistributionGenerator]], dist: DistributionGenerator.DistributionFamily=DistributionGenerator.DistributionFamily.WEIBULL) -> None:
        FailureParameters.FTClusteringAlgorithm = fMethod
        FailureParameters.monitorMode = monitor
        FailureParameters.failureMode = failure
        FailureParameters.generators = failureGenerators
        FailureParameters.distribution = dist

    @staticmethod
    def get_failure_generators() -> List[List[DistributionGenerator]]:
        if not FailureParameters.generators:
            Log.print_line("ERROR: alpha is not initialized")
        return FailureParameters.generators

    @staticmethod
    def get_failure_generators_max_first_index() -> int:
        if not FailureParameters.generators:
            Log.print_line("ERROR: alpha is not initialized")
            return -1
        return len(FailureParameters.generators)

    @staticmethod
    def get_failure_generators_max_second_index() -> int:
        # Test whether it is valid
        FailureParameters.get_failure_generators_max_first_index()
        if not FailureParameters.generators or not FailureParameters.generators[0]:
            Log.print_line("ERROR: alpha is not initialized")
            return -1
        return len(FailureParameters.generators[0])

    @staticmethod
    def get_generator(vmIndex: int, taskDepth: int) -> DistributionGenerator:
        return FailureParameters.generators[vmIndex][taskDepth]

    @staticmethod
    def get_failure_generator_mode() -> FTCFailure:
        return FailureParameters.failureMode

    @staticmethod
    def get_monitor_mode() -> FTCMonitor:
        return FailureParameters.monitorMode

    @staticmethod
    def get_ft_clustering_algorithm() -> FTCluteringAlgorithm:
        return FailureParameters.FTClusteringAlgorithm

    @staticmethod
    def get_failure_distribution() -> DistributionGenerator.DistributionFamily:
        return FailureParameters.distribution


class FailureGenerator:
    maxFailureSizeExtension: Final[int] = 50
    failureSizeExtension: int = 0
    hasChangeTime: Final[bool] = False

    @staticmethod
    def get_distribution(alpha: float, beta: float) -> Union[lognorm, weibull_min, gamma, norm, None]:
        distribution = None
        failureDistribution = FailureParameters.get_failure_distribution()
        if failureDistribution == DistributionGenerator.DistributionFamily.LOGNORMAL:
            distribution = lognorm(beta, scale=np.exp(-1.0 / alpha))
        elif failureDistribution == DistributionGenerator.DistributionFamily.WEIBULL:
            distribution = weibull_min(alpha, scale=beta)
        elif failureDistribution == DistributionGenerator.DistributionFamily.GAMMA:
            distribution = gamma(alpha, scale=1.0 / beta)
        elif failureDistribution == DistributionGenerator.DistributionFamily.NORMAL:
            distribution = norm(loc=1.0 / alpha, scale=beta)
        return distribution

    @staticmethod
    def init_failure_samples():
        pass

    @staticmethod
    def init():
        FailureGenerator.init_failure_samples()

    @staticmethod
    def check_failure_status(task: Task, vmId: int):
        generator: DistributionGenerator = None
        failureGeneratorMode = FailureParameters.get_failure_generator_mode()
        if failureGeneratorMode == FailureParameters.FTCFailure.FAILURE_ALL:
            generator = FailureParameters.get_generator(0, 0)
        elif failureGeneratorMode == FailureParameters.FTCFailure.FAILURE_JOB:
            generator = FailureParameters.get_generator(0, task.get_depth())
        elif failureGeneratorMode == FailureParameters.FTCFailure.FAILURE_VM:
            generator = FailureParameters.get_generator(vmId, 0)
        elif failureGeneratorMode == FailureParameters.FTCFailure.FAILURE_VM_JOB:
            generator = FailureParameters.get_generator(vmId, task.get_depth())
        else:
            return False

        start = task.get_exec_start_time()
        end = task.get_taskFinishTime()

        samples = generator.get_cumulative_samples()

        while samples[-1] < start:
            generator.extend_samples()
            samples = generator.get_cumulative_samples()
            FailureGenerator.failureSizeExtension += 1
            if FailureGenerator.failureSizeExtension >= FailureGenerator.maxFailureSizeExtension:
                raise Exception("Error rate is too high, and the simulator terminates")

        for sampleId in range(len(samples)):
            if end < samples[sampleId]:
                return False  # No failure
            if start <= samples[sampleId]:
                generator.get_next_sample()
                return True  # Has a failure

        return False

    @staticmethod
    def generate(job: Job):
        jobFailed = False
        if FailureParameters.get_failure_generator_mode() == FailureParameters.FTCFailure.FAILURE_NONE:
            return jobFailed

        try:
            for task in job.get_task_list():
                failedTaskSum = 0
                if FailureGenerator.check_failure_status(task, job.get_vm_id()):
                    jobFailed = True
                    failedTaskSum += 1
                    task.set_cloudlet_status(Cloudlet.FAILED)

                record = FailureRecord(0, failedTaskSum, task.get_depth(), 1, job.get_vm_id(), task.get_cloudlet_id(), job.get_user_id())
                FailureMonitor.post_failure_record(record)

            if jobFailed:
                job.set_cloudlet_status(Cloudlet.FAILED)
            else:
                job.set_cloudlet_status(Cloudlet.SUCCESS)
        except Exception as e:
            Log.print_line(str(e))

        return jobFailed
