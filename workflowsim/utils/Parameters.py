import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")

from typing import List, Union
from enum import Enum
from workflowsim.utils.ClusteringParameters import ClusteringParameters
from workflowsim.utils.OverheadParameters import OverheadParameters


class SchedulingAlgorithm:
    MAXMIN = 'MAXMIN'
    MINMIN = 'MINMIN'
    MCT = 'MCT'
    DATA = 'DATA'
    STATIC = 'STATIC'
    FCFS = 'FCFS'
    ROUNDROBIN = 'ROUNDROBIN'
    INVALID = 'INVALID'

class PlanningAlgorithm:
    INVALID = 'INVALID'
    RANDOM = 'RANDOM'
    HEFT = 'HEFT'
    DHEFT = 'DHEFT'

class FileType:
    NONE = 0
    INPUT = 1
    OUTPUT = 2

class ClassType:
    STAGE_IN = 1
    COMPUTE = 2
    STAGE_OUT = 3
    CLEAN_UP = 4

class CostModel:
    DATACENTER = 1
    VM = 2

class Parameters:
    SOURCE = "source"
    BASE = 0
    INVALID = "Invalid"
    schedulingAlgorithm: SchedulingAlgorithm = SchedulingAlgorithm.INVALID
    planningAlgorithm: PlanningAlgorithm = PlanningAlgorithm.INVALID
    reduceMethod: str = None
    vmNum: int = 0
    daxPath: str = ""
    daxPaths: List[str] = []
    runtimePath: str = ""
    datasizePath: str = ""
    version: str = "1.1.0"
    note: str = " supports planning algorithm at Nov 9, 2013"
    oParams: OverheadParameters = None
    cParams: ClusteringParameters = None
    deadline: int = 0
    bandwidths: List[List[float]] = [[]]
    maxDepth: int = 0
    runtime_scale: float = 1.0
    costModel: CostModel = CostModel.DATACENTER

    def __init__(self):
        pass

    @staticmethod
    def init(vm: int, dax: Union[str, List[str]], runtime: str, datasize: str, op: OverheadParameters, cp: ClusteringParameters, 
             scheduler: SchedulingAlgorithm, planner: PlanningAlgorithm, rMethod: str, dl: int) -> None:
        Parameters.cParams: ClusteringParameters = cp
        Parameters.vmNum: int = vm
        if (isinstance(dax, str)):
            Parameters.daxPath: str = dax
        elif (isinstance(dax, List)):
            Parameters.daxPaths: List[str] = dax
        Parameters.runtimePath: str = runtime
        Parameters.datasizePath: str = datasize
        Parameters.oParams: OverheadParameters = op
        Parameters.schedulingAlgorithm: SchedulingAlgorithm = scheduler
        Parameters.planningAlgorithm: PlanningAlgorithm = planner
        Parameters.reduceMethod: str = rMethod
        Parameters.deadline: float = dl
        Parameters.maxDepth: int = 0

    @staticmethod
    def getOverheadParams() -> OverheadParameters:
        return Parameters.oParams

    @staticmethod
    def getReduceMethod() -> str:
        if Parameters.reduceMethod:
            return Parameters.reduceMethod
        else:
            return Parameters.INVALID

    @staticmethod
    def get_daxPath() -> str:
        return Parameters.daxPath

    @staticmethod
    def getRuntimePath() -> str:
        return Parameters.runtimePath

    @staticmethod
    def getDatasizePath() -> str:
        return Parameters.datasizePath

    @staticmethod
    def getVmNum() -> int:
        return Parameters.vmNum

    @staticmethod
    def getCostModel() -> CostModel:
        return Parameters.costModel

    @staticmethod
    def setVmNum(num: int) -> None:
        Parameters.vmNum = num

    @staticmethod
    def getClusteringParameters() -> ClusteringParameters:
        return Parameters.cParams

    @staticmethod
    def getSchedulingAlgorithm() -> SchedulingAlgorithm:
        return Parameters.schedulingAlgorithm

    @staticmethod
    def getPlanningAlgorithm() -> PlanningAlgorithm:
        return Parameters.planningAlgorithm

    @staticmethod
    def getVersion() -> str:
        return Parameters.version

    @staticmethod
    def printVersion() -> None:
        print(f"WorkflowSim Version: {Parameters.version}")
        print(f"Change Note: {Parameters.note}")

    @staticmethod
    def getDeadline() -> float:
        return Parameters.deadline

    @staticmethod
    def getMaxDepth() -> int:
        return Parameters.maxDepth

    @staticmethod
    def setMaxDepth(depth: int) -> None:
        Parameters.maxDepth = depth

    @staticmethod
    def setRuntimeScale(scale: float) -> None:
        Parameters.runtime_scale = scale

    @staticmethod
    def setCostModel(model: CostModel) -> None:
        Parameters.costModel = model

    @staticmethod
    def getRuntimeScale() -> float:
        return Parameters.runtime_scale

    @staticmethod
    def get_daxPaths() -> List[str]:
        return Parameters.daxPaths


# Example usage
if __name__ == "__main__":
    params = Parameters.init(10, "path_to_dax", "path_to_runtime", "path_to_datasize",
                            None, None, SchedulingAlgorithm.MAXMIN, PlanningAlgorithm.HEFT, "reducer", 1000)
