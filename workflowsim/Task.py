from __future__ import annotations
import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")

from typing import List
from workflowsim.FileItem import FileItem
from cloudsim.Cloudlet import Cloudlet
from cloudsim.UtilizationModel import UtilizationModelFull


class Task(Cloudlet):
    def __init__(self, taskId: int, taskLength: int):
        super().__init__(taskId, taskLength, 1, 0, 0, UtilizationModelFull(), UtilizationModelFull(), UtilizationModelFull())
        self.taskId: int = taskId
        self.taskLength: int = taskLength
        self.childList: List[Task] = []
        self.parentList: List[Task] = []
        self.fileList: List[FileItem] = []
        self.impact: float = 0.0
        self.taskFinishTime: float = -1.0
        self.type: str = None
        self.priority: int = 0
        self.depth: int = 0

    def __str__(self):
        return f"Task ID: {self.taskId}, Task Length: {self.taskLength}, " \
               f"Child List: {self.childList}, Parent List: {self.parentList}, " \
               f"File List: {self.fileList}, Impact: {self.impact}, " \
               f"Task Finish Time: {self.taskFinishTime}, Type: {self.type}, " \
               f"Priority: {self.priority}, Depth: {self.depth}"


    def set_user_id(self, userId) -> None:
        self.userId = userId


    def set_type(self, type_: str):
        self.type = type_


    def get_type(self) -> str:
        return self.type


    def set_priority(self, priority: int):
        self.priority = priority


    def get_priority(self) -> int:
        return self.priority
    

    def set_depth(self, depth: int):
        self.depth = depth


    def get_depth(self) -> int:
        return self.depth
    

    def set_childList(self, childList: List['Task']):
        self.childList = childList


    def get_childList(self) -> List['Task']:
        return self.childList
    

    def set_parentList(self, parentList: List['Task']):
        self.parentList = parentList


    def get_parentList(self) -> List['Task']:
        return self.parentList
    
    
    def set_impact(self, impact: float):
        self.impact = impact


    def get_impact(self) -> float:
        return self.impact
    

    def add_childList(self, childList: List[Task]):
        self.childList.extend(childList)


    def add_parentList(self, parentList: List[Task]):
        self.parentList.extend(parentList)


    def add_child(self, task: Task):
        self.childList.append(task)


    def add_parent(self, task: Task):
        self.parentList.append(task)


    def add_file(self, file_: FileItem):
        self.fileList.append(file_)


    def set_fileList(self, fileList: List[FileItem]):
        self.fileList = fileList


    def get_fileList(self) -> List[FileItem]:
        return self.fileList


    def set_taskFinishTime(self, time: float):
        self.taskFinishTime = time


    def get_taskFinishTime(self) -> float:
        return self.taskFinishTime
    

    def get_processingCost(self) -> float:
        cost = Cloudlet.get_cost_per_sec() * Cloudlet.get_actual_cpu_time()
        fileSize = sum(len(file_) / 1_000_000 for file_ in self.get_fileList())
        cost += Cloudlet.costPerBw * fileSize
        return cost