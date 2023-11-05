from typing import List
from workflowsim.Task import Task

class Job(Task):
    def __init__(self, job_id: int, job_length: int):
        super().__init__(job_id, job_length)
        self.taskList = []


    def get_task_list(self) -> List[Task]:
        return self.taskList


    def set_task_list(self, taskList: List[Task]):
        self.taskList = taskList


    def add_task_list(self, taskList: List[Task]):
        self.taskList.extend(taskList)


    def get_parent_list(self) -> List[Task]:
        return super().get_parentList()