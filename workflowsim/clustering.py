import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")

from typing import List, Dict, Union
from workflowsim.FileItem import FileItem
from workflowsim.Job import Job
from workflowsim.Task import Task
from workflowsim.utils.Parameters import Parameters
from workflowsim.utils.Parameters import ClassType, FileType
from workflowsim.utils.OverheadParameters import OverheadParameters

class BasicClustering():
    def __init__(self):
        self.taskList: List[Task] = []
        self.jobList: List[Job] = []
        self.mTask2Job: Dict[Task, Job] = {}
        self.allFileList: List[FileItem] = []
        self.idIndex: int = 0
        self.root: Task = None


    def get_task_files(self) -> List[FileItem]:
        return self.allFileList
    

    def set_taskList(self, taskList: List[Task]) -> None:
        self.taskList = taskList


    def get_jobList(self) -> List[Job]:
        return self.jobList
    

    def get_taskList(self) -> List[Task]:
        return self.taskList
    
    
    def get_task2job(self) -> Dict:
        return self.mTask2Job
    

    def run(self) -> None:
        self.get_task2job().clear()
        for task in self.get_taskList():
            lst: List[Task] = []
            lst.append(task)
            job: Job = self.add_tasks_to_job(lst)
            job.set_vm_id(task.get_vm_id())
            self.get_task2job()[task] = job
        self.update_dependencies()


    def add_tasks_to_job(self, taskList: Union[Task, List[Task]]) -> Job:
        if (isinstance(taskList, Task)):
            tasks: List[Task] = []
            tasks.append(task)
            taskList = tasks

        if taskList and len(taskList) > 0:
            length: int = 0
            userId: int = 0
            priority: int = 0
            depth: int = 0
            job: Job = Job(self.idIndex, length)
            job.set_class_type(ClassType.COMPUTE)
            for task in taskList:
                length += task.get_cloudlet_length()
                userId = task.get_user_id()
                priority = task.get_priority()
                depth = task.get_depth()
                fileList: List[FileItem] = task.get_fileList()
                job.get_task_list().append(task)
                self.get_task2job()[task] = job
                for file in fileList:
                    hasFile: bool = file in job.get_fileList()
                    if not hasFile:
                        job.get_fileList().append(file)
                        if file.get_type().value == FileType.INPUT:
                            # for stag-in jobs to be used
                            if file not in self.allFileList:
                                self.allFileList.append(file)
                        elif file.get_type().value == FileType.OUTPUT:
                            self.allFileList.append(file)
                for fileName in task.get_required_files():
                    if fileName not in job.get_required_files():
                        job.get_required_files().append(fileName)
            job.set_cloudlet_length(length)
            job.set_user_id(userId)
            job.set_depth(depth)
            job.set_priority(priority)
            self.idIndex += 1
            self.jobList.append(job)
            return job
        return None
    

    def add_clust_delay(self) -> None:
        for job in self.jobList:
            op: OverheadParameters = Parameters.getOverheadParams()
            delay: float = op.get_clust_delay(job) * 1000
            length: int = job.get_cloudlet_length() + int(delay)
            job.set_cloudlet_length(length)


    def update_dependencies(self) -> None:
        for task in self.taskList:
            job: Job = self.mTask2Job[task]
            for parent_task in task.get_parentList():
                parent_job: Job = self.mTask2Job[parent_task]
                if parent_job not in job.get_parent_list() and parent_job != job:
                    job.add_parent(parent_job)
            for child_task in task.get_childList():
                child_job = self.mTask2Job[child_task]
                # avoid duplication
                if child_job not in job.get_childList() and child_job != job:
                    job.add_child(child_job)
        self.mTask2Job.clear()
        self.taskList.clear()

    def add_root(self) -> Task:
        # Add a fake root task, If you have used addRoot, please use clean() after that
        if self.root is None:
            self.root = Task(len(self.taskList) + 1, 0)
            for node in self.taskList:
                if len(node.get_parentList())==0:
                    node.add_parent(self.root)
                    self.root.add_child(node)
            self.taskList.append(self.root)
        return self.root

    def clean(self) -> None:
        # delete the root task
        if self.root is not None:
            for i in range(len(self.root.get_childList())):
                node = self.root.get_childList()[i]
                node.get_parentList().remove(self.root)
                self.root.get_childList().remove(node)
                i -= 1
            self.taskList.remove(self.root)