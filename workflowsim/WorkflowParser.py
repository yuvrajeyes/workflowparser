from __future__ import annotations
import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")

from xml.etree import ElementTree as ET
from typing import List, Final, Dict
import threading
from cloudsim.Log import Log
from workflowsim.utils.Parameters import *
from workflowsim.utils.ReplicaCatalog import ReplicaCatalog

from workflowsim.FileItem import *
from workflowsim.Task import Task

import warnings

# Suppress the DeprecationWarning
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=DeprecationWarning)


class WorkflowParser:
    def __init__(self, userId):
        ## User id. used to create a new task.
        self.userId: Final[int] = userId
        ## The path to DAX file
        self.daxPath: Final[str] = Parameters.get_daxPath()
        ## The path to DAX files
        self.daxPaths: Final[List[str]] = Parameters.get_daxPaths()
        ## All tasks
        self.taskList: Final[List[Task]] = []
        ## current job id. In case multiple workflow submission
        self.jobIdStartsFrom: Final[int] = 1
        ## Map from task name to task
        self.mName2Task: Dict[str, Task] = dict()
        # Create a lock object
        self.lock = threading.Lock()  
        self.set_taskList([])


    def get_taskList(self) -> List[Task] :
        return self.taskList
    

    def set_taskList(self, taskList: List[Task]) -> None :
        self.taskList = taskList


    def parse(self) -> None :
        if (self.daxPath is not None):
            self.parseXmlFile(self.daxPath)
        elif (self.daxPaths is not None):
            for path in self.daxPaths:
                self.parseXmlFile(path)


    def set_depth(self, task: Task, depth: int) -> None :
        if (depth > task.get_depth()):
            task.set_depth(depth)
        for cTask in task.get_childList():
            self.set_depth(cTask, task.get_depth()+1)


    def parseXmlFile(self, path: str) -> None :
        # Parse the XML file using ElementTree
        tree = ET.parse(path)
        root = tree.getroot()
        mName2Task: Dict[str, Task] = {}
        # iterate over children of root
        for childnode in root:
            tag: str = childnode.tag.lower()
            if "job" in tag:
                length = 0
                node_id: str = childnode.get("id")
                nodeType : str= childnode.get("name")
                runtime: float = 0.1 # Default runtime if not specified
                nodeTime: str = childnode.get("runtime")
                if (nodeTime is not None) :
                    runtime = 1000*float(nodeTime)
                    if (runtime < 100) :
                        runtime = 100
                    length = int(runtime)
                else:
                    Log.print_line(f"Cannot find runtime for {node_id}, set it to be 0")
                # Apply runtime scaling, by default it is 1.0
                length *= Parameters.getRuntimeScale()
                mFileList: List[FileItem] = []
                for file in childnode:  # children of childnode
                    if ("uses" in file.tag.lower()):
                        filename: str = file.get("name")
                        if (filename is None) :
                            filename = file.get("file")
                        if (filename is None) :
                            Log.print_line("Error in parsing xml")
                        inout: str = file.get("link")
                        size: float = 0
                        filesize: str = file.get("size")
                        if (filesize is not None):
                            size = float(filesize)
                        else :
                            Log.print_line(f"File size not found for {filename}")
                        if (size == 0) :
                            size = 1  # Avoid CloudSim issue with size 0
                        if (size < 0) :
                            size = abs(size)
                            Log.print_line("Size is negative, assuming it's a parser error")
                        filetype = FileType.NONE
                        match inout:
                            case "input":
                                filetype = FileType.INPUT
                            case "output":
                                filetype = FileType.OUTPUT
                            case _:
                                Log.print_line("Parsing Error")
                        tFile: FileItem = None 
                        if (filetype == FileType.OUTPUT) :
                            tFile = FileItem(filename, size)
                        elif ReplicaCatalog.contains_file(filename):
                            tFile = ReplicaCatalog.get_file(filename)
                        else:
                            tFile = FileItem(filename, size)
                            ReplicaCatalog.set_file(filename, tFile)
                        tFile.set_type(filetype)
                        mFileList.append(tFile)
                task : Task = None
                # //In case of multiple workflow submission. Make sure the jobIdStartsFrom is consistent.
                with self.lock :
                    task = Task(self.jobIdStartsFrom, length)
                    self.jobIdStartsFrom += 1
                task.set_type(nodeType)
                task.set_user_id(self.userId)
                mName2Task[node_id] = task
                for file in mFileList:
                    task.add_required_file(file.name)
                task.set_fileList(mFileList)
                self.get_taskList().append(task)
            elif "child" in tag:
                childName: str = childnode.get("ref")
                if childName in mName2Task:
                    childTask: Task = mName2Task[childName]
                    for parent in childnode:
                        if "parent" in parent.tag.lower():
                            parentName: str = parent.get("ref")
                            if (parentName in mName2Task):
                                parentTask: Task = mName2Task[parentName]
                                parentTask.add_child(childTask)
                                childTask.add_parent(parentTask)
        # If a task has no parent, then it is root task.
        roots: List[Task] = []
        for task in mName2Task.values():
            task.set_depth(0)
            if not task.get_parentList():
                roots.append(task)
        # Add depth from top to bottom.
        for task in roots:
            self.set_depth(task, 1)
        # Clean them so as to save memory. Parsing workflow may take much memory
        mName2Task.clear()