from __future__ import annotations

from typing import Final, List
from cloudsim.core import CloudSim
from cloudsim.UtilizationModel import UtilizationModel


class StringBuffer:
    def __init__(self):
        self.history = []

    def append(self, text):
        self.history.append(text)

    def __str__(self):
        return "".join(self.history)
    
    def toString(self):
        return "".join(self.history)
    

class Cloudlet:
    class Resource:
        def __init__(self):
            # Cloudlet's submission time to a CloudResource.
            self.submissionTime: float = 0.0
            # The time of this Cloudlet resides in a CloudResource (from arrival time until departure time).
            self.wallClockTime: float = 0.0
            # The total execution time of this Cloudlet in a CloudResource.
            self.actualCPUTime: float = 0.0
            # Cost per second a CloudResource charges to execute this Cloudlet.
            self.costPerSec: float = 0.0
            # A CloudResource id.
            self.resourceId: int = -1
            # A CloudResource name.
            self.resourceName = None
            # Cloudlets Finished So Far
            self.finishedSoFar: int = 0

    # Constants
    CREATED: Final[int] = 0
    READY: Final[int] = 1
    QUEUED: Final[int] = 2
    INEXEC: Final[int] = 3
    SUCCESS: Final[int] = 4
    FAILED: Final[int] = 5
    CANCELED: Final[int] = 6
    PAUSED: Final[int] = 7
    RESUMED: Final[int] = 8
    FAILED_RESOURCE_UNAVAILABLE: Final[int] = 9

    newline: str = '\n'
    num = "{:.2f}"

    # Initialization
    def __init__(self, cloudletId: int, cloudletLength: int, pesNumber: int, cloudletFileSize: int, cloudletOutputSize: int,
        utilizationModelCpu: UtilizationModel, utilizationModelRam: UtilizationModel, utilizationModelBw: UtilizationModel,
        record: bool=False, fileList: List[str]=list()):

        self.userId: int = -1
        self.status: int = Cloudlet.CREATED
        self.cloudletId: Final[int] = cloudletId
        self.numberOfPes: int = pesNumber
        self.execStartTime: float = 0.0
        self.finishTime: float = -1.0
        self.classType: int = 0
        self.netToS: int = 0
        # Cloudlet length, Input and Output size should be at least 1 byte.
        self.cloudletLength: int = max(1, cloudletLength)
        self.cloudletFileSize: Final[int] = max(1, cloudletFileSize)
        self.cloudletOutputSize: Final[int] = max(1, cloudletOutputSize)

        self.resList: List[Cloudlet.Resource] = list() # [None]*2
        self.index: int = -1
        self.reservationId: int = -1
        self.record: bool = record
        self.history: StringBuffer = StringBuffer()
        self.vmId: int = -1
        self.accumulatedBwCost: float = 0
        self.costPerBw: float = 0.0
        self.requiredFiles: List[str] = fileList

        self.utilizationModelCPU: UtilizationModel = utilizationModelCpu
        self.utilizationModelRam: UtilizationModel = utilizationModelRam
        self.utilizationModelBw: UtilizationModel = utilizationModelBw

        
    def set_reservation_id(self, resId: int) -> bool:
        if (resId <=0):
            return False
        self.reservationId = resId
        return True
    

    def get_reservation_id(self) -> int:
        return self.reservationId
    

    def has_researved(self) -> bool:
        if (Cloudlet.reservationId == -1):
            return False
        return True
    

    def set_cloudlet_length(self, cloudletLength: int) -> bool:
        if (cloudletLength <=0 ):
            return False
        self.cloudletLength = cloudletLength
        return True
    

    def set_net_service_level(self, netServiceLevel: int) -> bool:
        if (netServiceLevel > 0):
            self.netToS = netServiceLevel
            return True
        return False
    
    
    def get_net_service_level(self) -> int:
        return self.netToS
    
    
    def get_waiting_time(self) -> float:
        if (self.index == -1):
            return 0.0
        return self.execStartTime - self.resList[self.index].submissionTime
    

    def set_class_type(self, classType: int) -> bool:
        if (classType > 0):
            self.classType = classType
            return True
        return False
    

    def get_class_type(self) -> int:
        return self.classType
    

    def set_number_of_pes(self, numberOfPes: int) -> bool:
        if (numberOfPes > 0):
            self.numberOfPes = numberOfPes
            return True
        return False
    

    def get_number_of_pes(self) -> int:
        return self.numberOfPes
    
    
    def get_cloudlet_history(self) -> str:
        msg: str = None
        if (self.history is None):
            msg = f"No history is recorded for Cloudlet # {self.cloudletId}"
        else:
            msg = self.history.toString()
        return msg
    
    
    def get_cloudlet_finished_so_far(self, resId: int=None) -> int:
        if resId is None:
            if (self.index == -1):
                return self.cloudletLength
            finish: Final[float] = self.resList[self.index].finishedSoFar
            if (finish > self.cloudletLength):
                return self.cloudletLength
            return finish
        else:
            resource: Cloudlet.Resource = self.get_resource_by_id(resId)
            if (resource is not None):
                return resource.finishedSoFar
            return 0
        
    
    def is_finished(self) -> bool:
        if (self.index == -1):
            return False
        #if result is 0 or -ve then this Cloudlet has finished
        result: int = self.cloudletLength - self.resList[self.index].finishedSoFar
        if (result <= 0.0):
            return True
        return False
    
    
    def set_cloudlet_finished_so_far(self, length: int) -> int:
        # if length is -ve then ignore
        if (length < 0.0 or self.index < 0):
            return    
        res: Cloudlet.Resource = self.resList[self.index]
        res.finishedSoFar = length
        if (self.record):
            self.write(f"Sets the length's finished so far to {length}")


    def set_user_id(self, user_id) -> None:
        self.userId = user_id
        if (self.record):
            self.write(f"Assign the Cloudlet to {CloudSim.get_entity_name(user_id)} (ID #{user_id})")


    def get_user_id(self):
        return self.userId
    

    def get_resource_id(self) -> int:
        if (self.index == -1):
            return -1
        return self.resList[self.index].resourceId
    

    def get_cloudlet_file_size(self) -> int:
        return self.cloudletFileSize
    
    
    def get_cloudlet_output_size(self) -> int:
        return self.cloudletOutputSize
    
    
    def set_resource_parameter(self, resource_id: int, cost_per_cpu: float, cost_per_bw: float=None):
        res: Final[Cloudlet.Resource] = Cloudlet.Resource()
        res.resourceId = resource_id
        res.costPerSec = cost_per_cpu
        res.resourceName = CloudSim.get_entity_name(resource_id)
        # Add into a list if moving to a new grid resource
        self.resList.append(res)
        if (self.index == -1 and self.record):
            self.write(f"Allocates this Cloudlet to {res.resourceName} (ID #{resource_id}) with cost = ${cost_per_cpu}/sec")
        elif (self.record):
            id: Final[int] = self.resList[self.index].resourceId
            name: Final[str] = self.resList[self.index].resourceName
            self.write(f"Moves Cloudlet from {name} (ID #{id}) to {res.resourceName} (ID #{resource_id}) with cost = ${cost_per_cpu}/sec")
        self.index += 1
        if (cost_per_bw is not None):
            self.costPerBw = cost_per_bw
            self.accumulatedBwCost = self.costPerBw * self.get_cloudlet_file_size()


    def set_submission_time(self, clockTime: float) -> None:
        if (clockTime < 0.0 or self.index < 0):
            return
        res : Cloudlet.Resource = self.resList[self.index]
        res.submissionTime = clockTime
        if (self.record):
            self.write(f"sets the submission time to {Cloudlet.num(clockTime)}")
    

    def get_submission_time(self, resId: int=None) -> float:
        if resId is None:
            if (self.index == -1):
                return 0.0
            return self.resList[self.index].submissionTime
        else:
            resource: Cloudlet.Resource = self.get_resource_by_id(resId)
            if (resource is not None):
                return resource.submissionTime
            return 0.0
        
    
    def set_exec_start_time(self, clcokTime: float) -> None:
        self.execStartTime = clcokTime
        if (self.record):
            self.write(f"Sets the execution start time to {Cloudlet.num(clcokTime)}")


    def get_exec_start_time(self) ->float:
        return self.execStartTime
    

    def set_exec_param(self, wallTime: float, actualTime: float) -> None:
        if (wallTime < 0.0 or actualTime < 0.0 or self.index < 0):
            return
        res : Cloudlet.Resource = self.resList[self.index]
        res.wallClockTime = wallTime
        res.actualCPUTime = actualTime
        if (self.record):
            self.write(f"Sets the wall clock time to {Cloudlet.num(wallTime)} and the actual CPU time to {Cloudlet.num(actualTime)}")


    def set_cloudlet_status(self, status: int) -> None:
        if (self.status == status):
            return
        # throws an exception if the new status is outside the range
        if (status < Cloudlet.CREATED or status > Cloudlet.FAILED_RESOURCE_UNAVAILABLE):
            raise Exception("Cloudlet.set_cloudlet_status() : Error - Invalid integer range for Cloudlet status.")
        if (status ==  Cloudlet.SUCCESS):
            self.finishTime = CloudSim.clock()
        if (self.record):
            self.write(f"Sets Cloudlet status from {self.get_cloudlet_status_string()} to {Cloudlet.get_status_string(status)}")
        self.status = status


    def get_cloudlet_status(self) -> int:
        return self.status
    

    def get_cloudlet_status_string(self) -> str:
        return Cloudlet.get_status_string(self.status)
    

    @staticmethod
    def get_status_string(status):
        status_mapping = {
            Cloudlet.CREATED: "Created",
            Cloudlet.READY: "Ready",
            Cloudlet.INEXEC: "InExec",
            Cloudlet.SUCCESS: "Success",
            Cloudlet.QUEUED: "Queued",
            Cloudlet.FAILED: "Failed",
            Cloudlet.CANCELED: "Canceled",
            Cloudlet.PAUSED: "Paused",
            Cloudlet.RESUMED: "Resumed",
            Cloudlet.FAILED_RESOURCE_UNAVAILABLE: "Failed_resource_unavailable",
        }
        return status_mapping.get(status, "Unknown")
    
    
    def get_cloudlet_length(self) -> int:
        return self.cloudletLength
    

    def get_cloudlet_total_length(self) -> int:
        return self.cloudletLength * self.numberOfPes
    
    
    def get_cost_per_sec(self, resId: int=None) -> float:
        if resId is None:
            if (self.index == -1):
                return 0.0
            return self.resList[self.index].costPerSec
        else:
            resource: Cloudlet.Resource = self.get_resource_by_id(resId)
            if (resource is not None):
                return resource.costPerSec
            return 0.0


    def get_wall_clock_time(self, resId: int=None) -> float:
        if resId is None:
            if (self.index == -1):
                return 0.0
            return self.resList[self.index].wallClockTime
        else:
            resource: Cloudlet.Resource = self.get_resource_by_id(resId)
            if (resource is not None):
                return resource.wallClockTime
            return 0.0
        
        
    def get_all_resource_name(self) -> List[str]:
        size: Final[int] = len(self.resList)
        if (size <= 0):
            return None
        data: List[str] = [None]*size
        for i in range(size):
            data[i] = self.resList[i].resourceName
        return data
    
    
    def get_all_resource_id(self) -> List[int]:
        size: Final[int] = len(self.resList)
        if (size <= 0):
            return None
        data: List[int] = [None]*size
        for i in range(size):
            data[i] = self.resList[i].resourceId
        return data
    
    
    def get_actual_cpu_time(self, resId :int=None) ->float:
        if resId is None:
            return self.finishTime - self.execStartTime
        else:
            resource = self.get_resource_by_id(resId)
            if (resource is not None):
                return resource.actualCPUTime
            return 0.0
        
        
    def get_resource_name(self, res_id: int) -> str:
        resource =self.get_resource_by_id(res_id)
        if resource is not None:
            return resource.resourceName
        return None
    
    
    def get_resource_by_id(self, resourceId: int) -> Resource:
        for resource in self.resList:
            if (resource.resourceId == resourceId):
                return resource
        return None
    
    
    def get_finish_time(self) ->float:
        return self.finishTime
    
    
    def write(self, s: str) -> None:
        if (not self.record):
            return
        if (Cloudlet.num is None or Cloudlet.history is None):
            Cloudlet.history.append("TIme below denotes the simulation time.")
            Cloudlet.history.append(Cloudlet.newline)
            Cloudlet.history.append(f"Time (sec) Description Cloudlet #{self.cloudletId}")
            Cloudlet.history.append(Cloudlet.newline)
            Cloudlet.history.append("-------------------------------------------------")
            Cloudlet.history.append(Cloudlet.newline)
            Cloudlet.history.append(f"{Cloudlet.num(CloudSim.clock())}")
            Cloudlet.history.append(f"\tCreates Cloudlet ID #{self.cloudletId}")
            Cloudlet.history.append(Cloudlet.newline)
        Cloudlet.history.append(Cloudlet.num(CloudSim.clock()))
        Cloudlet.history.append(f"\t{s}{Cloudlet.newline}")


    def get_status(self) -> int:
        return self.status
    
    
    def get_cloudlet_id(self) -> int :
        return self.cloudletId
    
    
    def get_vm_id(self) -> int:
        return self.vmId
    
    
    def set_vm_id(self, vmId: int) -> None:
        self.vmId = vmId


    def get_processing_cost(self) -> float:
        return self.accumulatedBwCost + self.costPerBw*self.cloudletOutputSize
    

    def get_required_files(self) -> List[str]:
        return self.requiredFiles
    
    
    def set_required_files(self, requiredFiles: List[str]) -> None:
        self.requiredFiles = requiredFiles


    def add_required_file(self, fileName: str) -> bool:
        # if the list is empty
        if (self.get_required_files() is None):
            self.set_required_files([])
        # check whether filename already exists or not
        result: bool = False
        for i in range(len(self.get_required_files())):
            temp: Final[str] = self.get_required_files()[i]
            if (temp == fileName):
                result = True
                break
        if (not result):
            self.get_required_files().append(fileName)
        return result
    
    
    def delete_required_files(self, fileName: str) -> None:
        if (self.get_required_files() is None):
            return False
        for i in range(len(self.get_required_files())):
            temp: Final[str] = self.get_required_files[i]
            if (temp == fileName):
                self.get_required_files().remove(temp)
                return True
        return False
    
            
    def requires_files(self) -> bool:
        if (self.get_required_files() is not None and len(self.get_required_files()) > 0):
            return True
        return False
    

    def get_utilization_model_cpu(self) -> UtilizationModel:
        return self.utilizationModelCpu
    

    def set_utilization_model_cpu(self, utilizationModelCpu: UtilizationModel):
        self.utilizationModelCpu = utilizationModelCpu


    def get_utilization_model_ram(self) -> UtilizationModel:
        return self.utilizationModelRam
    

    def set_utilization_model_ram(self, utilizationModelRam: UtilizationModel):
        self.utilizationModelRam = utilizationModelRam


    def get_utilization_model_bw(self) -> UtilizationModel:
        return self.utilizationModelBw
    

    def set_utilization_model_bw(self, utilizationModelBw: UtilizationModel):
        self.utilizationModelBw = utilizationModelBw


    def get_utilization_of_cpu(self, time) -> float:
        return self.utilizationModelCpu.get_utilization(time)


    def get_utilization_of_ram(self, time) -> float:
        return self.utilizationModelRam.get_utilization(time)


    def get_utilization_of_bw(self, time) -> float:
        return self.utilizationModelBw.get_utilization(time)