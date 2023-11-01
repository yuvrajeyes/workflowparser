from __future__ import annotations

from typing import List, Final
from cloudsim.core import CloudSim
from cloudsim.Cloudlet import Cloudlet
from cloudsim.Consts import Consts


class ResCloudlet:
    NOT_FOUND: int = -1
    def __init__(self, cloudlet: Cloudlet, startTime=0, duration=0, reservID=NOT_FOUND):
        self.cloudlet: Final[Cloudlet] = cloudlet
        self.startTime: Final[int] = startTime
        self.duration: Final[int] = duration
        self.reservId: Final[int] = reservID

        self.pesNumber: int = self.cloudlet.get_number_of_pes()
        self.machineArrayId: List[int] = [self.NOT_FOUND] * self.pesNumber if self.pesNumber > 1 else None
        self.peArrayId: List[int] = [self.NOT_FOUND] * self.pesNumber if self.pesNumber > 1 else None
        self.arrivalTime: float = CloudSim.clock()
        self.cloudlet.set_submission_time(self.arrivalTime)
        self.machineId: int = self.NOT_FOUND
        self.peId: int = self.NOT_FOUND
        self.index: int = 0
        self.finishedTime: float = self.NOT_FOUND
        self.startExecTime: float = 0.0
        self.totalCompletionTime: float = 0.0
        self.cloudletFinishedSoFar: int = self.cloudlet.get_cloudlet_finished_so_far() * Consts.MILLION if self.cloudlet.get_cloudlet_status() != Cloudlet.SUCCESS else self.cloudlet.get_cloudlet_length()


    def get_start_time(self) -> int:
        return self.startTime
    
    
    def get_duration_time(self) -> int:
        return self.duration
    
    
    def get_number_of_pes(self) -> int:
        return self.pesNumber
    
    
    def get_reservation_id(self) -> int:
        return self.reservId
    
    
    def has_reserved(self) -> bool:
        if (self.reservId == ResCloudlet.NOT_FOUND):
            return False
        return True
    

    def init(self) -> None:
        self.pesNumber: int = self.cloudlet.get_number_of_pes()
        self.machineArrayId: List[int] = [self.NOT_FOUND] * self.pesNumber if self.pesNumber > 1 else None
        self.peArrayId: List[int] = [self.NOT_FOUND] * self.pesNumber if self.pesNumber > 1 else None
        self.arrivalTime: float = CloudSim.clock()
        self.cloudlet.set_submission_time(self.arrivalTime)
        self.machineId: int = self.NOT_FOUND
        self.peId: int = self.NOT_FOUND
        self.index: int = 0
        self.finishedTime: float = self.NOT_FOUND
        self.startExecTime: float = 0.0
        self.totalCompletionTime: float = 0.0
        self.cloudletFinishedSoFar: int = self.cloudlet.get_cloudlet_finished_so_far() * Consts.MILLION if self.cloudlet.get_cloudlet_status() != Cloudlet.SUCCESS else self.cloudlet.get_cloudlet_length()


    def get_cloudlet_id(self) -> int:
        return self.cloudlet.get_cloudlet_id()
    

    def get_user_id(self) -> int:
        return self.cloudlet.get_user_id()
    

    def get_cloudlet_Length(self) -> int:
        return self.cloudlet.get_cloudlet_length()
    

    def get_cloudlet_total_length(self) -> int:
        return self.cloudlet.get_cloudlet_total_length()
    

    def get_cloudlet_class_type(self) -> int:
        return self.cloudlet.get_class_type()
    

    def set_cloudlet_status(self, status: int) -> bool:
        prevStatus: int = self.cloudlet.get_cloudlet_status()
        if (prevStatus == status):
            return False
        
        success: bool = True
        clock: float = CloudSim.clock()
        self.cloudlet.set_cloudlet_status(status=status)
        # if a previous Cloudlet status is INEXEC
        if (prevStatus == Cloudlet.INEXEC):
            # and current status is either CANCELED, PAUSED or SUCCESS
            if (status==Cloudlet.CANCELED or status==Cloudlet.PAUSED or status==Cloudlet.SUCCESS):
                # then update the Cloudlet completion time
                self.totalCompletionTime += (clock - self.startExecTime)
                self.index=0
                return True
        if (prevStatus==Cloudlet.RESUMED and status==Cloudlet.SUCCESS):
            # then update the Cloudlet completion time
            self.totalCompletionTime += (clock - self.startExecTime)
            return True
        # if a Cloudlet is now in execution
        if (status==Cloudlet.INEXEC or (prevStatus==Cloudlet.PAUSED and status==Cloudlet.RESUMED)):
            self.startExecTime = clock
            self.cloudlet.set_exec_start_time(self.startExecTime)
        return success
    

    def get_exec_start_time(self) -> float:
        return self.cloudlet.get_exec_start_time()
    

    def set_exec_param(self, wallClockTime: float, actualCPUTime: float) -> None:
        self.cloudlet.set_exec_param(wallClockTime, actualCPUTime)


    def set_machine_and_pe_id(self, machineId: int, peId: int) -> None:
        self.machineId = machineId
        self.peId = peId
        # if this job requires 1 Pe
        if (self.peArrayId is not None and self.pesNumber > 1):
            self.machineArrayId[self.index] = machineId
            self.peArrayId[self.index] = peId
            self.index += 1

    def get_machine_id(self) -> int:
        return self.machineId
    

    def get_pe_id(self) -> int:
        return self.peId
    

    def get_pe_id_list(self) -> List[int]:
        return self.peArrayId
    

    def get_machine_id_list(self) -> List[int]:
        return self.machineArrayId
    
    
    def get_remaining_cloudlet_length(self) -> int:
        length: int = self.cloudlet.get_cloudlet_total_length()*Consts.MILLION - self.cloudletFinishedSoFar
        # Remaining Cloudlet length can't be negative number.
        if (length < 0):
            return 0
        return int(length//Consts.MILLION)
    

    def finalize_cloudlet(self) -> None:
        wallClockTime: float = CloudSim.clock() - self.arrivalTime
        self.cloudlet.set_exec_param(wallClockTime, self.totalCompletionTime)
        finished: int = 0
        if (self.cloudlet.get_cloudlet_status()==Cloudlet.SUCCESS):
            finished = self.cloudlet.get_cloudlet_length()
        else:
            finished = int(self.cloudletFinishedSoFar/Consts.MILLION)
        self.cloudlet.set_cloudlet_finished_so_far(finished)


    def update_cloudlet_finished_so_far(self, miLength: int) -> None:
        self.cloudletFinishedSoFar += miLength


    def get_cloudlet_arival_time(self) -> float:
        return self.arrivalTime
    

    def set_finish_time(self, time: float) -> None:
        if (time < 0.0):
            return
        self.finishedTime = time


    def get_cloudlet_finish_time(self) -> float:
        return self.finishedTime
    
    
    def get_cloudlet(self) -> Cloudlet:
        return self.cloudlet
    
    
    def get_cloudlet_status(self) -> int:
        return self.cloudlet.get_cloudlet_status()
    

    def get_UID(self) -> str:
        return f"{self.get_user_id()}-{self.get_cloudlet_id()}"