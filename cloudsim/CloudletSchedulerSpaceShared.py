from __future__ import annotations

from typing import List
from cloudsim.CloudletScheduler import CloudletScheduler
from cloudsim.ResCloudlet import ResCloudlet
from cloudsim.Cloudlet import Cloudlet
from cloudsim.core import CloudSim


class CloudletSchedulerSpaceShared(CloudletScheduler):
    def __init__(self):
        super().__init__()
        self.cloudletWaitingList: List[ResCloudlet] = []
        self.cloudletExecList: List[ResCloudlet] = []
        self.cloudletPausedList: List[ResCloudlet] = []
        self.cloudletFinishedList: List[ResCloudlet] = []
        self.usedPes: int = 0
        self.currentCpus: int = 0


    def update_vm_processing(self, currentTime: float, mipsShare: List[float]) -> float:
        self.set_current_mips_share(mipsShare)
        timeSpam: float = currentTime - self.get_previous_time()
        capacity: float = 0.0
        cpus: int = 0

        for mips in mipsShare:  # count the CPUs available to the VMM
            capacity += mips
            if mips > 0:
                cpus += 1
        self.currentCpus = cpus
        capacity /= cpus  # average capacity of each cpu

        # each machine in the exec list has the same amount of cpu
        for rcl in self.get_cloudlet_exec_list():
            rcl.update_cloudlet_finished_so_far(int(capacity * timeSpam * rcl.get_number_of_pes() * 1e6))
        
        # no more cloudlets in this scheduler
        if len(self.get_cloudlet_exec_list()) == 0 and len(self.get_cloudlet_waiting_list()) == 0:
            self.set_previous_time(currentTime)
            return 0.0
        
        # update each cloudlet
        finished: int = 0
        toRemove: List[ResCloudlet] = []
        for rcl in self.get_cloudlet_exec_list():
            # finished anyway, rounding issue...
            if rcl.get_remaining_cloudlet_length() == 0:
                toRemove.append(rcl)
                self.cloudlet_finish(rcl)
                finished += 1
        for rcl in toRemove:
            self.get_cloudlet_exec_list().remove(rcl)

        # for each finished cloudlet, add a new one from the waiting list
        if len(self.get_cloudlet_waiting_list()) > 0:
            for i in range(finished):
                toRemove.clear()
                for rcl in self.get_cloudlet_waiting_list():
                    if (self.currentCpus - self.usedPes) >= rcl.get_number_of_pes():
                        rcl.set_cloudlet_status(Cloudlet.INEXEC)
                        for k in range(rcl.get_number_of_pes()):
                            rcl.set_machine_and_pe_id(0, i)
                        self.get_cloudlet_exec_list().append(rcl)
                        self.usedPes += rcl.get_number_of_pes()
                        toRemove.append(rcl)
                        break
                for rcl in toRemove:
                    self.get_cloudlet_waiting_list().remove(rcl)

        # estimate finish time of cloudlets in the execution queue
        nextEvent: float = float('inf')
        for rcl in self.get_cloudlet_exec_list():
            remainingLength: float = rcl.get_remaining_cloudlet_length()
            estimatedFinishTime: float = currentTime + (remainingLength / (capacity * rcl.get_number_of_pes()))
            if estimatedFinishTime - currentTime < CloudSim.get_min_time_between_events():
                estimatedFinishTime = currentTime + CloudSim.get_min_time_between_events()
            if estimatedFinishTime < nextEvent:
                nextEvent = estimatedFinishTime
        self.set_previous_time(currentTime)
        return nextEvent


    def cloudlet_cancel(self, cloudletId: int) -> Cloudlet:
        # First, looks in the finished queue
        for rcl in self.get_cloudlet_finished_list():
            if rcl.get_cloudlet_id() == cloudletId:
                self.get_cloudlet_finished_list().remove(rcl)
                return rcl.get_cloudlet()
            
        # Then searches in the exec list
        for rcl in self.get_cloudlet_exec_list():
            if rcl.get_cloudlet_id() == cloudletId:
                self.get_cloudlet_exec_list().remove(rcl)
                if rcl.get_remaining_cloudlet_length() == 0:
                    self.cloudlet_finish(rcl)
                else:
                    rcl.set_cloudlet_status(Cloudlet.CANCELED)
                return rcl.get_cloudlet()
            
        # Now, looks in the paused queue
        for rcl in self.get_cloudlet_paused_list():
            if rcl.get_cloudlet_id() == cloudletId:
                self.get_cloudlet_paused_list().remove(rcl)
                return rcl.get_cloudlet()
            
        # Finally, looks in the waiting list
        for rcl in self.get_cloudlet_waiting_list():
            if rcl.get_cloudlet_id() == cloudletId:
                rcl.set_cloudlet_status(Cloudlet.CANCELED)
                self.get_cloudlet_waiting_list().remove(rcl)
                return rcl.get_cloudlet()
        return None


    def cloudlet_pause(self, cloudletId: int) -> bool:
        found: bool = False
        position: int = 0
        # first, looks for the cloudlet in the exec list
        for rcl in self.get_cloudlet_exec_list():
            if rcl.get_cloudlet_id() == cloudletId:
                found = True
                break
            position += 1
            
        if found:
            # moves to the paused list
            rgl: ResCloudlet = self.get_cloudlet_exec_list().pop(position)
            if rgl.get_remaining_cloudlet_length() == 0:
                self.cloudlet_finish(rgl)
            else:
                rgl.set_cloudlet_status(Cloudlet.PAUSED)
                self.get_cloudlet_paused_list().append(rgl)
            return True
        
        # now, look for the cloudlet in the waiting list
        position = 0
        found = False
        for rcl in self.get_cloudlet_waiting_list():
            if rcl.get_cloudlet_id() == cloudletId:
                found = True
                break
            position += 1

        if found:
            # moves to the paused list
            rgl: ResCloudlet = self.get_cloudlet_waiting_list().pop(position)
            if rgl.get_remaining_cloudlet_length() == 0:
                self.cloudlet_finish(rgl)
            else:
                rgl.set_cloudlet_status(Cloudlet.PAUSED)
                self.get_cloudlet_paused_list().append(rgl)
            return True
        return False


    def cloudlet_finish(self, rcl: ResCloudlet) -> None:
        rcl.set_cloudlet_status(Cloudlet.SUCCESS)
        rcl.finalize_cloudlet()
        self.get_cloudlet_finished_list().append(rcl)
        self.usedPes -= rcl.get_number_of_pes()


    def cloudlet_resume(self, cloudletId: int) -> float:
        found: bool = False
        position: int = 0
        # look for the cloudlet in the paused list
        for rcl in self.get_cloudlet_paused_list():
            if rcl.get_cloudlet_id() == cloudletId:
                found = True
                break
            position += 1

        if found:
            rcl: ResCloudlet = self.get_cloudlet_paused_list().pop(position)
            # it can go to the exec list
            if (self.currentCpus - self.usedPes) >= rcl.get_number_of_pes():
                rcl.set_cloudlet_status(Cloudlet.INEXEC)
                for i in range(rcl.get_number_of_pes()):
                    rcl.set_machine_and_pe_id(0, i)
                size: float = rcl.get_remaining_cloudlet_length() * rcl.get_number_of_pes()
                rcl.get_cloudlet().set_cloudlet_length(size)
                self.get_cloudlet_exec_list().append(rcl)
                self.usedPes += rcl.get_number_of_pes()

                # calculate the expected time for cloudlet completion
                capacity: float = 0.0
                cpus: int = 0
                for mips in self.get_current_mips_share():
                    capacity += mips
                    if mips > 0:
                        cpus += 1
                self.currentCpus = cpus
                capacity /= cpus

                remainingLength: float = rcl.get_remaining_cloudlet_length()
                return CloudSim.clock() + (remainingLength / (capacity * rcl.get_number_of_pes()))
            
            else: # no enough free PEs: go to the waiting queue
                rcl.set_cloudlet_status(Cloudlet.QUEUED)
                size: float = rcl.get_remaining_cloudlet_length() * rcl.get_number_of_pes()
                rcl.get_cloudlet().set_cloudlet_length(size)
                self.get_cloudlet_waiting_list().append(rcl)
                return 0.0
            
        # not found in the paused list: either it is in in the queue, executing or not exist
        return 0.0
    
    
    def cloudlet_submit(self, cloudlet: Cloudlet, fileTransferTime: float=0.0) -> float:
        # Check if there are enough free PEs
        if (self.currentCpus - self.usedPes) >= cloudlet.get_number_of_pes():
            rcl: ResCloudlet = ResCloudlet(cloudlet)
            rcl.set_cloudlet_status(Cloudlet.INEXEC)
            for i in range(cloudlet.get_number_of_pes()):
                rcl.set_machine_and_pe_id(0, i)
            self.get_cloudlet_exec_list().append(rcl)
            self.usedPes += cloudlet.get_number_of_pes()
        else:  # Not enough free PEs: go to the waiting queue
            rcl = ResCloudlet(cloudlet)
            rcl.set_cloudlet_status(Cloudlet.QUEUED)
            self.get_cloudlet_waiting_list().append(rcl)
            return 0.0
        
        # Calculate the expected time for cloudlet completion
        capacity: float = 0.0
        cpus: int = 0
        for mips in self.get_current_mips_share():
            capacity += mips
            if mips > 0:
                cpus += 1
        self.currentCpus = cpus
        capacity /= cpus

        # Use the current capacity to estimate the extra amount of
        # time for file transferring. It must be added to the cloudlet length
        extra_size: float = capacity * fileTransferTime
        length: int = cloudlet.get_cloudlet_length() + extra_size
        cloudlet.set_cloudlet_length(length)
        return cloudlet.get_cloudlet_length() / capacity
    

    def get_cloudlet_status(self, cloudletId: int) -> int:
        # Check in the exec list
        for rcl in self.get_cloudlet_exec_list():
            if rcl.get_cloudlet_id() == cloudletId:
                return rcl.get_cloudlet_status()
            
        # Check in the paused list
        for rcl in self.get_cloudlet_paused_list():
            if rcl.get_cloudlet_id() == cloudletId:
                return rcl.get_cloudlet_status()
            
        # Check in the waiting list
        for rcl in self.get_cloudlet_waiting_list():
            if rcl.get_cloudlet_id() == cloudletId:
                return rcl.get_cloudlet_status()
        return -1
    
    
    def get_total_utilization_of_cpu(self, time: float) -> float:
        total_utilization: float = 0.0
        for rcl in self.get_cloudlet_exec_list():
            total_utilization += rcl.get_cloudlet().get_utilization_of_cpu(time)
        return total_utilization


    def is_finished_cloudlets(self) -> bool:
        return len(self.get_cloudlet_finished_list()) > 0
    

    def get_next_finished_cloudlet(self):
        finishedList: List[ResCloudlet] = self.get_cloudlet_finished_list()
        if len(finishedList) > 0:
            return finishedList.pop(0).get_cloudlet()
        return None


    def running_cloudlets(self) -> int:
        return len(self.get_cloudlet_exec_list())
    

    def migrate_cloudlet(self):
        rcl: ResCloudlet = self.get_cloudlet_exec_list().pop(0)
        rcl.finalize_cloudlet()
        cl: Cloudlet = rcl.get_cloudlet()
        self.usedPes -= cl.get_number_of_pes()
        return cl


    def set_cloudlet_waiting_list(self, cloudWaitingList: List[ResCloudlet]) -> None:
        self.cloudletWaitingList = cloudWaitingList


    def get_cloudlet_waiting_list(self) -> List[ResCloudlet]:
        return self.cloudletWaitingList
    
    
    def set_cloudlet_exec_list(self, cloudExecList: List[ResCloudlet]) -> None:
        self.cloudletExecList = cloudExecList


    def get_cloudlet_exec_list(self) -> List[ResCloudlet]:
        return self.cloudletExecList
    
    
    def set_cloudlet_paused_list(self, cloudPausedList: List[ResCloudlet]) -> None:
        self.cloudletPausedList = cloudPausedList


    def get_cloudlet_paused_list(self) -> List[ResCloudlet]:
        return self.cloudletPausedList
    

    def set_cloudlet_finished_list(self, cloudFinishedList: List[ResCloudlet]) -> None:
        self.cloudletFinishedList = cloudFinishedList


    def get_cloudlet_finished_list(self) -> List[ResCloudlet]:
        return self.cloudletFinishedList
    
    
    def get_current_requested_mips(self) -> List[float]:
        mipsShare: List[float] = []
        if self.get_current_mips_share() is not None:
            for mips in self.get_current_mips_share():
                mipsShare.append(mips)
        return mipsShare
    

    def get_total_current_available_mips_for_cloudlet(self, rcl: ResCloudlet, mipsShare: List[float]) -> float:
        capacity = 0.0
        cpus = 0
        for mips in mipsShare:
            capacity += mips
            if mips > 0:
                cpus += 1
        self.currentCpus = cpus
        capacity /= cpus
        return capacity
    
    
    def get_total_current_allocated_mips_for_cloudlet(self, rcl: ResCloudlet, time: float) -> float:
        return 0.0
    

    def get_total_current_requested_mips_for_cloudlet(self, rcl: ResCloudlet, time: float) -> float:
        return 0.0


    def get_current_requested_utilization_of_ram(self) -> float:
        return 0.0
    

    def get_current_requested_utilization_of_bw(self) -> float:
        return 0.0
