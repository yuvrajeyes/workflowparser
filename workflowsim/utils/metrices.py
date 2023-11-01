from __future__ import annotations
import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")

from typing import List, Dict, cast
from cloudsim.Consts import Consts
from cloudsim.Log import Log
from workflowsim.CondorVM import CondorVM
from workflowsim.Job import Job
from workflowsim.CustomVM import CustomVM


class Metrics:
    @staticmethod
    def get_makespan(jobs: List[Job]) -> float:
        start: float = float('inf')
        end: float = -float('inf')
        for j in jobs:
            start = min(start, j.execStartTime)
            end = max(end, j.finishTime)
        return end-start
    
    
    @staticmethod
    def get_cost(vms: List[CustomVM], jobs: List[Job]):
        cost: float = 0.0
        for j in jobs:
            vm: CondorVM = None
            for cvm in vms:
                if (cvm.id == j.vmId):
                    vm = cvm
                    break
            assert vm != None, "VM can't be None"
            # cost for execution on vm
            cost += j.get_actual_cpu_time() * vm.cost
            # cost for file transfer on this vm
            fileSize: int = 0
            for file in j.fileList:
                fileSize += file.size / Consts.MILLION
            cost += vm.costPerBw*fileSize
        return cost
    
    
    @staticmethod
    def get_power(vm: CustomVM, runningFreq: float) -> float:
        runningVolt: float = vm.minVolt + (vm.maxVolt - vm.minVolt)*(runningFreq-vm.minFreq)/(vm.maxFreq - vm.minFreq)
        return runningFreq * runningVolt * runningVolt
    

    @staticmethod
    def get_utilization(jobs: List[Job], vms: List[CustomVM]) -> float:
        utilization: float = 0.0
        makespan: float = Metrics.get_makespan(jobs)
        vmsz: int = 0
        activeTimes: Dict[int, float] = dict()
        # initialization
        for vm in vms:
            activeTimes[vm.id] = 0
        # get active times of each vm
        for j in jobs:
            vm: CustomVM = None
            for cvm in vms:
                if (cvm.id == j.vmId):
                    vm = cvm
                    break
            activeTimes[vm.id] = activeTimes[j.vmId]+j.get_actual_cpu_time()
        # Number of active vms
        for vm in vms:
            if (vm.powerOn):
                vmsz += 1
            utilization += (activeTimes[vm.id] / makespan)

        if (vmsz != 0):
            utilization /= vmsz
            utilization *= 100
        return utilization
    

    @staticmethod
    def get_energy_consumed(jobs: List[Job], vms: List[CustomVM]):
        energy: float = 0.0
        activeTimes: Dict[int, float] = dict()
        # initialization
        for vm in vms:
            activeTimes[vm.id] = 0
        for j in jobs:
            # get the vm running this job
            vm: CustomVM = None
            for cvm in vms:
                if (cvm.id == j.vmId):
                    vm = cvm
                    break
            assert vm != None, "VM can't be None"
            energy += (Metrics.get_power(vm, vm.maxFreq)*j.get_actual_cpu_time())
            activeTimes[vm.id] = activeTimes[vm.id] + j.cloudletLength/vm.mips

        makespan: float = Metrics.get_makespan(jobs)
        for vm in vms:
            if (vm.powerOn):
                energy += (Metrics.get_power(vm, vm.minFreq)*(makespan-activeTimes[vm.id]))
        return energy
    

    @staticmethod
    def print_matrices(jobs: List[Job], vms: List[CustomVM]):
        Log.print_line("Makesapn: " + str(Metrics.get_makespan(jobs)))
        Log.print_line("Energy: " + str(Metrics.get_energy_consumed(jobs, vms)))
        Log.print_line("Utilization: " + str(Metrics.get_utilization(jobs, vms)))
        Log.print_line("Costs: " + str(Metrics.get_cost(vms, jobs)))
        Log.print_line("==========================================================")