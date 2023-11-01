from __future__ import annotations
import sys
sys.path.append("C:/Users/yuvrajeyes/Desktop/HEFT/")

from typing import List
from cloudsim.CloudletSchedulerSpaceShared import CloudletSchedulerSpaceShared
from workflowsim.CondorVM import CondorVM
from workflowsim.CustomVM import CustomVM


class CustomVMGenerator:
    @staticmethod
    def create_condor_vm(userId: int, vms: int) -> List[CondorVM]:
        # Creates a container to store VMs. This deque is passed to the broker later.
        vmList: List[CondorVM] = list()

        # VM parameters
        size: int = 10000  # image size (MB)
        ram: int = 512  # VM memory (MB)
        mips: int = 1000
        bw: int = 20
        pesNumber: int = 1  # number of CPUs
        vmm: str = "Xen"  # VMM name

        # Create VMs
        vm: List[CondorVM] = [None] * vms
        for i in range(vms):
            ratio = 0.5
            vm[i] = CondorVM(i, userId, mips * ratio, pesNumber, ram, int(bw), size, vmm, CloudletSchedulerSpaceShared())
            vmList.append(vm[i])
        return vmList
    

    @staticmethod
    def create_custom_vms(userId: int, vms: int) -> List[CondorVM]:
        vmList: List[CustomVM] = list()
        # first create regular condor vm
        list0: List[CondorVM] = CustomVMGenerator.create_condor_vm(userId, vms)
        vmType: int = 0
        for vm in list0:
            cvm: CustomVM = CustomVMGenerator.get_vm(vmType//5, vm)
            vmType += 1
            vmList.append(cvm)
        return vmList
    

    @staticmethod
    def get_vm(vmType: int, vm: CondorVM) -> CustomVM:
        pesNumber: int = 0
        memory: float = 0.0
        bandwidth: int = 0
        mips: float = 0.0
        cost: float = 0.0
        maxFreq: float = 0.0
        minFreq: float = 0.0
        minVoltage: float = 0.0
        maxVoltage: float = 0.0
        lambda_: float = 0.0
        costPerMem: float = 0.05  # the cost of using memory in this VM
        costPerStorage: float = 0.1  # the cost of using storage in this VM
        costPerBW: float = 0.1

        if vmType == 0:
            pesNumber = 2
            memory = 3.75  # GiB
            bandwidth = 500  # Mbps
            cost = 0.116
            mips = 1000
            maxFreq = 1.0
            minFreq = 0.50
            minVoltage = 5.0
            maxVoltage = 7.0
            lambda_ = 0.000150
        elif vmType == 1:
            pesNumber = 4
            memory = 7.5  # GiB
            bandwidth = 750  # Mbps
            cost = 0.232
            mips = 2000
            maxFreq = 2.0
            minFreq = 1.00
            minVoltage = 9.0
            maxVoltage = 11.0
            lambda_ = 0.000080
        elif vmType == 2:
            pesNumber = 8
            memory = 15.0  # GiB
            bandwidth = 1000  # Mbps
            cost = 0.464
            mips = 3000
            maxFreq = 3.0
            minFreq = 1.50
            minVoltage = 13.0
            maxVoltage = 15.0
            lambda_ = 0.000040
        elif vmType == 3:
            pesNumber = 36
            memory = 60.0  # GiB
            bandwidth = 4000  # Mbps
            cost = 1.856
            mips = 5000
            maxFreq = 5.0
            minFreq = 2.50
            minVoltage = 21.0
            maxVoltage = 23.0
            lambda_ = 0.000002
        else:
            pesNumber = 16
            memory = 30.0  # GiB
            bandwidth = 2000  # Mbps
            cost = 0.928
            mips = 4000
            maxFreq = 4.0
            minFreq = 2.00
            minVoltage = 17.0
            maxVoltage = 19.0
            lambda_ = 0.000010

        # Create a CustomVM object
        # maxVoltage = 30
        # minVoltage = 15
        cvm = CustomVM(vm, cost, costPerMem, costPerStorage, costPerBW, minFreq, maxFreq, minVoltage, maxVoltage, lambda_)
        
        return cvm