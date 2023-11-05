from __future__ import annotations

from workflowsim.CondorVM import CondorVM


class CustomVM(CondorVM):
    maxFreq: float = 0.0
    minFreq: float = 0.0
    maxVolt: float = 0.0
    minVolt: float = 0.0
    lambdaValue: float = 0.000015
    powerOn: bool = True

    def __init__(self, vm: CondorVM, cost: float=0.0, costPerMem: float=0.0, costPerStorage: float=0.0, costPerBw: float=0.0,
                minFreq: float=0.0, maxFreq: float=0.0, minVolt: float=0.0, maxVolt: float=0.0, lambdaValue: float=0.000015) -> None:
        super().__init__(vm.id, vm.userId, vm.mips, vm.numberOfPes, vm.ram, vm.bw, vm.size, vm.vmm, vm.cloudlet_scheduler, cost, costPerMem, costPerStorage, costPerBw)
        self.maxFreq: float = maxFreq
        self.minFreq: float = minFreq
        self.maxVolt: float = maxVolt
        self.minVolt: float = minVolt
        self.lambdaValue: float = lambdaValue
        self.powerOn: bool = True


    def set_freq_range(self, minFreq, maxFreq):
        self.minFreq = minFreq
        self.maxFreq = maxFreq

    
    def get_min_freq(self) -> float:
        return self.minFreq
    
    
    def get_max_freq(self) -> float:
        return self.maxFreq
    

    def set_volt_range(self, minVolt, maxVolt):
        self.minVolt = minVolt
        self.maxVolt = maxVolt


    def get_min_volt(self) -> float:
        return self.minVolt
    
    
    def get_max_volt(self) -> float:
        return self.maxVolt
    
    
    def shut_down(self):
        self.powerOn = False


    def turn_power_on(self):
        self.powerOn = True


    def is_power_on(self) -> bool:
        return self.powerOn
    
    
    def get_lambda(self, runningFreq: float) -> float:
        return self.lambdaValue * pow(10, (runningFreq-1)/(self.minFreq-1))