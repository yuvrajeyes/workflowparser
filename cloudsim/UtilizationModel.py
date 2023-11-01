from __future__ import annotations
from abc import ABC


class UtilizationModel(ABC):
    def get_utilization(self, time: float) -> float:
        pass



class UtilizationModelFull(UtilizationModel):
    def get_utilization(self, time: float) -> float:
        return 1
    


class UtilizationModelNull(UtilizationModel):
    def get_utilization(self, time: float) -> float:
        return 0