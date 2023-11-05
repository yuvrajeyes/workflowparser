from typing import List, Dict, Union
from cloudsim.Cloudlet import Cloudlet
from workflowsim.utils.DistributionGenerator import DistributionGenerator
from workflowsim.Job import Job

class OverheadParameters:
    def __init__(self, wed_interval: int, wed_delay: Dict[int, DistributionGenerator],
                 queue_delay: Dict[int, DistributionGenerator], post_delay: Dict[int, DistributionGenerator],
                 cluster_delay: Dict[int, DistributionGenerator], bandwidth: float):
        self.WED_INTERVAL: int = wed_interval
        self.WED_DELAY: Dict[int, DistributionGenerator] = wed_delay
        self.QUEUE_DELAY: Dict[int, DistributionGenerator] = queue_delay
        self.POST_DELAY: Dict[int, DistributionGenerator] = post_delay
        self.CLUST_DELAY: Dict[int, DistributionGenerator] = cluster_delay
        self.bandwidth: float = bandwidth

    def get_bandwidth(self) -> float:
        return self.bandwidth

    def get_wed_interval(self) -> int:
        return self.WED_INTERVAL
    
    def get_queue_delay(self, cl: Cloudlet=None) -> Union[float, Dict[int, DistributionGenerator]]:
        if cl is None:
            return self.QUEUE_DELAY
        delay: float = 0.0
        if self.QUEUE_DELAY is None:
            return delay
        if cl is not None:
            job: Job = cl
            if job.get_depth() in self.QUEUE_DELAY:
                delay = self.QUEUE_DELAY[job.get_depth()].get_next_sample()
            elif 0 in self.QUEUE_DELAY:
                delay = self.QUEUE_DELAY[0].get_next_sample()
            else:
                delay = 0.0
        else:
            print("Not yet supported")
        return delay
    
    def get_post_delay(self, job: Job=None) -> Union[float, Dict[int, DistributionGenerator]]:
        if job is None:
            return self.POST_DELAY
        delay: float = 0.0
        if self.POST_DELAY is None:
            return delay
        if job is not None:
            if job.get_depth() in self.POST_DELAY:
                delay = self.POST_DELAY[job.get_depth()].get_next_sample()
            elif 0 in self.POST_DELAY:
                delay = self.POST_DELAY[0].get_next_sample()
            else:
                delay = 0.0
        else:
            print("Not yet supported")
        return delay

    def get_wed_delay(self, job_list: List[Job]=None) -> Union[float, Dict[int, DistributionGenerator]]:
        if job_list is None:
            return self.WED_DELAY
        delay: float = 0.0
        if self.WED_DELAY is None:
            return delay
        if job_list:
            job = job_list[0]
            if job.get_depth() in self.WED_DELAY:
                delay = self.WED_DELAY[job.get_depth()].get_next_sample()
            elif 0 in self.WED_DELAY:
                delay = self.WED_DELAY[0].get_next_sample()
            else:
                delay = 0.0
        else:
            # Actually, set it to be 0.0;
            # print("Not yet supported")
            pass
        return delay
    
    def get_clust_delay(self, cl: Cloudlet=None) -> Union[float, Dict[int, DistributionGenerator]]:
        if cl is None:
            return self.CLUST_DELAY
        delay: float = 0.0
        if self.CLUST_DELAY is None:
            return delay
        if cl is not None:
            job: Job = cl

            if job.get_depth() in self.CLUST_DELAY:
                delay = self.CLUST_DELAY[job.get_depth()].get_next_sample()
            elif 0 in self.CLUST_DELAY:
                delay = self.CLUST_DELAY[0].get_next_sample()
            else:
                delay = 0.0
        else:
            print("Not yet supported")
        return delay