from __future__ import annotations

from typing import List, Optional
import cloudsim.Vm as Vm
from cloudsim.Cloudlet import Cloudlet
from cloudsim.ResCloudlet import ResCloudlet
import cloudsim.Pe as Pe
from cloudsim.Log import Log


class HostList:
    @staticmethod
    def get_by_id(hostList: List[Vm.Host], hostId: int) -> Optional[Vm.Host]:
        for host in hostList:
            if host.get_id() == hostId:
                return host
        return None

    @staticmethod
    def get_number_of_pes(hostList: List[Vm.Host]) -> int:
        numberOfPes = 0
        for host in hostList:
            numberOfPes += len(host.get_pe_list())
        return numberOfPes

    @staticmethod
    def get_number_of_free_pes(hostList: List[Vm.Host]) -> int:
        numberOfFreePes = 0
        for host in hostList:
            numberOfFreePes += Vm.PeList.get_number_of_free_pes(host.get_pe_list())
        return numberOfFreePes

    @staticmethod
    def get_number_of_busy_pes(hostList: List[Vm.Host]) -> int:
        numberOfBusyPes = 0
        for host in hostList:
            numberOfBusyPes += Vm.PeList.get_number_of_busy_pes(host.get_pe_list())
        return numberOfBusyPes

    @staticmethod
    def get_host_with_free_Pe(hostList: List[Vm.Host]) -> Optional[Vm.Host]:
        return HostList.get_host_with_free_pe(hostList, 1)

    @staticmethod
    def get_host_with_free_pe(hostList: List[Vm.Host], pesNumber: int) -> Optional[Vm.Host]:
        for host in hostList:
            if Vm.PeList.get_number_of_free_pes(host.get_pe_list()) >= pesNumber:
                return host
        return None

    @staticmethod
    def set_pe_status(hostList: List[Vm.Host], status: int, hostId: int, peId: int) -> bool:
        host = HostList.get_by_id(hostList, hostId)
        if host is None:
            return False
        return host.set_pe_status(peId, status)
    

class VmList:
    @staticmethod
    def get_by_id(vmList: List[Vm.Vm], id: int) -> Vm.Vm:
        for vm in vmList:
            if (vm.get_id()==id):
                return vm        
        return None
    
    @staticmethod
    def get_by_id_and_user_id(vmList: List[Vm.Vm], id: int, userId: int) -> Vm.Vm:
        for vm in vmList:
            if (vm.get_id()==id and vm.get_user_id()==userId):
                return vm
        return None
    

class CloudletList:
    @staticmethod
    def get_by_id(cloudletList: List[Cloudlet], id: int) -> Cloudlet:
        for cloudlet in cloudletList:
            if (cloudlet.get_cloudlet_id()==id):
                return cloudlet        
        return None
    
    @staticmethod
    def get_by_id_and_user_id(cloudletList: List[Cloudlet], id: int) -> int:
        i: int = 0
        for cloudlet in cloudletList:
            if (cloudlet.get_cloudlet_id()==id):
                return i
            i += 1
        return -1
    
    @staticmethod
    def sort(cloudletList: List[Cloudlet]):
        cloudletList.sort(key=lambda cloudlet: cloudlet.get_cloudlet_total_length())


class ResCloudletList:
    @staticmethod
    def getByIdAndUserId(cloudletList: List[ResCloudlet], cloudletId: int, userId: int) -> ResCloudlet:
        for rcl in cloudletList:
            if rcl.get_cloudlet_id() == cloudletId and rcl.get_user_id() == userId:
                return rcl
        return None

    @staticmethod
    def indexOf(cloudletList: List[ResCloudlet], cloudletId: int, userId: int) -> int:
        for i, rcl in enumerate(cloudletList):
            if rcl.get_cloudlet_id() == cloudletId and rcl.get_user_id() == userId:
                return i
        return -1

    @staticmethod
    def move(listFrom: List[ResCloudlet], listTo: List[ResCloudlet], cloudlet: ResCloudlet) -> bool:
        if cloudlet in listFrom:
            listFrom.remove(cloudlet)
            listTo.append(cloudlet)
            return True
        return False

    @staticmethod
    def getPositionById(cloudletList: List[ResCloudlet], id: int) -> int:
        for i, cloudlet in enumerate(cloudletList):
            if cloudlet.get_cloudlet_id() == id:
                return i
        return -1


class PeList:
    @staticmethod
    def get_by_id(pe_list: List[Pe.Pe], id: int) -> Pe.Pe:
        for pe in pe_list:
            if pe.get_id() == id:
                return pe
        return None

    @staticmethod
    def get_mips(pe_list: List[Pe.Pe], id: int) -> int:
        pe: Pe.Pe = PeList.get_by_id(pe_list, id)
        if pe:
            return pe.get_mips()
        return -1

    @staticmethod
    def get_total_mips(pe_list: List[Pe.Pe]) -> int:
        total_mips: int = 0
        for pe in pe_list:
            total_mips += pe.get_mips()
        return total_mips

    @staticmethod
    def get_max_utilization(pe_list: List[Pe.Pe]) -> float:
        max_utilization: float = 0.0
        for pe in pe_list:
            utilization = pe.get_pe_provisioner().get_utilization()
            if utilization > max_utilization:
                max_utilization = utilization
        return max_utilization

    @staticmethod
    def get_max_utilization_among_vms_pes(pe_list: List[Pe.Pe], vm: Vm) -> float:
        max_utilization: float = 0.0
        for pe in pe_list:
            if pe.get_pe_provisioner().get_allocated_mips_for_vm(vm) is None:
                continue
            utilization = pe.get_pe_provisioner().get_utilization()
            if utilization > max_utilization:
                max_utilization = utilization
        return max_utilization

    @staticmethod
    def get_free_pe(pe_list: List[Pe.Pe]) -> Pe:
        for pe in pe_list:
            if pe.get_status() == Pe.Pe.FREE:
                return pe
        return None

    @staticmethod
    def get_number_of_free_pes(pe_list: List[Pe.Pe]) -> int:
        cnt: int = 0
        for pe in pe_list:
            if pe.get_status() == Pe.Pe.FREE:
                cnt += 1
        return cnt

    @staticmethod
    def set_pe_status(pe_list: List[Pe.Pe], id: int, status: int) -> bool:
        pe: Pe.Pe = PeList.get_by_id(pe_list, id)
        if pe:
            pe.set_status(status)
            return True
        return False

    @staticmethod
    def get_number_of_busy_pes(pe_list: List[Pe.Pe]) -> int:
        cnt: int = 0
        for pe in pe_list:
            if pe.get_status() == Pe.Pe.BUSY:
                cnt += 1
        return cnt

    @staticmethod
    def set_status_failed(pe_list: List[Pe.Pe], res_name: str, host_id: int, failed: bool)-> None:
        status = "FAILED" if failed else "WORKING"
        Log.print_line(f"{res_name} - Machine: {host_id} is {status}")
        PeList.set_status_failed_internal(pe_list, failed)

    @staticmethod
    def set_status_failed_internal(pe_list: List[Pe.Pe], failed: bool) -> None:
        for pe in pe_list:
            if failed:
                pe.set_status(Pe.Pe.FAILED)
            else:
                pe.set_status(Pe.Pe.FREE)