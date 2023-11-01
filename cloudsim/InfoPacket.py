from __future__ import annotations

from typing import List, Optional
from decimal import Decimal
from cloudsim.core import CloudSimTags, CloudSim

class InfoPacket:
    def __init__(self, name: Optional[str], packet_id: int, size: int, src_id: int, dest_id: int, net_service_type: int) -> None:
        self.name: Optional[str] = name
        self.size: int = size
        self.packet_id: int = packet_id
        self.src_id: int = src_id
        self.dest_id: int = dest_id
        self.last: int = src_id
        self.tag: int = CloudSimTags.INFOPKT_SUBMIT
        self.hops_number: int = 0
        self.ping_size: int = size
        self.net_service_type: int = net_service_type
        self.bandwidth: float = -1
        self.entities: List[int] = []
        self.entry_times: List[float] = []
        self.exit_times: List[float] = []
        self.baud_rates: List[float] = []
        self.num: Decimal = Decimal("#0.000#")  # 4 decimal spaces

    def get_id(self) -> int:
        return self.packet_id

    def set_original_ping_size(self, size: int) -> None:
        self.ping_size = size

    def get_original_ping_size(self) -> int:
        return self.ping_size

    def __str__(self) -> str:
        if self.name is None:
            return "Empty InfoPacket that contains no ping information."

        SIZE: int = 1000  # number of chars
        sb: List[str] = []
        sb.append(f"Ping information for {self.name}\n")
        sb.append("Entity Name\tEntry Time\tExit Time\t Bandwidth\n")
        sb.append("----------------------------------------------------------\n")

        tab: str = "    "  # 4 spaces
        for i in range(len(self.entities)):
            res_id: int = self.entities[i]
            sb.append(f"{CloudSim.get_entity_name(res_id)}\t\t")

            entry: str = self.get_data(self.entry_times, i)
            exit: str = self.get_data(self.exit_times, i)
            bw: str = self.get_data(self.baud_rates, i)

            sb.append(f"{entry}{tab}{tab}{exit}{tab}{tab}{bw}\n")

        sb.append("\nRound Trip Time : {0} seconds".format(self.get_total_response_time()))
        sb.append("\nNumber of Hops  : {0}".format(self.get_num_hop()))
        sb.append("\nBottleneck Bandwidth : {0} bits/s".format(self.bandwidth))
        return ''.join(sb)

    def get_data(self, v: List[float], index: int) -> str:
        try:
            obj: float = v[index]
            id: float = obj
            return str(self.num.format(id))
        except Exception:
            return "    N/A"

    def get_size(self) -> int:
        return self.size

    def set_size(self, size: int) -> bool:
        if size >= 0:
            self.size = size
            return True
        else:
            return False

    def get_dest_id(self) -> int:
        return self.dest_id

    def get_src_id(self) -> int:
        return self.src_id

    def get_num_hop(self) -> int:
        PAIR: int = 2
        return ((self.hops_number - PAIR) + 1) // PAIR

    def get_total_response_time(self) -> float:
        if self.exit_times is None or self.entry_times is None:
            return 0

        try:
            start_time: float = self.exit_times[0]
            receive_time: float = self.entry_times[-1]
            return receive_time - start_time
        except Exception:
            return 0

    def get_baud_rate(self) -> float:
        return self.bandwidth

    def add_hop(self, id: int) -> None:
        self.hops_number += 1
        self.entities.append(id)

    def add_entry_time(self, time: float) -> None:
        if time < 0:
            time = 0.0
        self.entry_times.append(time)

    def add_exit_time(self, time: float) -> None:
        if time < 0:
            time = 0.0
        self.exit_times.append(time)

    def add_baud_rate(self, baud_rate: float) -> None:
        self.baud_rates.append(baud_rate)
        if self.bandwidth < 0 or baud_rate < self.bandwidth:
            self.bandwidth = baud_rate

    def get_detail_baud_rate(self) -> List[float]:
        if self.baud_rates is None:
            return []
        return self.baud_rates.copy()

    def get_detail_hops(self) -> List[int]:
        if self.entities is None:
            return []
        return self.entities.copy()

    def get_detail_entry_times(self) -> List[float]:
        if self.entry_times is None:
            return []
        return self.entry_times.copy()

    def get_detail_exit_times(self) -> List[float]:
        if self.exit_times is None:
            return []
        return self.exit_times.copy()

    def get_last(self) -> int:
        return self.last

    def set_last(self, last: int) -> None:
        self.last = last

    def get_net_service_type(self) -> int:
        return self.net_service_type

    def set_net_service_type(self, net_service_type: int) -> None:
        self.net_service_type = net_service_type

    def get_tag(self) -> int:
        return self.tag

    def set_tag(self, tag: int) -> bool:
        if tag in [CloudSimTags.INFOPKT_SUBMIT, CloudSimTags.INFOPKT_RETURN]:
            self.tag = tag
            return True
        else:
            return False

    def set_dest_id(self, id: int) -> None:
        self.dest_id = id