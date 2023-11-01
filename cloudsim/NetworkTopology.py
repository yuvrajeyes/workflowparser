from __future__ import annotations

from typing import Dict, List
from cloudsim.network import GraphReaderBrite
from cloudsim.network import TopologicalNode, TopologicalLink
from cloudsim.Log import Log

class NetworkTopology:
    nextIdx: int = 0
    networkEnabled: bool = False
    delayMatrix: List[List[float]] = []
    bwMatrix: List[List[float]] = []
    graph: List[List[float]] = []
    map: Dict[int, int] = {}

    @classmethod
    def build_network_topology(cls, fileName: str) -> None:
        Log.print_line("Topology file:", fileName)
        reader = GraphReaderBrite()

        try:
            cls.graph = reader.read_graph_file(fileName)
            cls.map = {}
            cls.generate_matrices()
        except IOError as e:
            Log.print_line("Problem in processing BRITE file. Network simulation is disabled. Error:", str(e))

    @classmethod
    def generate_matrices(cls) -> None:
        cls.delayMatrix = [[0.0 for _ in range(len(cls.graph))] for _ in range(len(cls.graph))]
        cls.bwMatrix = cls.create_bw_matrix(cls.graph, False)
        cls.networkEnabled = True

    @classmethod
    def add_link(cls, srcId: int, destId: int, bw: float, lat: float) -> None:
        if cls.graph is None:
            cls.graph = []

        if cls.map is None:
            cls.map = {}

        if srcId not in cls.map:
            cls.graph.append(TopologicalNode(cls.nextIdx))
            cls.map[srcId] = cls.nextIdx
            cls.nextIdx += 1

        if destId not in cls.map:
            cls.graph.append(TopologicalNode(cls.nextIdx))
            cls.map[destId] = cls.nextIdx
            cls.nextIdx += 1

        cls.graph.append(TopologicalLink(cls.map[srcId], cls.map[destId], float(lat), float(bw)))
        cls.generate_matrices()

    @classmethod
    def create_bw_matrix(cls, graph: List[List[float]], directed: bool) -> List[List[float]]:
        nodes = len(graph)
        mtx = [[0.0 for _ in range(nodes)] for _ in range(nodes)]

        for i in range(nodes):
            for j in range(nodes):
                mtx[i][j] = 0.0

        for edge in graph:
            mtx[edge[0]][edge[1]] = edge[3]

            if not directed:
                mtx[edge[1]][edge[0]] = edge[3]

        return mtx

    @classmethod
    def map_node(cls, cloudSimEntityID: int, briteID: int) -> None:
        if cls.networkEnabled:
            try:
                if cloudSimEntityID not in cls.map:
                    if briteID not in cls.map.values():
                        cls.map[cloudSimEntityID] = briteID
                    else:
                        Log.print_line("Error in network mapping. BRITE node", briteID, "already in use.")
                else:
                    Log.print_line("Error in network mapping. CloudSim entity", cloudSimEntityID, "already mapped.")
            except Exception as e:
                Log.print_line("Error in network mapping. CloudSim node", cloudSimEntityID, "not mapped to BRITE node", briteID, ".", str(e))

    @classmethod
    def unmap_node(cls, cloudSimEntityID: int) -> None:
        if cls.networkEnabled:
            try:
                cls.map.pop(cloudSimEntityID)
            except Exception as e:
                Log.print_line("Error in network unmapping. CloudSim node:", cloudSimEntityID, ".", str(e))

    @classmethod
    def get_delay(cls, srcID: int, destID: int) -> float:
        if cls.networkEnabled:
            try:
                delay = cls.delayMatrix[cls.map[srcID]][cls.map[destID]]
                return delay
            except Exception as e:
                pass
        return 0.0

    @classmethod
    def is_network_enabled(cls) -> bool:
        return cls.networkEnabled
