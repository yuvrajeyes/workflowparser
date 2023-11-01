from __future__ import annotations

from typing import Final, List, Iterator
import math


class TopologicalLink:
    def __init__(self, srcNode: int, destNode: int, delay: float, bw: float):
        self.srcNodeID: int = srcNode
        self.destNodeID: int = destNode
        self.linkDelay: float = delay
        self.linkBw: float = bw

    def get_src_node_id(self) -> int:
        return self.srcNodeID

    def get_dest_node_id(self) -> int:
        return self.destNodeID

    def get_link_delay(self) -> float:
        return self.linkDelay

    def get_link_bw(self) -> float:
        return self.linkBw
    
class TopologicalNode:
    def __init__(self, nodeID: int):
        self.nodeID: int = nodeID
        self.nodeName: str = str(nodeID)
        self.worldX: int = 0
        self.worldY: int = 0

    def __init__(self, nodeID: int, x: int, y: int):
        self.nodeID: int = nodeID
        self.nodeName: str = str(nodeID)
        self.worldX: int = x
        self.worldY: int = y

    def __init__(self, nodeID: int, nodeName: str, x: int, y: int):
        self.nodeID: int = nodeID
        self.nodeName: str = nodeName
        self.worldX: int = x
        self.worldY: int = y

    def get_node_id(self) -> int:
        return self.nodeID

    def get_node_label(self) -> str:
        return self.nodeName

    def get_coordinate_x(self) -> int:
        return self.worldX

    def get_coordinate_y(self) -> int:
        return self.worldY


class TopologicalGraph:
    def __init__(self):
        self.linkList: List[TopologicalLink] = []
        self.nodeList: List[TopologicalNode] = []

    def add_link(self, edge: TopologicalLink):
        self.linkList.append(edge)

    def add_node(self, node: TopologicalNode):
        self.nodeList.append(node)

    def get_number_of_nodes(self) -> int:
        return len(self.nodeList)

    def get_number_of_links(self) -> int:
        return len(self.linkList)

    def get_link_iterator(self) -> Iterator[TopologicalLink]:
        return iter(self.linkList)

    def get_node_iterator(self) -> Iterator[TopologicalNode]:
        return iter(self.nodeList)

    def __str__(self):
        buffer = []
        buffer.append("topological-node-information: \n")

        for node in self.nodeList:
            buffer.append(f"{node.get_node_id()} | x is: {node.get_coordinate_x()} y is: {node.get_coordinate_y()}\n")

        buffer.append("\n\n node-link-information:\n")

        for link in self.linkList:
            buffer.append(f"from: {link.get_src_node_id()} to: {link.get_dest_node_id()} delay: {link.get_link_delay()}\n")

        return "".join(buffer)
    

class GraphReaderBrite:
    PARSE_NOTHING: Final[int] = 0
    PARSE_NODES: Final[int] = 1
    PARSE_EDGES: Final[int] = 2

    def __init__(self) -> None:
        self.state: int = GraphReaderBrite.PARSE_NOTHING
        self.graph = None

    def read_graph_file(self, filename: str) -> TopologicalGraph:
        self.graph = TopologicalGraph()

        with open(filename, 'r') as file:
            for line in file:
                self.process_line(line)

        return self.graph

    def process_line(self, line: str) -> None:
        if self.state == self.PARSE_NOTHING:
            if "Nodes:" in line:
                self.state = self.PARSE_NODES

        # the state to retrieve all node-information
        elif self.state == self.PARSE_NODES:
            if "Edges:" in line:
                self.state = self.PARSE_EDGES
            else:
                self.parse_node_string(line)
        # the state to retrieve all edges-information
        elif self.state == self.PARSE_EDGES:
            self.parse_edges_string(line)

    def parse_node_string(self, nodeLine: str) -> None:
        nodeParams = nodeLine.split()

        if not nodeParams:
            return

        nodeID = int(nodeParams[0])
        nodeLabel = str(nodeID)
        xPos = int(nodeParams[1])
        yPos = int(nodeParams[2])

        topoNode = TopologicalNode(nodeID, nodeLabel, xPos, yPos)
        self.graph.add_node(topoNode)

    def parse_edges_string(self, edgeLine: str) -> None:
        edgeParams = edgeLine.split()

        if not edgeParams:
            return

        fromNode = int(edgeParams[1])
        toNode = int(edgeParams[2])
        linkDelay = float(edgeParams[4])
        linkBandwidth = int(float(edgeParams[5]))

        self.graph.add_link(TopologicalLink(fromNode, toNode, linkDelay, linkBandwidth))


class FloydWarshall_Float:
    def __init__(self) -> None:
        self.numVertices: int = 0
        self.Dk: List[List[float]] = []
        self.Dk_minus_one: List[List[float]] = []
        self.Pk: List[List[int]] = []
        self.Pk_minus_one: List[List[int]] = []

    def initialize(self, numVertices: int) -> None:
        self.numVertices = numVertices

        self.Dk = [[0.0] * numVertices for _ in range(numVertices)]
        self.Dk_minus_one = [[0.0] * numVertices for _ in range(numVertices)]

        self.Pk = [[0] * numVertices for _ in range(numVertices)]
        self.Pk_minus_one = [[0] * numVertices for _ in range(numVertices)]

    def all_pairs_shortest_paths(self, adjMatrix: List[List[float]]) -> List[List[float]]:
        for i in range(self.numVertices):
            for j in range(self.numVertices):
                if adjMatrix[i][j] != 0:
                    self.Dk_minus_one[i][j] = adjMatrix[i][j]
                    self.Pk_minus_one[i][j] = i
                else:
                    self.Dk_minus_one[i][j] = float('inf')
                    self.Pk_minus_one[i][j] = -1

        for k in range(self.numVertices):
            for i in range(self.numVertices):
                for j in range(self.numVertices):
                    if i != j:
                        if self.Dk_minus_one[i][j] <= self.Dk_minus_one[i][k] + self.Dk_minus_one[k][j]:
                            self.Dk[i][j] = self.Dk_minus_one[i][j]
                            self.Pk[i][j] = self.Pk_minus_one[i][j]
                        else:
                            self.Dk[i][j] = self.Dk_minus_one[i][k] + self.Dk_minus_one[k][j]
                            self.Pk[i][j] = self.Pk_minus_one[k][j]
                    else:
                        self.Pk[i][j] = -1

            for i in range(self.numVertices):
                for j in range(self.numVertices):
                    self.Dk_minus_one[i][j] = self.Dk[i][j]
                    self.Pk_minus_one[i][j] = self.Pk[i][j]

        return self.Dk

    def get_Pk(self) -> List[List[int]]:
        return self.Pk


class DelayMatrix_Float:
    def __init__(self, graph: TopologicalGraph, directed: bool) -> None:
        self.mDelayMatrix: List[List[float]] = []
        self.mTotalNodeNum: int = 0
        self.create_delay_matrix(graph, directed)
        self.calculate_shortest_path()

    def get_delay(self, srcID: int, destID: int) -> float:
        if srcID > self.mTotalNodeNum or destID > self.mTotalNodeNum:
            raise ValueError("srcID or destID is higher than the highest stored node-ID!")
        return self.mDelayMatrix[srcID][destID]

    def create_delay_matrix(self, graph: TopologicalGraph, directed: bool) -> None:
        self.mTotalNodeNum = graph.get_number_of_nodes()
        self.mDelayMatrix = [[float('inf') for _ in range(self.mTotalNodeNum)] for _ in range(self.mTotalNodeNum)]
        
        for row in range(self.mTotalNodeNum):
            for col in range(self.mTotalNodeNum):
                self.mDelayMatrix[row][col] = float('inf')

        itr: Iterator[TopologicalLink] = graph.get_link_iterator()

        for edge in itr:
            self.mDelayMatrix[edge.get_src_node_id()][edge.get_dest_node_id()] = edge.get_link_delay()

            if not directed:
                self.mDelayMatrix[edge.get_dest_node_id()][edge.get_src_node_id()] = edge.get_link_delay()

    def calculate_shortest_path(self) -> None:
        floyd = FloydWarshall_Float()
        floyd.initialize(self.mTotalNodeNum)
        self.mDelayMatrix = floyd.all_pairs_shortest_paths(self.mDelayMatrix)

    def __str__(self) -> str:
        buffer = []

        buffer.append("just a simple printout of the distance-aware-topology-class")
        buffer.append("delay-matrix is:")

        for column in range(self.mTotalNodeNum):
            buffer.append("\t" + str(column))

        for row in range(self.mTotalNodeNum):
            buffer.append("\n" + str(row))

            for col in range(self.mTotalNodeNum):
                if math.isinf(self.mDelayMatrix[row][col]):
                    buffer.append("\t" + "-")
                else:
                    buffer.append("\t" + str(self.mDelayMatrix[row][col]))

        return '\n'.join(buffer)