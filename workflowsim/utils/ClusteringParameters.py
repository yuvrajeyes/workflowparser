class ClusteringParameters:
    class ClusteringMethod:
        HORIZONTAL = "HORIZONTAL"
        VERTICAL = "VERTICAL"
        NONE = "NONE"
        BLOCK = "BLOCK"
        BALANCED = "BALANCED"

    def __init__(self, clusters_num: int, clusters_size: int, method: ClusteringMethod = ClusteringMethod.NONE, code: str = ""):
        self.clusters_num = clusters_num
        self.clusters_size = clusters_size
        self.method = method
        self.code = code

    def get_clusters_num(self) -> int:
        return self.clusters_num

    def get_clusters_size(self) -> int:
        return self.clusters_size

    def get_clustering_method(self) -> ClusteringMethod:
        return self.method

    def get_code(self) -> str:
        return self.code
