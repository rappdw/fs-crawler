from abc import ABC
from fscrawler.model.graph import Graph


class GraphIO(ABC):

    def __init__(self, out_dir, basename: str, graph: Graph):
        self.out_dir = out_dir
        self.basename = basename
        self.edges_filename = out_dir / f"{basename}.edges.csv"
        self.vertices_filename = out_dir / f"{basename}.vertices.csv"
        self.residual_edges_filename = out_dir / f"{basename}.residual.edges.csv"
        self.frontier_vertices_filename = out_dir / f"{basename}.frontier.vertices.csv"
        self.frontier_edges_filename = out_dir / f"{basename}.frontier.edges.csv"
        self.graph = graph
