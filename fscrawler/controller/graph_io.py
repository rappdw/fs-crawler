from abc import ABC
from typing import Optional
from fscrawler.model.graph import Graph
from os.path import exists


class GraphIO(ABC):

    def __init__(self, out_dir, basename: str, graph: Optional[Graph]):
        self.out_dir = out_dir
        self.basename = basename
        self.edges_filename = out_dir / f"{basename}.edges.csv"
        self.vertices_filename = out_dir / f"{basename}.vertices.csv"
        self.spanning_edges_filename = out_dir / f"{basename}.spanning.edges.csv"
        self.frontier_vertices_filename = out_dir / f"{basename}.frontier.vertices.csv"
        self.frontier_edges_filename = out_dir / f"{basename}.frontier.edges.csv"
        self.graph = graph

    def exists(self):
        return exists(self.edges_filename) and \
               exists(self.vertices_filename) and \
               exists(self.spanning_edges_filename) and \
               exists(self.frontier_vertices_filename) and \
               exists(self.frontier_edges_filename)
