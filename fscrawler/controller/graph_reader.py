import csv
from .graph_io import GraphIO
from fscrawler.model import Graph


class GraphReader(GraphIO):

    def __init__(self, out_dir, basename: str, graph: Graph):
        super().__init__(out_dir, basename, graph)
        self.max_iter = -1
        self._initialize_graph()

    def _initialize_graph(self):
        # load the visited edges by reading the edges file
        with self.vertices_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                # living = row[4].find('Living') != -1
                self.graph.add_visited_individual(row[0])
                self.max_iter = max(self.max_iter, int(row[3]))
        with self.frontier_vertices_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                self.graph.add_to_frontier(row[0])
        # load the relationships
        with self.edges_filename.open("r") as file:
            self.load_relationships(file)
        with self.frontier_edges_filename.open("r") as file:
            self.load_relationships(file)
        with self.spanning_edges_filename.open("r") as file:
            self.load_relationships(file)

    def load_relationships(self, file):
        reader = csv.reader(file)
        for row in reader:
            if row[0].startswith('#'):
                continue
            self.graph.add_parent_child_relationship(row[0], row[1], row[3])

    def get_max_iteration(self):
        return self.max_iter
