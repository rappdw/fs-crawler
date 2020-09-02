import csv
from .graph_io import GraphIO
from fscrawler.model.graph import Graph
from fscrawler.model.relationship_types import RelationshipType

class GraphReader(GraphIO):

    def __init__(self, out_dir, basename: str, graph: Graph):
        super().__init__(out_dir, basename, graph)
        self.max_iter = -1
        self.initialize_graph()

    def initialize_graph(self):
        # load the visited edges by reading the edges file
        with self.vertices_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                living = row[4].find('Living') != -1
                self.graph.add_visited_individual(row[0], living)
                self.max_iter = max(self.max_iter, int(row[3]))
        # load the visited relationships
        with self.edges_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                self.graph.add_visited_relationship((row[0], row[1]))
        with self.frontier_vertices_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                self.graph.add_to_frontier(row[0])
        with self.frontier_edges_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                self.graph.add_next_iter((row[0], row[1]), RelationshipType(row[2]))
        with self.residual_edges_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                self.graph.add_next_iter((row[0], row[1]), RelationshipType(row[2]))

    def get_max_iteration(self):
        return self.max_iter
