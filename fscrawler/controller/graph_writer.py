import csv
from fscrawler.model.graph import Graph, EdgeConditions

from .graph_io import GraphIO


class GraphWriter(GraphIO):

    def __init__(self, out_dir, basename: str, save_living: bool, graph: Graph, restart: bool):
        super().__init__(out_dir, basename, graph)
        self.save_living = save_living
        self._initialize_output(restart)

    def _initialize_output(self, restart: bool):
        if not restart:
            with self.edges_filename.open("w") as file:
                writer = csv.writer(file)
                writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type', 'relationship_id'])
            with self.vertices_filename.open("w") as file:
                writer = csv.writer(file)
                writer.writerow(["#external_id", "color", "name", "iteration", "lifespan"])

    def write_iteration(self, span_frontier: bool):
        relationships = self.graph.get_relationships()
        individuals = self.graph.get_individuals()
        residual = dict()
        # in a given iteration, we write edges for which both vertices are in the current iteration
        with self.edges_filename.open("a") as file:
            writer = csv.writer(file)
            for src, dest_dict in relationships.items():
                for dest, (rel_type, rel_id) in dest_dict.items():
                    edge_condition = self.graph._get_edge_condition(src, dest, self.save_living, span_frontier)
                    if edge_condition == EdgeConditions.writeable:
                        writer.writerow([src, dest, rel_type.value, rel_id])
                    elif not span_frontier and edge_condition in [EdgeConditions.spanning, EdgeConditions.spanning_and_unresolved]:
                        residual[(src, dest)] = (rel_type, rel_id)
        with self.vertices_filename.open("a") as file:
            writer = csv.writer(file)
            for person in individuals:
                if not person.living or self.save_living:
                    writer.writerow([person.fid, person.gender.value, f"{person.name.surname}, {person.name.given}",
                                     person.iteration, person.lifespan])

        frontier = self.graph.get_frontier()
        next_iter = self.graph.get_next_iter_relationships()
        with self.frontier_vertices_filename.open("w") as file:
            writer = csv.writer(file)
            writer.writerow(["#external_id"])
            for fid in frontier:
                writer.writerow([fid])
        with self.frontier_edges_filename.open("w") as file:
            writer = csv.writer(file)
            writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type', 'relationship_id'])
            for src, dest_dict in next_iter.items():
                for dest, (rel_type, rel_id) in dest_dict.items():
                    writer.writerow([src, dest, rel_type.value, rel_id])
        if not span_frontier:
            with self.residual_edges_filename.open("w") as file:
                writer = csv.writer(file)
                writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type', 'relationship_id'])
                for (src, dest), (rel_type, rel_id) in residual.items():
                    writer.writerow([src, dest, rel_type.value, rel_id])
