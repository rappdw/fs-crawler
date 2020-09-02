import csv
from fscrawler.model.graph import Graph, EdgeConditions, determine_edge_condition
from fscrawler.model.individual import Gender

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
                writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type'])
            with self.vertices_filename.open("w") as file:
                writer = csv.writer(file)
                writer.writerow(["#external_id", "color", "name", "iteration", "lifespan"])

    def _get_edge_condition(self, person_id1: str, person_id2: str, span_frontier: bool):
        p, r = self.graph.get_individual_info(person_id1)
        q, s = self.graph.get_individual_info(person_id2)
        t = self.graph.is_relationship_resolved(person_id1, person_id2)
        return determine_edge_condition(p, q, r, s, t, self.save_living, span_frontier)

    def write_iteration(self, span_frontier: bool):
        relationships = self.graph.get_relationships()
        individuals = self.graph.get_individuals()
        residual = dict()
        # in a given iteration, we can write relationships that are resolved (~UNTYPED_PARENT)
        # or for which both vertices are in the current iteration
        with self.edges_filename.open("a") as file:
            writer = csv.writer(file)
            for (src, dest), rel_type in relationships.items():
                edge_condition = self._get_edge_condition(src, dest, span_frontier)
                if edge_condition == EdgeConditions.writeable:
                    writer.writerow([src, dest, rel_type.value])
                elif not span_frontier and edge_condition == EdgeConditions.spanning:
                    residual[(src, dest)] = rel_type
        with self.vertices_filename.open("a") as file:
            writer = csv.writer(file)
            for person in individuals:
                if not person.living or self.save_living:
                    color = ''
                    if person.gender is Gender.Male:
                        color = -1
                    elif person.gender is Gender.Female:
                        color = 1
                    writer.writerow([person.fid, color, f"{person.name.surname}, {person.name.given}",
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
            writer.writerow(['#source_vertex', 'destination_vertex, relationship_type'])
            for (src, dest), rel_type in next_iter.items():
                writer.writerow([src, dest, rel_type.value])
        if not span_frontier:
            with self.residual_edges_filename.open("w") as file:
                writer = csv.writer(file)
                writer.writerow(['#source_vertex', 'destination_vertex, relationship_type'])
                for (src, dest), rel_type in residual.items():
                    writer.writerow([src, dest, rel_type.value])
