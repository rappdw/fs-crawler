import csv
from fscrawler.model import Graph, Individual, RelationshipType
from .graph_io import GraphIO

RELATIONSHIP_HEADER = ['#source_vertex', 'destination_vertex', 'relationship_type', 'relationship_id']
VERTEX_HEADER = ["#external_id", "color", "name", "iteration", "lifespan"]
CANONICAL_VERTEX_HEADER = ["vertex_number", "external_id", "color", "name", "iteration", "lifespan"]


class GraphWriter(GraphIO):

    def __init__(self, out_dir, basename: str, save_living: bool, graph: Graph, restart: bool):
        super().__init__(out_dir, basename, graph)
        self.log_filename = out_dir / f"{basename}.log.csv"
        self.temp_edges_filename = out_dir / f"{basename}.temp.edges.csv"
        self.save_living = save_living
        self._initialize_output(restart)

    def _initialize_output(self, restart: bool):
        if not restart:
            with self.edges_filename.open("w") as file, \
                    self.spanning_edges_filename.open("w") as span_file, \
                    self.frontier_edges_filename.open("w") as frontier_file:
                for f in [file, span_file, frontier_file]:
                    writer = csv.writer(f)
                    writer.writerow(RELATIONSHIP_HEADER)

            with self.vertices_filename.open("w") as file:
                writer = csv.writer(file)
                writer.writerow(VERTEX_HEADER)

    def log_iteration(self, iteration: int, duration: float):
        exists = self.log_filename.exists()
        with self.log_filename.open("a") as file:
            writer = csv.writer(file)
            if not exists:
                writer.writerow(
                    ['#iteration', 'duration', 'vertices', 'frontier', 'edges', 'spanning_edges', 'frontier_edges'])
            rel_counts = self.graph.get_relationship_count()
            writer.writerow([
                iteration,
                duration,
                self.graph.get_individual_count(),
                self.graph.get_frontier_count(),
                rel_counts.within,
                rel_counts.spanning,
                rel_counts.frontier
            ])

    def checkpoint_iteration(self, final_iteration: bool):
        # time.sleep(2)
        # self.end_iteration(final_iteration, True)
        pass

    def start_iteration(self):
        with self.temp_edges_filename.open("w") as file:
            writer = csv.writer(file)
            writer.writerow(RELATIONSHIP_HEADER)

    def write_individual(self, writer: csv.writer, person: Individual, clear_on_write: bool):
        writer.writerow([person.fid, person.gender.value, f"{person.name.surname}, {person.name.given}",
                         person.iteration, person.lifespan])
        if clear_on_write:
            # by convention when a relationship is written, the individual is set to None
            # graph is a GraphMemoryImpl graph and the function exists...
            # noinspection PyUnresolvedReferences
            self.graph.clear_individual(person.fid)

    def write_relationship(self, writer: csv.writer, src: str, dest: str, rel_type: RelationshipType, rel_id: str,
                           clear_on_write: bool):
        writer.writerow([src, dest, rel_type.value, rel_id])
        if clear_on_write:
            # by convention when a relationship is written, the rel_id is set to None
            # graph is a GraphMemoryImpl graph and the function exists...
            # noinspection PyUnresolvedReferences
            self.graph.clear_relationship((src, dest))

    def end_iteration(self, iteration: int, duration: float, final_iteration: bool, checkpoint: bool = False):
        self.log_iteration(iteration, duration)
        relationships = self.graph.get_relationships()
        individuals = self.graph.get_individuals()
        # in a given iteration, we write edges for which both vertices are in the current iteration
        with self.vertices_filename.open("a") as file:
            writer = csv.writer(file)
            for person in individuals:
                if not person.living or self.save_living:
                    self.write_individual(writer, person, True)
        with self.edges_filename.open("a") as in_file, \
                self.spanning_edges_filename.open("a") as span_file, \
                self.frontier_edges_filename.open("a") as frontier_file, \
                self.temp_edges_filename.open("a") as temp_file:
            in_writer = csv.writer(in_file)
            if final_iteration:
                span_writer = csv.writer(span_file)
                frontier_writer = csv.writer(frontier_file)
            elif checkpoint:
                span_writer = frontier_writer = csv.writer(temp_file)
            else:
                span_writer = frontier_writer = None
            for (src, dest), rel_id in relationships:
                clear_on_write = False
                src_in = self.graph.is_individual_in_graph(src)
                dest_in = self.graph.is_individual_in_graph(dest)
                if rel_id:
                    if src_in and dest_in:
                        writer = in_writer
                        clear_on_write = True
                    elif not src_in and not dest_in:
                        writer = frontier_writer
                    else:
                        writer = span_writer
                    if writer:
                        self.write_relationship(writer, src, dest, RelationshipType.UNTYPED_PARENT, rel_id,
                                                clear_on_write)

        if not checkpoint:
            frontier = self.graph.get_frontier()
            with self.frontier_vertices_filename.open("w") as file:
                writer = csv.writer(file)
                writer.writerow(["#external_id"])
                for fid in frontier:
                    writer.writerow([fid])
