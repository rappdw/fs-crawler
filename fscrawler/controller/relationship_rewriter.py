import csv
from typing import Dict, Tuple
from fscrawler.model.graph import Graph
from fscrawler.model.relationship_types import RelationshipType
from .graph_io import GraphIO


class RelationshipReWriter(GraphIO):

    def __init__(self, out_dir, basename: str, graph: Graph, relationships: Dict[str, Dict[str, Tuple[RelationshipType, str]]]):
        super().__init__(out_dir, basename, graph)
        self.relationships = relationships
        # copy original edges file to "orig.edges.csv"
        self.orig_edges_filename = out_dir / f"{basename}.orig.edges.csv"
        self._initialize_output()

    def _initialize_output(self):
        self.orig_edges_filename.write_text(self.edges_filename.read_text())
        with self.edges_filename.open("w") as file:
            writer = csv.writer(file)
            writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type', 'relationship_id'])

    def rewrite_relationships(self):
        with self.orig_edges_filename.open("r") as in_file, self.edges_filename.open("a") as out_file:
            reader = csv.reader(in_file)
            writer = csv.writer(out_file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                child_id = row[0]
                parent_id = row[1]
                rel_type = RelationshipType(row[2])
                rel_id = row[3]
                if parent_id in self.relationships[child_id]:
                    (rel_type, rel_id) = self.relationships[child_id][parent_id]
                writer.writerow([child_id, parent_id, rel_type.value, rel_id])

