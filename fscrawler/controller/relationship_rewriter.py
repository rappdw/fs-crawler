import csv
from typing import Dict, Tuple
from fscrawler.model import Graph, RelationshipType
from .graph_io import GraphIO

# The type of relationships we will continue to store in the edges file, all other types will be in
# the aux.edges.csv file
REWRITE_REL_TYPES = {
    RelationshipType.UNTYPED_PARENT,
    RelationshipType.UNSPECIFIED_PARENT,
    RelationshipType.BIOLOGICAL_PARENT
}


class RelationshipReWriter(GraphIO):

    def __init__(self, out_dir, basename: str, graph: Graph,
                 relationships: Dict[str, Dict[str, Tuple[RelationshipType, str]]]):
        super().__init__(out_dir, basename, graph)
        self.relationships = relationships
        # copy original edges file to "orig.edges.csv"
        self.orig_edges_filename = out_dir / f"{basename}.orig.edges.csv"
        self.aux_edges_filename = out_dir / f"{basename}.aux.edges.csv"
        self._initialize_output()

    def _initialize_output(self):
        self.orig_edges_filename.write_text(self.edges_filename.read_text())
        with self.edges_filename.open("w") as file:
            writer = csv.writer(file)
            writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type', 'relationship_id'])
        if not self.aux_edges_filename.exists():
            with self.aux_edges_filename.open("w") as file:
                writer = csv.writer(file)
                writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type', 'relationship_id'])

    def rewrite_relationships(self) -> int:
        rels_moved_to_aux = 0
        with self.orig_edges_filename.open("r") as in_file, \
                self.edges_filename.open("a") as out_file, \
                self.aux_edges_filename.open("a") as aux_file:
            reader = csv.reader(in_file)
            writer = csv.writer(out_file)
            aux_writer = csv.writer(aux_file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                child_id = row[0]
                parent_id = row[1]
                rel_type = RelationshipType(row[2])
                rel_id = row[3]
                if parent_id in self.relationships[child_id]:
                    (rel_type, rel_id) = self.relationships[child_id][parent_id]
                if rel_type in REWRITE_REL_TYPES:
                    writer.writerow([child_id, parent_id, rel_type.value, rel_id])
                else:
                    aux_writer.writerow([child_id, parent_id, rel_type.value, rel_id])
                    rels_moved_to_aux += 1
        return rels_moved_to_aux
