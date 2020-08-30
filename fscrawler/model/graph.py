import csv
from pathlib import Path
from typing import Dict, Tuple, Set

from .individual import Individual, Gender
from .relationship_types import RelationshipType

REL_TYPES_TO_VALIDATE = {RelationshipType.UNTYPED_PARENT, RelationshipType.BIOLOGICAL_PARENT,
                         RelationshipType.UNSPECIFIED_PARENT}


class Graph:
    """ Graph of individuals in FamilySearch
    """

    def __init__(self):
        self.individuals: Dict[str, Individual] = dict()
        self.relationships: Dict[Tuple[str, str], str] = dict()  # dictionary of (source, dest) to relationship type
        self.frontier: Set[str] = set()

    def add_to_frontier(self, fs_id: str):
        if fs_id not in self.individuals:
            self.frontier.add(fs_id)

    def add_parent_child_relationship(self, child, parent, type: RelationshipType = RelationshipType.UNTYPED_PARENT):
        rel_key = (child, parent)
        if rel_key not in self.relationships:
            self.relationships[rel_key] = type

    def get_invalid_relationships(self) -> Tuple[Set[str], Set[str]]:
        """Based on relationship types, determine which relationships are invalid, e.g. a child
        with more than one father or more than one mother

        return - Tuple of two sets of invalid relationships. The first set is of parent child relationships
        where there is more than one parent of a certain gender. The second is a set of relationships that
        span the current frontier
        """
        frontier_relationships = set()
        validation: Dict[str, Tuple[int, int]] = dict()  # maps child to tuple of count of male and female parents
        for (person1, person2), rel_type in self.relationships.items():
            if rel_type in REL_TYPES_TO_VALIDATE:
                # in this case person1 is the child and person2 is the parent
                child = person1
                parent = person2
                if child not in validation:
                    validation[child] = (0, 0)
                if parent in self.individuals:
                    gender = self.individuals[parent].gender
                    counts = validation[child]
                    if gender is Gender.Male:
                        validation[child] = (counts[0] + 1, counts[1])
                    elif gender is Gender.Female:
                        validation[child] = (counts[0], counts[1] + 1)
                else:
                    frontier_relationships.add(child)
        return {k for k, v in validation.items() if v[0] > 1 or v[1] > 1}, frontier_relationships

    def write_ok(self, person_id: str, save_living: bool):
        return person_id in self.individuals and (save_living or not self.individuals[person_id].living)

    def print_graph(self, out_dir: Path, basename: str, save_living: bool = False):
        (invalid_parent_relationships, frontier_spanning_relationships) = self.get_invalid_relationships()
        if invalid_parent_relationships or frontier_spanning_relationships:
            edges_filename = out_dir / f"{basename}.invalid.edges.csv"
            with edges_filename.open("w") as file:
                writer = csv.writer(file)
                writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type', 'validity_reason'])
                for (src, dest), rel_tpe in self.relationships.items():
                    if self.write_ok(src, save_living) and self.write_ok(dest, save_living) and \
                            (src in invalid_parent_relationships or src in frontier_spanning_relationships):
                        writer.writerow([src, dest, rel_tpe,
                                         'frontier' if src in frontier_spanning_relationships
                                         else 'parent_cardinality'])
        edges_filename = out_dir / f"{basename}.edges.csv"
        with edges_filename.open("w") as file:
            writer = csv.writer(file)
            writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type'])
            for (src, dest), rel_tpe in self.relationships.items():
                if self.write_ok(src, save_living) and self.write_ok(dest, save_living) and \
                        (src not in invalid_parent_relationships and src not in frontier_spanning_relationships):
                    writer.writerow([src, dest, rel_tpe])
        vertices_filename = out_dir / f"{basename}.vertices.csv"
        with vertices_filename.open("w") as file:
            writer = csv.writer(file)
            writer.writerow(["#external_id", "color", "name", "hop"])
            for fid in sorted(self.individuals, key=lambda x: self.individuals.__getitem__(x).num):
                if self.write_ok(fid, save_living):
                    person = self.individuals[fid]
                    color = ''
                    if person.gender is Gender.Male:
                        color = -1
                    elif person.gender is Gender.Female:
                        color = 1
                    writer.writerow([fid, color, f"{person.name.surname}, {person.name.given}", person.hop])
