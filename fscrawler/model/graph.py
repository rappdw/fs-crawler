import csv
from pathlib import Path
from typing import Dict, Tuple, Set

from .individual import Individual
from .cp_validator import ChildParentRelationshipValidator
from .relationship_types import UNTYPED_PARENT, UNSPECIFIED_PARENT, BIOLOGICAL_PARENT


REL_TYPES_TO_VALIDATE = [UNTYPED_PARENT, BIOLOGICAL_PARENT, UNSPECIFIED_PARENT]

class Graph:
    """ Graph of individuals in FamilySeearch
        :param fs: a Session object
    """

    def __init__(self, fs=None):
        self.fs = fs
        self.individuals:Dict[str, Individual] = dict()
        self.relationships:Dict[Tuple[str, str], str] = dict() # dictionary of (source, dest) to relationship type
        self.frontier:Set[str] = set()
        self.cp_validator = ChildParentRelationshipValidator()

    def add_to_frontier(self, fsid:str):
        if fsid not in self.individuals:
            self.frontier.add(fsid)

    def get_relationships_to_validate(self, strict:bool = False):
        '''Use cp_validator to return list of relationships to use for retrieving facts (to resolve from
        UntypedParent to one of the specified relationship types'''
        return self.cp_validator.get_relationships_to_validate(strict, self.individuals)

    def get_invalid_relationships(self) -> Tuple[Set[str], Set[str]]:
        '''Based on relationship types, determine which relationships are invalid, e.g. a child
        with more than one father or more than one mother

        return - Tuple of two sets of invalid relationships. The first set is of parent child relationships
        where there is more than one parent of a certain gender. The second is a set of relationships that
        span the current frontier
        '''
        frontier_relationships = set()
        validation: Dict[str, Tuple[int, int]] = dict() # maps child to tuple of count of male and female parents
        for (person1, person2), rel_type in self.relationships.items():
            if rel_type in REL_TYPES_TO_VALIDATE:
                # in this case person1 is the child and person2 is the parent
                child = person1
                parent = person2
                if not child in validation:
                    validation[child] = (0, 0)
                if parent in self.individuals:
                    gender = self.individuals[parent].gender
                    counts = validation[child]
                    if gender in ['M', 'm']:
                        validation[child] = (counts[0] + 1, counts[1])
                    else:
                        validation[child] = (counts[0], counts[1] + 1)
                else:
                    frontier_relationships.add(child)
        return ({k for k, v in validation.items() if v[0] > 1 or v[1] > 1}, frontier_relationships)


    def print_graph(self, out_dir:Path, basename:str):
        (invalid_parent_relationships, frontier_spanning_relationships) = self.get_invalid_relationships()
        if invalid_parent_relationships or frontier_spanning_relationships:
            edges_filename = out_dir / f"{basename}.invalid.edges.csv"
            with edges_filename.open("w") as file:
                writer = csv.writer(file)
                writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type', 'validity_reason'])
                for (src, dest), type in self.relationships.items():
                    if src in self.individuals and \
                            dest in self.individuals and \
                            (src in invalid_parent_relationships or src in frontier_spanning_relationships):
                        writer.writerow([src, dest, type,
                                         'frontier' if src in frontier_spanning_relationships else 'parent_cardinality'])
        edges_filename = out_dir / f"{basename}.edges.csv"
        with edges_filename.open("w") as file:
            writer = csv.writer(file)
            writer.writerow(['#source_vertex', 'destination_vertex', 'relationship_type'])
            for (src, dest), type in self.relationships.items():
                if src in self.individuals and \
                        dest in self.individuals and \
                        (src not in invalid_parent_relationships and src not in frontier_spanning_relationships):
                    writer.writerow([src, dest, type])
        vertices_filenam = out_dir / f"{basename}.vertices.csv"
        with vertices_filenam.open("w") as file:
            writer = csv.writer(file)
            writer.writerow(["#external_id", "color", "name", "hop"])
            for fid in sorted(self.individuals, key=lambda x: self.individuals.__getitem__(x).num):
                person = self.individuals[fid]
                color = ''
                if person.gender in ['M', 'm']:
                    color = -1
                elif person.gender in ['F', 'f']:
                    color = 1
                writer.writerow([fid, color, f"{person.name.surname}, {person.name.given}", person.hop])
