from pathlib import Path
from typing import Dict, Tuple, Set

from .individual import Individual
from .cp_validator import ChildParentRelationshipValidator
from .relationship_types import DEFAULT_PARENT_REL_TYPE, UNSPECIFIED_PARENT_REL_TYPE, BIOLOGICL_PARENT


REL_TYPES_TO_VALIDATE = [DEFAULT_PARENT_REL_TYPE, BIOLOGICL_PARENT, UNSPECIFIED_PARENT_REL_TYPE]

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

    def get_invalid_relationships(self) -> Set[str]:
        '''Based on relationship types, determine which relationships are invalid, e.g. a child
        with more than one father or more than one mother'''
        validation: Dict[str, Tuple[int, int]] = dict() # maps child to tuple of count of maie and female parents
        for count_tuple, rel_type in self.relationships.items():
            if rel_type in REL_TYPES_TO_VALIDATE:
                if not count_tuple[0] in validation:
                    validation[count_tuple[0]] = (0, 0)
                if count_tuple[1] in self.individuals:
                    gender = self.individuals[count_tuple[1]].gender
                    counts = validation[count_tuple[0]]
                    if gender in ['M', 'm']:
                        validation[count_tuple[0]] = (counts[0] + 1, counts[1])
                    else:
                        validation[count_tuple[0]] = (counts[0], counts[1] + 1)
        return {k for k, v in validation.items() if v[0] > 1 or v[1] > 1}


    def print_graph(self, out_dir:Path, basename:str):
        invalid_relationships = self.get_invalid_relationships()
        if invalid_relationships:
            edges_filename = out_dir / f"{basename}.invalid.edges.csv"
            with edges_filename.open("w") as file:
                file.write('#source_vertex,destination_vertex,type\n')
                for (src, dest), type in self.relationships.items():
                    if src in self.individuals and dest in self.individuals and src in invalid_relationships:
                        file.write(f"{src},{dest},{type}\n")
        edges_filename = out_dir / f"{basename}.edges.csv"
        with edges_filename.open("w") as file:
            file.write('#source_vertex,destination_vertex,type\n')
            for (src, dest), type in self.relationships.items():
                if src in self.individuals and dest in self.individuals and src not in invalid_relationships:
                    file.write(f"{src},{dest},{type}\n")
        vertices_filenam = out_dir / f"{basename}.vertices.csv"
        with vertices_filenam.open("w") as file:
            file.write("#external_id,color,name,hop\n")
            for fid in sorted(self.individuals, key=lambda x: self.individuals.__getitem__(x).num):
                person = self.individuals[fid]
                color = ''
                if person.gender in ['M', 'm']:
                    color = -1
                elif person.gender in ['F', 'f']:
                    color = 1
                file.write(f"{fid},{color},\"{person.name.surname}, {person.name.given}\",{person.hop}\n")
