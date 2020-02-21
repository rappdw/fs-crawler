from pathlib import Path
from typing import Dict, Tuple, Set

from .individual import Individual


class Graph:
    """ Graph of individuals in FamilySeearch
        :param fs: a Session object
    """

    def __init__(self, fs=None):
        self.fs = fs
        self.individuals:Dict[str, Individual] = dict()
        self.relationships:Dict[Tuple[str, str], str] = dict() # dictionary of (source, dest) to relationship type
        self.frontier:Set[str] = set()

    def add_to_frontier(self, fsid:str):
        if fsid not in self.individuals:
            self.frontier.add(fsid)

    def print_graph(self, out_dir:Path, basename:str):
        edges_filename = out_dir / f"{basename}.edges.csv"
        with edges_filename.open("w") as file:
            file.write('#source_vertex,#destination_vertex,type\n')
            for (src, dest), type in self.relationships.items():
                if src in self.individuals and dest in self.individuals:
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
