import csv
from collections import defaultdict
from typing import DefaultDict, List, Dict, Set
from .graph_io import GraphIO
from fscrawler.model.individual import Gender
from fscrawler.model.relationship_types import RelationshipType

RELATIONSHIPS = [RelationshipType.BIOLOGICAL_PARENT, RelationshipType.UNTYPED_PARENT,
                 RelationshipType.UNSPECIFIED_PARENT]


def convert_lifespan_to_birth_year(lifespan: str) -> int:
    dash_idx = lifespan.find('-1')
    if dash_idx != -1:
        if dash_idx == 0:
            birth_year = int(lifespan[1:])
        else:
            birth_year = int(lifespan[:dash_idx])
    elif lifespan == 'Living':
        birth_year = 2020
    else:
        birth_year = 0  # Deceased
    return birth_year


class GraphReader(GraphIO):

    def __init__(self, out_dir, basename: str):
        super().__init__(out_dir, basename, None)
        self.id_to_iteration_and_birth_year: Dict[str, (int, int)] = dict()
        self.id_to_invalid_relationships: DefaultDict[str, (Set[str], Set[str])] = defaultdict(lambda : (set(), set()))
        self.child_count = 0
        self.vertex_count = 0
        self.unknown_vertex_count = 0
        self.edge_count = 0
        self.frontier_vertex_count = 0
        self.invalid_male_and_female_count = 0
        self.invalid_male_and_unknown_count = 0
        self.invalid_female_and_unknown_count = 0
        self.invalid_male_female_and_unknown_count = 0
        self.invalid_father_count = 0
        self.invalid_mother_count = 0
        self.invalid_unknown_count = 0
        self.invalid_rel_src_count = 0
        self.no_rel_count = 0
        self.max_father = 0
        self.max_mother = 0
        self.by_histo: DefaultDict[int, int] = defaultdict(int)

        self._calculate_stats()

    def _calculate_stats(self):
        # load the visited vertices by reading the vertices file
        gender_map: Dict[str, Gender] = dict()
        with self.vertices_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                self.vertex_count += 1
                fs_id = row[0]
                g = row[1] or 0  # for backward compatibility
                gender = Gender(int(g))
                gender_map[fs_id] = gender
                self.id_to_iteration_and_birth_year[fs_id] = (int(row[3]), convert_lifespan_to_birth_year(row[4]))

        # load vertices that are in the frontier
        frontier = set()
        with self.frontier_vertices_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                self.frontier_vertex_count += 1
                fs_id = row[0]
                frontier.add(fs_id)

        # load the visited relationships
        unknown_vertices = set()
        rel_counts: DefaultDict[str, List[int]] = defaultdict(lambda: [0, 0, 0])
        with self.edges_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                self.edge_count += 1
                child_id = row[0]
                rel_type = RelationshipType(row[2])
                parent_id = row[1]
                if rel_type in RELATIONSHIPS:
                    self.edge_count += 0
                    if parent_id in gender_map:
                        gender = gender_map[row[1]].value
                        counts = rel_counts[child_id]
                        counts[gender] += 1
                    else:
                        if parent_id not in frontier:
                            unknown_vertices.add(parent_id)

        self.child_count = len(rel_counts)
        self.unknown_vertex_count = len(unknown_vertices)
        del gender_map

        # calculate the invalid relationship counts
        invalid_src = set()
        for k, v in rel_counts.items():
            if v[0] > 1 or v[1] > 1 or v[2] > 1:
                self.invalid_rel_src_count += 1
                if v[Gender.Male.value] > 1 and v[Gender.Female.value] > 1 and v[Gender.Unknown.value] > 1:
                    self.invalid_male_female_and_unknown_count += 1
                    self.max_father = max(self.max_father, v[Gender.Male.value])
                    self.max_mother = max(self.max_mother, v[Gender.Female.value])
                elif v[Gender.Female.value] > 1 and v[Gender.Unknown.value] > 1:
                    self.invalid_female_and_unknown_count += 1
                    self.max_mother = max(self.max_mother, v[Gender.Female.value])
                elif v[Gender.Male.value] > 1 and v[Gender.Unknown.value] > 1:
                    self.invalid_male_and_unknown_count += 1
                    self.max_father = max(self.max_father, v[Gender.Male.value])
                elif v[Gender.Male.value] > 1 and v[Gender.Female.value] > 1:
                    self.invalid_male_and_female_count += 1
                    self.max_father = max(self.max_father, v[Gender.Male.value])
                    self.max_mother = max(self.max_mother, v[Gender.Female.value])
                elif v[Gender.Male.value] > 1:
                    self.invalid_father_count += 1
                    self.max_father = max(self.max_father, v[Gender.Male.value])
                elif v[Gender.Female.value] > 1:
                    self.invalid_mother_count += 1
                    self.max_mother = max(self.max_mother, v[Gender.Female.value])
                elif v[Gender.Unknown.value] > 1:
                    self.invalid_unknown_count += 1
                invalid_src.add(k)
            if v[0] + v[1] + v[2] == 0:
                self.no_rel_count += 1
                invalid_src.add(k)

        # calculate the birth year histogram for the invalid relationships
        with self.vertices_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                fs_id = row[0]
                if fs_id in invalid_src:
                    self.by_histo[convert_lifespan_to_birth_year(row[4]) // 10 * 10] += 1

    def get_validation_stats(self):
        histo = '\n'.join([f"{k or 'Dead'}: {self.by_histo[k]}" for k in sorted(self.by_histo.keys())])

        return f"Birth years for invalid counts: \n{histo}\n"\
               f"{self.vertex_count:,} total vertices.\n" \
               f"{self.edge_count:,} total edges.\n" \
               f"{self.unknown_vertex_count:,} total unknown vertex count.\n" \
               f"{self.child_count:,} total child count.\n" \
               f"{self.invalid_male_female_and_unknown_count:,} invalid father and mother and unknown count.\n" \
               f"{self.invalid_male_and_unknown_count:,} invalid father and unknown count.\n" \
               f"{self.invalid_female_and_unknown_count:,} invalid mother and unknown count.\n" \
               f"{self.invalid_male_and_female_count:,} invalid father and mother count.\n" \
               f"{self.invalid_father_count:,} invalid father count.\n" \
               f"{self.invalid_mother_count:,} invalid mother count.\n" \
               f"{self.invalid_unknown_count:,} invalid unknown count.\n" \
               f"{self.max_father:,} max father count.\n" \
               f"{self.max_mother:,} max mother count.\n" \
               f"{self.no_rel_count:,} roots without relationships"
