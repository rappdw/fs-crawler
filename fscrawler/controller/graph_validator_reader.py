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
        self.invalid_both_count = 0
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
        # load the visited edges by reading the edges file
        gender_map: Dict[str, Gender] = dict()
        with self.vertices_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                fs_id = row[0]
                g = row[1] or 0  # for backward compatibility
                gender = Gender(int(g))
                gender_map[fs_id] = gender
                self.id_to_iteration_and_birth_year[fs_id] = (int(row[3]), convert_lifespan_to_birth_year(row[4]))

        # load the visited relationships
        rel_counts: DefaultDict[str, List[int]] = defaultdict(lambda: [0, 0, 0])
        with self.edges_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                child_id = row[0]
                rel_type = RelationshipType(row[2])
                if rel_type in RELATIONSHIPS:
                    gender = gender_map[row[1]].value
                    counts = rel_counts[child_id]
                    counts[gender] += 1

        self.child_count = len(rel_counts)
        del gender_map

        # calculate the invalid relationship counts
        invalid_src = set()
        for k, v in rel_counts.items():
            if v[0] > 1 or v[1] > 1 or v[2] > 1:
                self.invalid_rel_src_count += 1
                if v[Gender.Male.value] > 1:
                    self.invalid_father_count += 1
                    self.max_father = max(self.max_father, v[Gender.Male.value])
                if v[Gender.Female.value] > 1:
                    self.invalid_mother_count += 1
                    self.max_mother = max(self.max_mother, v[Gender.Female.value])
                if v[Gender.Unknown.value] > 1:
                    self.invalid_unknown_count += 1
                if v[Gender.Male.value] > 1 and v[Gender.Female.value] > 1:
                    self.invalid_both_count += 1
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
               f"{self.child_count:,} total child count.\n" \
               f"{self.invalid_both_count:,} invalid father and mother count.\n" \
               f"{self.invalid_father_count - self.invalid_both_count:,} invalid father count.\n" \
               f"{self.invalid_mother_count - self.invalid_both_count:,} invalid mother count.\n" \
               f"{self.invalid_unknown_count:,} invalid unknown count.\n" \
               f"{self.max_father:,} max father count.\n" \
               f"{self.max_mother:,} max mother count.\n" \
               f"{self.no_rel_count:,} roots without relationships"
