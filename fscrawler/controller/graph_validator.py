import csv
from collections import defaultdict
from typing import DefaultDict, List, Dict, Tuple, Set, Generator
from .graph_io import GraphIO
from fscrawler.model.individual import Gender
from fscrawler.model.relationship_types import RelationshipType
from .graph_writer import CANONICAL_VERTEX_HEADER

# Relationship types that require further resolution prior to final validation
RELATIONSHIPS_RESOLUTIONS = [
    RelationshipType.UNTYPED_PARENT,
]

# Relationship types that if violations represent an invalid relationship
RELATIONSHIP_VALIDATIONS = [
    RelationshipType.BIOLOGICAL_PARENT,
    RelationshipType.UNSPECIFIED_PARENT
]

DEAD = 0
LIVING = 3000


def convert_lifespan_to_birth_year(lifespan: str) -> int:
    dash_idx = lifespan.find('-1')
    if dash_idx != -1:
        if dash_idx == 0:
            birth_year = int(lifespan[1:])
        else:
            birth_year = int(lifespan[:dash_idx])
    elif lifespan == 'Living':
        birth_year = LIVING
    else:
        birth_year = DEAD
    return birth_year


class GraphValidator(GraphIO):

    def __init__(self, out_dir, basename: str):
        super().__init__(out_dir, basename, None)
        self.invalid_relationships_filename = out_dir / f"{basename}.invalid.edges.csv"
        self.validated_vertices_filename = out_dir / f"{basename}.validated.vertices.csv"
        self.validated_edges_filename = out_dir / f"{basename}.validated.edges.csv"
        self.invalid_src = set()
        self.resolution_src = set()
        self.child_to_rel: Dict[str, Set[str]] = defaultdict(lambda: set())
        self.id_to_iteration_and_birth_year: Dict[str, Tuple[int, int]] = dict()
        self.child_count = 0
        self.vertex_count = 0
        self.unknown_vertex_count = 0
        self.edge_count = 0
        self.frontier_vertex_count = 0
        self.invalid_male_female_and_unknown_count = 0
        self.invalid_male_and_female_count = 0
        self.invalid_male_and_unknown_count = 0
        self.invalid_female_and_unknown_count = 0
        self.invalid_father_count = 0
        self.invalid_mother_count = 0
        self.invalid_unknown_count = 0
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
                g = row[1]
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
        rel_resolution_counts: DefaultDict[str, List[int]] = defaultdict(lambda: [0, 0, 0])
        rel_validation_counts: DefaultDict[str, List[int]] = defaultdict(lambda: [0, 0, 0])
        with self.edges_filename.open("r") as file:
            reader = csv.reader(file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                self.edge_count += 1
                child_id = row[0]
                parent_id = row[1]
                rel_type = RelationshipType(row[2])
                rel_id = row[3]
                self.child_to_rel[child_id].add(rel_id)
                if parent_id in gender_map:
                    parent_gender = gender_map[parent_id].value
                    if rel_type in RELATIONSHIP_VALIDATIONS:
                        counts = rel_validation_counts[child_id]
                        counts[parent_gender] += 1
                    if rel_type in RELATIONSHIPS_RESOLUTIONS:
                        counts = rel_resolution_counts[child_id]
                        counts[parent_gender] += 1
                else:
                    if parent_id not in frontier:
                        unknown_vertices.add(parent_id)

        self.child_count = len(rel_resolution_counts) + len(rel_validation_counts)
        self.unknown_vertex_count = len(unknown_vertices)
        del gender_map

        # calculate the invalid relationship counts
        for child_id, v in rel_validation_counts.items():
            if v[0] + v[1] + v[2] > 2:
                self.invalid_src.add(child_id)

                if v[Gender.Male.value] > 1:
                    self.max_father = max(self.max_father, v[Gender.Male.value])
                if v[Gender.Female.value] > 1:
                    self.max_mother = max(self.max_mother, v[Gender.Female.value])

                if v[Gender.Male.value] > 1 and v[Gender.Female.value] > 1 and v[Gender.Unknown.value] > 1:
                    self.invalid_male_female_and_unknown_count += 1
                elif v[Gender.Female.value] > 1 and v[Gender.Unknown.value] > 1:
                    self.invalid_female_and_unknown_count += 1
                elif v[Gender.Male.value] > 1 and v[Gender.Unknown.value] > 1:
                    self.invalid_male_and_unknown_count += 1
                elif v[Gender.Male.value] > 1 and v[Gender.Female.value] > 1:
                    self.invalid_male_and_female_count += 1
                elif v[Gender.Male.value] > 1:
                    self.invalid_father_count += 1
                elif v[Gender.Female.value] > 1:
                    self.invalid_mother_count += 1
                elif v[Gender.Unknown.value] > 1:
                    self.invalid_unknown_count += 1
                else:
                    self.invalid_unknown_count += 1

            if v[0] + v[1] + v[2] == 0:
                self.no_rel_count += 1
                self.invalid_src.add(child_id)

        # calculate the birth year histogram for the invalid relationships
        for child_id in self.invalid_src:
            (iteration, birth_year) = self.id_to_iteration_and_birth_year[child_id]
            self.by_histo[birth_year // 10 * 10] += 1

        # calculate the relationships requiring resolution
        for child_id, v in rel_resolution_counts.items():
            if v[0] > 1 or v[1] > 1 or v[2] > 1:
                self.resolution_src.add(child_id)

    def get_count_of_relationships_to_resolve(self) -> int:
        return len(self.resolution_src)

    def get_relationships_to_resolve(self) -> Generator[str, None, None]:
        for src_id in self.resolution_src:
            for rel_id in self.child_to_rel[src_id]:
                yield rel_id

    @staticmethod
    def get_year_string(birth_year: int):
        if birth_year == DEAD:
            return 'Dead'
        elif birth_year == LIVING:
            return 'Living'
        else:
            return birth_year

    def get_validation_histogram(self):
        # returns the number of invalid relationships in each iteration
        histo: DefaultDict[int, int] = defaultdict(int)
        for child_id in self.invalid_src:
            (iteration, _) = self.id_to_iteration_and_birth_year[child_id]
            histo[iteration] += 1
        return '\n'.join([f"Iteration {k}: {histo[k]}" for k in sorted(histo.keys())])

    def get_invalid_rel_count(self):
        return len(self.invalid_src)

    def save_valid_graph(self):
        id_to_number = dict()
        vertex_number = 1
        with self.vertices_filename.open("r") as in_file, self.validated_vertices_filename.open("w") as out_file:
            reader = csv.reader(in_file)
            writer = csv.writer(out_file)
            writer.writerow(CANONICAL_VERTEX_HEADER)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                id_to_number[row[0]] = vertex_number
                row.insert(0, str(vertex_number))
                writer.writerow(row)
                vertex_number += 1

        with self.edges_filename.open("r") as in_file, self.validated_edges_filename.open("w") as out_file:
            reader = csv.reader(in_file)
            writer = csv.writer(out_file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                src_id = row[0]
                dest_id = row[1]
                if src_id not in self.invalid_src:
                    writer.writerow([id_to_number[src_id], id_to_number[dest_id]])

        with self.invalid_relationships_filename.open("w") as file:
            writer = csv.writer(file)
            writer.writerow(['#source_vertex', 'relationship_id'])
            for child_id in self.invalid_src:
                for rel_id in self.child_to_rel[child_id]:
                    writer.writerow([child_id, rel_id])

    def get_validation_stats(self):
        histo = '\n'.join([f"{self.get_year_string(k)}: {self.by_histo[k]}" for k in sorted(self.by_histo.keys())])

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
               f"{self.no_rel_count:,} roots without relationships.\n" \
               f"{len(self.resolution_src):,} source nodes requiring relationship resolution.\n" \
               f"{len(self.invalid_src):,} source nodes with invalid relationships."
