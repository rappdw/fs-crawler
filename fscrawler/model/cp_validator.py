from typing import Dict, Set, Tuple

class ChildParentRelationshipValidator:

    def __init__(self):
        # this validator has a dictionary keyed by child fsid
        # the value is a tuple of sets
        # the first set is the fsids of all parents
        # the second set is the fsids of parent-child relationships
        self.relationships: Dict[str, Tuple[Set[str], Set[str]]] = dict()

    def add(self, child_id:str, parent_id:str, rel_id: str):
        if child_id not in self.relationships:
            self.relationships[child_id] = (set(), set())
        parents, relationships = self.relationships.get(child_id)
        parents.add(parent_id)
        relationships.add(rel_id)

    def get_relationships_to_validate(self) -> Set[str]:
        relationships_to_validate = set()
        for k, v in self.relationships.items():
            if len(v[0]) > 2:
                relationships_to_validate.update(v[1])
        return relationships_to_validate