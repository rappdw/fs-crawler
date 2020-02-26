from typing import Dict, Set, Tuple
from .individual import Individual

class ChildParentRelationshipValidator:
    '''
    This relationship validator is used to identify relationships for which facts should be retrieved for
    further validation.

    There is strict validation (regardless of parent relationship cardinality) where relationship facts
    will be retrieved

    There is lazy validation. In this case unless parent relationship cardinality issues are present,
    relationship type is assumed to be BiologicalParent
    '''

    def __init__(self):
        # relationships is a dictionary keyed by childid. The value held in the dictionary is also
        # a dictionary which is keyed by relationship id with a value of the set of parentids
        self.relationships: Dict[str, Dict[str, Set[str]]] = dict()

    def add(self, child_id:str, parent_id:str, rel_id: str):
        if child_id not in self.relationships:
            self.relationships[child_id] = dict()
        relationship_dict = self.relationships[child_id]
        if rel_id not in relationship_dict:
            relationship_dict[rel_id] = set()
        relationship_dict[rel_id].add(parent_id)

    def get_relationships_to_validate(self, strict:bool, individuals:Dict[str, Individual]) -> Set[str]:
        relationships_to_validate = set()
        for rel_dict in self.relationships.values():
            father_count = 0
            mother_count = 0
            parents_counted = set()
            for rel_id, parent_set in rel_dict.items():
                if strict:
                    relationships_to_validate.add(rel_id)
                else:
                    for parent in parent_set:
                        if parent in individuals and parent not in parents_counted:
                            parents_counted.add(parent)
                            if individuals[parent].gender in ['M', 'm']:
                                father_count += 1
                            elif individuals[parent].gender in ['F', 'f']:
                                mother_count += 1
                    if father_count > 1 or mother_count > 1:
                        relationships_to_validate |= rel_dict.keys()
        return relationships_to_validate