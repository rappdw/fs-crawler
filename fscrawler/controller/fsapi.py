import asyncio
import logging
from urllib.parse import urlparse
from .session import Session
from fscrawler.model.individual import Individual
from fscrawler.model.graph import Graph
from fscrawler.model.cp_validator import ChildParentRelationshipValidator

# is subject to change: see https://www.familysearch.org/developers/docs/api/tree/Persons_resource
MAX_PERSONS = 200

PARENT_CHILD_RELATIONSHIP_TYPES = [
    "http://gedcomx.org/AdoptiveParent",        # A fact about an adoptive relationship between a parent and a child.
    "http://gedcomx.org/BiologicalParent",      # A fact about the biological relationship between a parent and a child.
    "http://gedcomx.org/FosterParent",          # A fact about a foster relationship between a foster parent and a child.
    "http://gedcomx.org/GuardianParent",        # A fact about a legal guardianship between a parent and a child.
    "http://gedcomx.org/StepParent",            # A fact about the step relationship between a parent and a child.
    "http://gedcomx.org/SociologicalParent",    # A fact about a sociological relationship between a parent and a child, but not definable in typical legal or biological terms.
    "http://gedcomx.org/SurrogateParent",       # A fact about a pregnancy surrogate relationship between a parent and a child.
]

DEFAULT_PARENT_REL_TYPE='UntypedParent'
DEFAULT_COUPLE_REL_TYPE='UntypedCouple'

logger = logging.getLogger(__name__)

class FamilySearchAPI:

    def __init__(self, username, password, verbose=False, logfile=False, timeout=60, resolve_parent_child=True):
        self.session = Session(username, password, verbose, logfile, timeout)
        self.resolve_parent_child = resolve_parent_child
        self.rel_set = set()
        self.cp_validator = ChildParentRelationshipValidator()

    def get_counter(self):
        return self.session.counter

    def get_defaul_starting_id(self):
        return self.session.fid

    def is_logged_in(self):
        return self.session.logged

    def _get_persons(self, fids):
        return self.session.get_url("/platform/tree/persons.json?pids=" + ",".join(fids))
    
    def get_relationship(self, rel_id):
        return self.session.get_url(f"/platform/tree/child-and-parents-relationships/{rel_id}.json")

    def get_relationship_type(self, rel, field, default):
        type = default
        if field in rel:
            for fact in rel[field]:
                new_type = urlparse(fact['type']).path.strip('/')
                if type != default and type != new_type:
                    logger.warning(f"Replacing fact: {type} with {new_type} for relationship id: {rel} ({field})")
                type = new_type
        return type

    def get_relationships_from_id(self, graph, rel_id):
        data = self.get_relationship(rel_id)
        if data and "childAndParentsRelationships" in data:
            for rel in data["childAndParentsRelationships"]:
                parent1 = rel["parent1"]["resourceId"] if "parent1" in rel else None
                parent2 = rel["parent2"]["resourceId"] if "parent2" in rel else None
                child = rel["child"]["resourceId"] if "child" in rel else None
                if child and parent1:
                    relationship_type = self.get_relationship_type(rel, "parent1Facts", DEFAULT_PARENT_REL_TYPE)
                    graph.relationships[(child, parent1)] = relationship_type if relationship_type else "http://gedcomx.org/BiologicalParent"
                if child and parent2:
                    relationship_type = self.get_relationship_type(rel, "parent2Facts", DEFAULT_PARENT_REL_TYPE)
                    graph.relationships[(child, parent2)] = relationship_type if relationship_type else "http://gedcomx.org/BiologicalParent"

    def add_individuals_to_graph(self, hopcount, graph, fids):
        """ add individuals to the family tree
            :param fids: an iterable of fid
        """

        async def parse_person_data(loop, data):
            futures = set()
            for relationship in parse_result_data(data):
                if (self.resolve_parent_child):
                    futures.add(loop.run_in_executor(None, self.get_relationships_from_id, graph, relationship))
            _ = await asyncio.gather(*futures)

        def parse_result_data(data):
            fetch_facts = set()
            for person in data["persons"]:
                working_on = graph.individuals[person["id"]] = Individual(person["id"])
                working_on.hop = hopcount
                working_on.add_data(person)
            if "relationships" in data:
                for relationship in data["relationships"]:
                    relationship_type = relationship["type"]
                    person1 = None
                    person2 = None
                    if relationship_type in ["http://gedcomx.org/Couple", "http://gedcomx.org/ParentChild"]:
                        person1 = relationship["person1"]["resourceId"]
                        person2 = relationship["person2"]["resourceId"]
                        graph.add_to_frontier(person1)
                        graph.add_to_frontier(person2)
                    if relationship_type == "http://gedcomx.org/Couple":
                        # we have the facts of the relationship already, no need to fetch them
                        relationship_type = self.get_relationship_type(relationship, "facts", DEFAULT_COUPLE_REL_TYPE)
                        graph.relationships[(
                        person1, person2)] = relationship_type if relationship_type else "http://gedcomx.org/Marriage"
                    elif relationship_type == "http://gedcomx.org/ParentChild":
                        rel_id = relationship["id"][2:]
                        parent = relationship["person1"]["resourceId"]
                        child = relationship["person2"]["resourceId"]
                        graph.relationships[(child, parent)] = DEFAULT_PARENT_REL_TYPE
                        self.cp_validator.add(child, parent, rel_id)
                    else:
                        logger.warning(f"Unknown relationship type: {relationship_type}")
                fetch_facts = self.cp_validator.get_relationships_to_validate()
            return fetch_facts

        total_count = len(fids)
        new_fids = [fid for fid in fids if fid and fid not in graph.individuals]
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        count = 0
        while new_fids:
            data = self._get_persons(new_fids[:MAX_PERSONS])
            count += MAX_PERSONS
            logger.info(f"Retrieved {count} of {total_count} individuals")
            if data:
                loop.run_until_complete(parse_person_data(loop, data))
            new_fids = new_fids[MAX_PERSONS:]

    def process_hop(self, hopcount: int, graph: Graph):
        todo = graph.frontier.copy()
        graph.frontier.clear()
        self.add_individuals_to_graph(hopcount, graph, todo)