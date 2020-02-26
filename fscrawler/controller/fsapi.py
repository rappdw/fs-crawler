import asyncio
import logging
from urllib.parse import urlparse
from .session import Session
from fscrawler.model.individual import Individual
from fscrawler.model.graph import Graph

# is subject to change: see https://www.familysearch.org/developers/docs/api/tree/Persons_resource
from ..model.relationship_types import UNTYPED_PARENT, UNSPECIFIED_PARENT, UNTYPED_COUPLE

MAX_PERSONS = 200
MAX_CONCURRENT_RELATIONSHIP_REQUESTS = 200

logger = logging.getLogger(__name__)

class FamilySearchAPI:

    def __init__(self, username, password, verbose=False, timeout=60):
        self.session = Session(username, password, verbose, timeout)
        self.rel_set = set()

    def get_counter(self):
        return self.session.counter

    def get_defaul_starting_id(self):
        return self.session.fid

    def is_logged_in(self):
        return self.session.logged

    def _get_persons(self, fids):
        return self.session.get_url("/platform/tree/persons.json?pids=" + ",".join(fids))
    
    def get_relationship_type(self, rel, field, default):
        type = default
        if field in rel:
            for fact in rel[field]:
                new_type = urlparse(fact['type']).path.strip('/')
                if type != default and type != new_type:
                    logger.warning(f"Replacing fact: {type} with {new_type} for relationship id: {rel} ({field})")
                type = new_type
        return type

    async def get_relationships_from_id(self, graph, rel_id):
        data = await self.session.get_urla(f"/platform/tree/child-and-parents-relationships/{rel_id}.json")
        if data and "childAndParentsRelationships" in data:
            for rel in data["childAndParentsRelationships"]:
                parent1 = rel["parent1"]["resourceId"] if "parent1" in rel else None
                parent2 = rel["parent2"]["resourceId"] if "parent2" in rel else None
                child = rel["child"]["resourceId"] if "child" in rel else None
                if child and parent1:
                    relationship_type = self.get_relationship_type(rel, "parent1Facts", UNSPECIFIED_PARENT)
                    graph.relationships[(child, parent1)] = relationship_type
                if child and parent2:
                    relationship_type = self.get_relationship_type(rel, "parent2Facts", UNSPECIFIED_PARENT)
                    graph.relationships[(child, parent2)] = relationship_type

    def add_individuals_to_graph(self, hopcount, graph, fids):
        """ add individuals to the family tree
            :param fids: an iterable of fid
        """

        def parse_result_data(data):
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
                        relationship_type = self.get_relationship_type(relationship, "facts", UNTYPED_COUPLE)
                        graph.relationships[(person1, person2)] = relationship_type
                    elif relationship_type == "http://gedcomx.org/ParentChild":
                        rel_id = relationship["id"][2:]
                        parent = relationship["person1"]["resourceId"]
                        child = relationship["person2"]["resourceId"]
                        graph.relationships[(child, parent)] = UNTYPED_PARENT
                        graph.cp_validator.add(child, parent, rel_id)
                    else:
                        logger.warning(f"Unknown relationship type: {relationship_type}")

        total_count = len(fids)
        new_fids = [fid for fid in fids if fid and fid not in graph.individuals]
        count = 0
        while new_fids:
            data = self._get_persons(new_fids[:MAX_PERSONS])
            count += MAX_PERSONS
            logger.info(f"Retrieved {count} of {total_count} individuals")
            if data:
                parse_result_data(data)
            new_fids = new_fids[MAX_PERSONS:]

    def process_hop(self, hopcount: int, graph: Graph):
        todo = graph.frontier.copy()
        graph.frontier.clear()
        self.add_individuals_to_graph(hopcount, graph, todo)

    def resolve_relationships(self, graph, relationships):
        new_rel_ids = [rel_id for rel_id in relationships]
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        while new_rel_ids:
            coroutines = [self.get_relationships_from_id(graph, rel_id) for idx, rel_id in enumerate(new_rel_ids) if idx < MAX_CONCURRENT_RELATIONSHIP_REQUESTS]
            loop.run_until_complete(asyncio.gather(*coroutines))
            new_rel_ids = new_rel_ids[MAX_CONCURRENT_RELATIONSHIP_REQUESTS:]

