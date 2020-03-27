import asyncio
import itertools
import logging
from urllib.parse import urlparse
from .session import Session
from fscrawler.model.individual import Individual
from fscrawler.model.graph import Graph

# is subject to change: see https://www.familysearch.org/developers/docs/api/tree/Persons_resource
from ..model.relationship_types import UNTYPED_PARENT, UNSPECIFIED_PARENT, UNTYPED_COUPLE

MAX_PERSONS = 200
MAX_CONCURRENT_REQUESTS = 20

logger = logging.getLogger(__name__)

def split_seq(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))

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

    def get_relationship_type(self, rel, field, default):
        type = default
        if field in rel:
            for fact in rel[field]:
                new_type = urlparse(fact['type']).path.strip('/')
                if type != default and type != new_type:
                    logger.warning(f"Replacing fact: {type} with {new_type} for relationship id: {rel['id']} ({field})")
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

    async def get_persons_from_list(self, ids, graph, hopcount):
        data = await self.session.get_urla("/platform/tree/persons/.json?pids=" + ",".join(ids))
        if data:
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

    def add_individuals_to_graph(self, hopcount, graph, fids, loop):
        """ add individuals to the family tree
            :param fids: an iterable of fid
        """
        new_fids = [fid for fid in fids if fid and fid not in graph.individuals]
        n = MAX_PERSONS
        final = [new_fids[i * n:(i + 1) * n] for i in range((len(new_fids) + n - 1) // n)]
        for group in split_seq(final, MAX_CONCURRENT_REQUESTS):
            coroutines = [self.get_persons_from_list(block, graph, hopcount) for block in group]
            loop.run_until_complete(asyncio.gather(*coroutines))

    def process_hop(self, hopcount: int, graph: Graph, loop):
        todo = graph.frontier.copy()
        graph.frontier.clear()
        self.add_individuals_to_graph(hopcount, graph, todo, loop)
        graph.frontier -= graph.individuals.keys()

    def resolve_relationships(self, graph, relationships, loop):
        new_rel_ids = [rel_id for rel_id in relationships]
        for group in split_seq(new_rel_ids, MAX_CONCURRENT_REQUESTS):
            coroutines = [self.get_relationships_from_id(graph, rel_id) for rel_id in group]
            loop.run_until_complete(asyncio.gather(*coroutines))

