import asyncio
import itertools
import logging
import time
from urllib.parse import urlparse
from .session import Session
from fscrawler.model.individual import Individual
from fscrawler.model.graph import Graph

# is subject to change: see https://www.familysearch.org/developers/docs/api/tree/Persons_resource
from ..model.relationship_types import UNTYPED_PARENT, UNSPECIFIED_PARENT, UNTYPED_COUPLE

GET_PERSONS = "/platform/tree/persons/.json?pids="
RESOLVE_RELATIONSHIP = "/platform/tree/child-and-parents-relationships/"

# these constants are used to govern the load placed on the FS API
MAX_PERSONS = 200  # The maximum number of persons that will be in a get request for person information
MAX_CONCURRENT_REQUESTS = 20  # the maximum number of concurrent requests that will be issued
DELAY_BETWEEN_SUBSEQUENT_REQUESTS = 2  # the number of seconds to delay before issuing a subsequent block of requests

logger = logging.getLogger(__name__)


def split_seq(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))


def partition_requests(ids, exclusion=None, max_ids_per_request=MAX_PERSONS,
                       max_concurrent_requests=MAX_CONCURRENT_REQUESTS):
    """
    Based on the maximum number of persons in a request, and the maximum number of
    concurrent requests allowed, split a 1d array into a 2d array of arrays where
    each cell is a set of ids to be requested in one call and each row is a group
    of requests that will run concurrently
    """
    if exclusion is None:
        exclusion = {}
    ids = [req_id for req_id in ids if req_id is not None and req_id not in exclusion]
    if max_ids_per_request > 1:
        final = [ids[i * max_ids_per_request:(i + 1) * max_ids_per_request]
                 for i in range((len(ids) + max_ids_per_request - 1) // max_ids_per_request)]
    else:
        final = ids
    return split_seq(final, max_concurrent_requests)


class FamilySearchAPI:

    def __init__(self, username, password, verbose=False, timeout=60):
        self.session = Session(username, password, verbose, timeout)
        self.rel_set = set()

    def get_counter(self):
        return self.session.counter

    def get_default_starting_id(self):
        return self.session.fid

    def is_logged_in(self):
        return self.session.logged

    @staticmethod
    def get_relationship_type(rel, field, default):
        rel_type = default
        if field in rel:
            for fact in rel[field]:
                new_type = urlparse(fact['type']).path.strip('/')
                if rel_type != default and rel_type != new_type:
                    logger.debug(f"Replacing fact: {rel_type} with {new_type} "
                                 f"for relationship id: {rel['id']} ({field})")
                rel_type = new_type
        return rel_type

    async def get_relationships_from_id(self, graph, rel_id):
        self.process_relationship_result(await self.session.get_urla(f"{RESOLVE_RELATIONSHIP}{rel_id}.json"), graph)

    def process_relationship_result(self, data, graph):
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

    async def get_persons_from_list(self, ids, graph, hop_count):
        self.process_persons_result(await self.session.get_urla(GET_PERSONS + ",".join(ids)), graph, hop_count)

    def process_persons_result(self, data, graph, hop_count):
        if data:
            for person in data["persons"]:
                working_on = graph.individuals[person["id"]] = Individual(person["id"])
                working_on.hop = hop_count
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

    def add_individuals_to_graph(self, hop_count, graph, ids, loop, delay=DELAY_BETWEEN_SUBSEQUENT_REQUESTS):
        """ add individuals to the graph
            :param ids: the ids to get person records for
            :param hop_count: upper bound on how many relationships from seed individual(s) should be traversed
            :param graph: graph object that is constructed through fs api requests
            :param loop: asyncio event loop
            :param delay: delay to insert between successive concurrent get_persons requests
        """
        for requests in partition_requests(ids, graph.individuals):
            coroutines = [self.get_persons_from_list(request, graph, hop_count) for request in requests]
            loop.run_until_complete(asyncio.gather(*coroutines))
            if delay:
                time.sleep(delay)

    def resolve_relationships(self, graph, relationships, loop, delay=DELAY_BETWEEN_SUBSEQUENT_REQUESTS):
        """ resolve relationship types in the graph
            :param relationships: iterable relationship ids to resolve
            :param graph: graph object that is constructed through fs api requests
            :param loop: asyncio event loop
            :param delay: delay to insert between successive concurrent get_persons requests
        """
        for requests in partition_requests(relationships, None, 1):
            coroutines = [self.get_relationships_from_id(graph, request) for request in requests]
            loop.run_until_complete(asyncio.gather(*coroutines))
            if delay:
                time.sleep(delay)

    def process_hop(self, hop_count: int, graph: Graph, loop, strict_resolve: bool = False):
        logger.info(f"Starting hop: {hop_count}... ({len(graph.frontier):,} individuals in hop)")
        todo = graph.frontier.copy()
        graph.frontier.clear()
        self.add_individuals_to_graph(hop_count, graph, todo, loop)
        graph.frontier -= graph.individuals.keys()

        relationships_to_validate = graph.get_relationships_to_validate(strict_resolve)
        logger.info(f"\tValidating {len(relationships_to_validate):,} relationships...")
        self.resolve_relationships(graph, relationships_to_validate, loop)

        logger.info(f"\tFinished hop: {hop_count}. Graph stats: {len(graph.individuals):,} persons, "
                    f"{len(graph.relationships):,} relationships, {len(graph.frontier):,} frontier")
