import asyncio
import itertools
import logging
import time
import traceback

from math import ceil
from typing import Dict, Tuple
from urllib.parse import urlparse
from tqdm import tqdm
from .session import Session
from fscrawler.model.individual import Individual
from fscrawler.model.graph import Graph
from fscrawler.model.relationship_types import RelationshipType
from .graph_writer import GraphWriter

GET_PERSONS = "/platform/tree/persons/.json?pids="
RESOLVE_RELATIONSHIP = "/platform/tree/child-and-parents-relationships/"

# these constants are used to govern the load placed on the FS API
# max persons is subject to change: see https://www.familysearch.org/developers/docs/api/tree/Persons_resource
MAX_PERSONS = 200  # The maximum number of persons that will be in a get request for person information
MAX_CONCURRENT_PERSON_REQUESTS = 40  # the maximum number of concurrent requests that will be issued
MAX_CONCURRENT_RELATIONSHIP_REQUESTS = 200  # the maximum number of concurrent requests that will be issued
DELAY_BETWEEN_SUBSEQUENT_REQUESTS = 2 # the number of seconds to delay before issuing a subsequent block of requests
DELAY_BETWEEN_SUBSEQUENT_RELATIONSHIP_REQUESTS = 2  # the number of seconds to delay before issuing a subsequent block of requests

logger = logging.getLogger(__name__)
interesting_relationships_gedcomx_types = {"http://gedcomx.org/Couple", "http://gedcomx.org/ParentChild"}


def split_seq(iterable, size):
    it = iter(iterable)
    item = list(itertools.islice(it, size))
    while item:
        yield item
        item = list(itertools.islice(it, size))


def partition_requests(ids, exclusion=None, max_ids_per_request=MAX_PERSONS,
                       max_concurrent_requests=MAX_CONCURRENT_PERSON_REQUESTS):
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
                rel_type = RelationshipType(new_type)
        return rel_type

    async def get_relationships_from_id(self, resolved_relationships: Dict[str, Dict[str, Tuple[RelationshipType, str]]], rel_id):
        self.process_relationship_result(await self.session.get_urla(f"{RESOLVE_RELATIONSHIP}{rel_id}.json"), resolved_relationships)

    @staticmethod
    def _update_relationship_info(rel, child, parent, fact_key, rel_id,
                                  resolved_relationships: Dict[str, Dict[str, Tuple[RelationshipType, str]]]):
        if child and parent:
            relationship_type = FamilySearchAPI.get_relationship_type(rel, fact_key,
                                                                      RelationshipType.UNSPECIFIED_PARENT)
            resolved_relationships[child][parent] = (relationship_type, rel_id)
        else:
            logger.warning(f"Child: {child}, Parent: {parent}, Relationship: {rel_id}, value unexpected")

    @staticmethod
    def process_relationship_result(data, resolved_relationships: Dict[str, Dict[str, Tuple[RelationshipType, str]]]):
        data = FamilySearchAPI.check_error(data)
        if data and "childAndParentsRelationships" in data:
            for rel in data["childAndParentsRelationships"]:
                rel_id = rel["id"]
                parent1 = rel["parent1"]["resourceId"] if "parent1" in rel else None
                parent2 = rel["parent2"]["resourceId"] if "parent2" in rel else None
                child = rel["child"]["resourceId"] if "child" in rel else None
                if parent1:
                    FamilySearchAPI._update_relationship_info(rel, child, parent1, "parent1Facts", rel_id, resolved_relationships)
                if parent2:
                    FamilySearchAPI._update_relationship_info(rel, child, parent2, "parent2Facts", rel_id, resolved_relationships)

    @staticmethod
    def check_error(data):
        if data and "error" in data:
            try:
                # a number of "error" responses actually have data, if we can get data, do so...
                data = data["error"].json()
            except:
                pass
        return data

    async def get_persons_from_list(self, ids, graph, iteration):
        self.process_persons_result(await self.session.get_urla(GET_PERSONS + ",".join(ids)), graph, iteration)

    @staticmethod
    def _process_parent_child(key, data, graph, child, rel_id):
        if key in data:
            parent = data[key]["resourceId"]
            graph.add_to_frontier(parent)
            graph.add_parent_child_relationship(child, parent, rel_id)

    @staticmethod
    def process_persons_result(data, graph, iteration):
        data = FamilySearchAPI.check_error(data)
        if data:
            for person in data["persons"]:
                graph.add_individual(Individual(person, iteration))
            if 'relationships' in data:
                for relationship in data["relationships"]:
                    if relationship['type'] == "http://gedcomx.org/Couple":
                        graph.add_to_frontier(relationship['person1']['resourceId'])
                        graph.add_to_frontier(relationship['person2']['resourceId'])
            if "childAndParentsRelationships" in data:
                for relationship in data["childAndParentsRelationships"]:
                    rel_id = relationship["id"]
                    child = relationship["child"]["resourceId"]
                    graph.add_to_frontier(child)
                    FamilySearchAPI._process_parent_child("parent1", relationship, graph, child, rel_id)
                    FamilySearchAPI._process_parent_child("parent2", relationship, graph, child, rel_id)

    def resolve_relationships(self, resolved_relationships: Dict[str, Dict[str, Tuple[RelationshipType, str]]],
                              relationships, loop, delay=DELAY_BETWEEN_SUBSEQUENT_RELATIONSHIP_REQUESTS):
        """ resolve relationship types in the graph
            :param relationships: iterable relationship ids to resolve
            :param resolved_relationships: dictionary to record resolutions
            :param loop: asyncio event loop
            :param delay: delay to insert between successive concurrent get_persons requests
        """
        partitioned_requests = partition_requests(relationships, None, 1, MAX_CONCURRENT_RELATIONSHIP_REQUESTS)
        for requests in tqdm(partitioned_requests, total=ceil(len(relationships) / MAX_CONCURRENT_RELATIONSHIP_REQUESTS)):
            coroutines = [self.get_relationships_from_id(resolved_relationships, request) for request in requests]
            results = loop.run_until_complete(asyncio.gather(*coroutines, return_exceptions=True))
            for result in results:
                if result: # no return from get_relationships_from_id, so we have an exception
                    # we can tolerate exceptions during relationship resolution... just continue
                    # processing, but log a warning
                    logger.warning(traceback.format_exception(result))
                    return
            if delay:
                time.sleep(delay)

    def iterate(self, iteration: int, iteration_bound: int, graph: Graph, loop, writer: GraphWriter = None):
        final_iteration = iteration == iteration_bound - 1
        graph.iterate()

        logger.info(f"Starting iteration: {iteration}... ({len(graph.processing):,} individuals to process)")
        partitioned_requests = partition_requests(graph.get_ids_to_process(), graph.get_visited_individuals())
        for requests in tqdm(partitioned_requests, total=ceil(len(graph.processing) / MAX_CONCURRENT_PERSON_REQUESTS / MAX_PERSONS)):
            coroutines = [self.get_persons_from_list(request, graph, iteration) for request in requests]
            results = loop.run_until_complete(asyncio.gather(*coroutines, return_exceptions=True))
            for result in results:
                if result: # no return from get_relationships_from_id, so we have an exception
                    raise result
            time.sleep(DELAY_BETWEEN_SUBSEQUENT_REQUESTS)
        logger.info(f"\tFinished iteration: {iteration}. Graph stats: {graph.graph_stats()}")

        graph.end_iteration()
        if writer:
            writer.write_iteration(not final_iteration)
