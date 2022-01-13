import asyncio
import logging
import time
import traceback

from collections import namedtuple
from iteration_utilities import grouper
from math import ceil
from typing import Dict, Tuple, Set
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

# The maximum number of persons that will be in a get request for person information
MAX_PERSONS = 200
# the maximum number of concurrent requests that will be issued
MAX_CONCURRENT_PERSON_REQUESTS = 40
# the maximum number of concurrent requests that will be issued
MAX_CONCURRENT_RELATIONSHIP_REQUESTS = 200
# the number of seconds to delay before issuing a subsequent block of requests
DELAY_BETWEEN_SUBSEQUENT_REQUESTS = 2
# the number of seconds to delay before issuing a subsequent block of requests
DELAY_BETWEEN_SUBSEQUENT_RELATIONSHIP_REQUESTS = 2
# If there are more partitions than this, partial iterations will be written for each chunk of half of this value
PARTIAL_WRITE_THRESHOLD = 20

logger = logging.getLogger(__name__)
interesting_relationships_gedcomx_types = {"http://gedcomx.org/Couple", "http://gedcomx.org/ParentChild"}

PartitionedRequest = namedtuple("PartionedRequest", "number_of_partitions iterator")


def partition_requests(ids: Set, max_ids_per_request: int = MAX_PERSONS,
                       max_concurrent_requests: int = MAX_CONCURRENT_PERSON_REQUESTS) -> PartitionedRequest:
    """
    Based on the maximum number of persons in a request, and the maximum number of
    concurrent requests allowed, split a 1d array into a 2d array of arrays where
    each cell is a set of ids to be requested in one call and each row is a group
    of requests that will run concurrently

    Parameters:
        ids (Set): the set of ids to partion into requests
        max_ids_per_request (int): the maximum number of ids in an individual request
        max_concurrent_requests (int): the maximum number of concurrent requests

    Returns:
        partitioned_request (PartitionedRequest): A tuple that holds a count of the number of partitions and an
        iterator that iterates over the partitioning
    """
    if max_ids_per_request > 1:
        grouped_ids = grouper(ids, max_ids_per_request)
    else:
        grouped_ids = ids
    return PartitionedRequest(
        ceil(len(ids) / max_concurrent_requests / max_ids_per_request),
        grouper(grouped_ids, max_concurrent_requests)
    )


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

    async def get_relationships_from_id(self, resolved_relationships: Dict[str, Dict[str, Tuple[RelationshipType, str]]], rel_id: str):
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

    async def get_persons_from_ids(self, ids, graph, iteration):
        self.process_persons_result(await self.session.get_urla(GET_PERSONS + ",".join(ids)), graph, iteration)

    @staticmethod
    def _process_parent_child(key, data, graph, child, rel_id):
        if key in data:
            parent = data[key]["resourceId"]
            graph.add_to_frontier(child)
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
                              relationships:Set, loop, delay=DELAY_BETWEEN_SUBSEQUENT_RELATIONSHIP_REQUESTS):
        """
        Resolve relationship types in the graph

        Parameters:
            relationships: set of relationship ids to resolve
            resolved_relationships: dictionary to record resolutions
            loop: asyncio event loop
            delay: delay to insert between successive concurrent get_persons requests
        """
        partitioned_request = partition_requests(relationships, 1, MAX_CONCURRENT_RELATIONSHIP_REQUESTS)
        for requests in tqdm(partitioned_request.iterator, total=partitioned_request.number_of_partitions, disable=partitioned_request.number_of_partitions==1):
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

    def iterate(self, iteration: int, iteration_bound: int, graph: Graph, loop, writer: GraphWriter):
        final_iteration = iteration == iteration_bound - 1
        graph.start_iteration()
        writer.start_iteration()

        start = time.time()

        logger.info(f"Starting iteration: {iteration}... ({len(graph.processing):,} individuals to process)")
        partitioned_request = partition_requests(graph.get_ids_to_process())
        iteration_count = 0
        for requests in tqdm(partitioned_request.iterator, total=partitioned_request.number_of_partitions, disable=partitioned_request.number_of_partitions==1):
            iteration_count += 1
            coroutines = [self.get_persons_from_ids(request, graph, iteration) for request in requests]
            results = loop.run_until_complete(asyncio.gather(*coroutines, return_exceptions=True))
            for result in results:
                if result: # no return from get_relationships_from_id, so we have an exception
                    raise result
            if iteration_count > PARTIAL_WRITE_THRESHOLD:
                iteration_count = 0
                writer.checkpoint_iteration(not final_iteration)
            else:
                time.sleep(DELAY_BETWEEN_SUBSEQUENT_REQUESTS)
        duration = time.time() - start
        logger.info(f"\tFinished iteration: {iteration}. Duration: {duration:.2f} s. Graph stats: {graph.graph_stats()}")
        writer.end_iteration(iteration, duration, final_iteration)
