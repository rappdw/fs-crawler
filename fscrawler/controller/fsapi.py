import asyncio
import logging
import time
import traceback
from dataclasses import dataclass

from collections import namedtuple
from iteration_utilities import grouper
from math import ceil
from typing import Iterable, Optional
from urllib.parse import urlparse
from tqdm import tqdm
from .session import Session
from fscrawler.model.individual import Individual
from fscrawler.model.graph import Graph
from fscrawler.model.relationship_types import RelationshipType
from fscrawler.util.telemetry import TelemetryEmitter

GET_PERSONS = "/platform/tree/persons/.json?pids="
RESOLVE_RELATIONSHIP = "/platform/tree/child-and-parents-relationships/"

# these defaults are used to govern the load placed on the FS API. They can be overridden at runtime.


@dataclass(frozen=True)
class ThrottleConfig:
    person_batch_size: int = 200
    max_concurrent_person_requests: int = 40
    max_concurrent_relationship_requests: int = 200
    delay_between_person_batches: float = 2.0
    delay_between_relationship_batches: float = 2.0
    requests_per_second: float = 6.0
    max_retries: int = 5
    backoff_base_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    backoff_max_seconds: float = 60.0


# If there are more partitions than this, partial iterations will be written for each chunk of half of this value
PARTIAL_WRITE_THRESHOLD = 20

DEFAULT_THROTTLE = ThrottleConfig()

logger = logging.getLogger(__name__)
# noinspection HttpUrlsUsage
interesting_relationships_gedcomx_types = {"http://gedcomx.org/Couple", "http://gedcomx.org/ParentChild"}

PartitionedRequest = namedtuple("PartitionedRequest", "number_of_partitions iterator")


class StopRequested(Exception):
    """Raised to indicate that the crawl should stop gracefully."""
    pass


def partition_requests(ids: Iterable, count: int,
                       max_ids_per_request: int = DEFAULT_THROTTLE.person_batch_size,
                       max_concurrent_requests: int = DEFAULT_THROTTLE.max_concurrent_person_requests) -> PartitionedRequest:
    """
    Based on the maximum number of ids in a request, and the maximum number of
    concurrent requests allowed, split a 1d array into a 2d array of arrays where
    each cell is a set of ids to be requested in one call and each row is a group
    of requests that will run concurrently

    Parameters:
        ids (Iterable): an iterable object of the ids to partition
        count: the count of the ids that are being partitioned
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
        ceil(count / max_concurrent_requests / max_ids_per_request),
        grouper(grouped_ids, max_concurrent_requests)
    )


class FamilySearchAPI:

    def __init__(self, username, password, verbose=False, timeout=60,
                 throttle: Optional[ThrottleConfig] = None,
                 control: Optional[object] = None,
                 telemetry: Optional[TelemetryEmitter] = None):
        self.throttle = throttle or DEFAULT_THROTTLE
        self.control = control
        self.telemetry = telemetry
        self.session = Session(
            username,
            password,
            verbose,
            timeout,
            requests_per_second=self.throttle.requests_per_second,
            max_retries=self.throttle.max_retries,
            backoff_base_seconds=self.throttle.backoff_base_seconds,
            backoff_multiplier=self.throttle.backoff_multiplier,
            backoff_max_seconds=self.throttle.backoff_max_seconds,
        )
        self.rel_set = set()
        self._stop_exc = StopRequested

    def get_counter(self):
        return self.session.counter

    def get_default_starting_id(self):
        return self.session.fid

    def is_logged_in(self):
        return self.session.logged

    def _enforce_control(self, graph: Graph, iteration: Optional[int] = None):
        if not self.control:
            return
        if hasattr(self.control, "wait_if_paused"):
            self.control.wait_if_paused(logger, graph, iteration)
        if hasattr(self.control, "should_stop") and self.control.should_stop():
            reason = getattr(self.control, "stop_reason", None)
            raise self._stop_exc(reason or "Stop requested")

    def _emit(self, event: str, **fields):
        if self.telemetry:
            self.telemetry.emit(event, **fields)

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

    async def get_relationships_from_id(self, rel_id: str, graph: Graph):
        self.process_relationship_result(await self.session.get_urla(f"{RESOLVE_RELATIONSHIP}{rel_id}.json"), graph)

    @staticmethod
    def _update_relationship_info(rel, child, parent, fact_key, rel_id, graph: Graph):
        if child and parent:
            relationship_type = FamilySearchAPI.get_relationship_type(rel, fact_key,
                                                                      RelationshipType.UNSPECIFIED_PARENT)
            graph.update_relationship((child, parent), relationship_type)
        else:
            logger.warning(f"Child: {child}, Parent: {parent}, Relationship: {rel_id}, value unexpected")

    @staticmethod
    def process_relationship_result(data, graph: Graph):
        data = FamilySearchAPI.check_error(data)
        if data and "childAndParentsRelationships" in data:
            for rel in data["childAndParentsRelationships"]:
                rel_id = rel["id"]
                parent1 = rel["parent1"]["resourceId"] if "parent1" in rel else None
                parent2 = rel["parent2"]["resourceId"] if "parent2" in rel else None
                child = rel["child"]["resourceId"] if "child" in rel else None
                if parent1:
                    FamilySearchAPI._update_relationship_info(rel, child, parent1, "parent1Facts", rel_id, graph)
                if parent2:
                    FamilySearchAPI._update_relationship_info(rel, child, parent2, "parent2Facts", rel_id, graph)

    @staticmethod
    def check_error(data):
        if data and "error" in data:
            # noinspection PyBroadException
            try:
                # a number of "error" responses actually have data, if we can get data, do so...
                data = data["error"].json()
            except:
                pass
        return data

    async def get_persons_from_ids(self, ids, graph, iteration):
        self.process_persons_result(await self.session.get_urla(GET_PERSONS + ",".join(ids)), graph, iteration)

    @staticmethod
    def _process_parent_child(key, data, graph: Graph, child: str, rel_id: str):
        if key in data:
            parent = data[key]["resourceId"]
            graph.add_parent_child_relationship(child, parent, rel_id)

    @staticmethod
    def process_persons_result(data, graph: Graph, iteration):
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
                    FamilySearchAPI._process_parent_child("parent1", relationship, graph, child, rel_id)
                    FamilySearchAPI._process_parent_child("parent2", relationship, graph, child, rel_id)

    def _resolve_relationships(self, relationships: Iterable[str], relationship_count: int, graph: Graph, loop,
                               delay: Optional[float] = None):
        """
        Resolve relationship types in the graph

        Parameters:
            relationships: an iterable of relationship ids to resolve
            relationship_count: the number of relationships to resolve
            graph: graph that holds edges to resolve
            loop: asyncio event loop
            delay: delay between successive concurrent get_persons requests
        """
        partitioned_request = partition_requests(
            relationships,
            relationship_count,
            1,
            self.throttle.max_concurrent_relationship_requests,
        )
        batch_index = 0
        for requests in tqdm(partitioned_request.iterator, total=partitioned_request.number_of_partitions,
                             disable=partitioned_request.number_of_partitions == 1):
            self._enforce_control(graph, None)
            coroutines = [self.get_relationships_from_id(request, graph) for request in requests]
            batch_index += 1
            batch_start = time.time()
            results = loop.run_until_complete(asyncio.gather(*coroutines, return_exceptions=True))
            batch_duration = time.time() - batch_start
            for result in results:
                if result:
                    # no return from get_relationships_from_id, so we have an exception
                    # we can tolerate exceptions during relationship resolution... just continue
                    # processing, but log a warning
                    if isinstance(result, Exception):
                        logger.warning(traceback.format_exception(type(result), result, result.__traceback__))
                    else:
                        logger.warning(f"Returned unexpected result of type: {type(result)}. Value: {result}")
                    return
            effective_delay = self.throttle.delay_between_relationship_batches if delay is None else delay
            if effective_delay:
                time.sleep(effective_delay)
            self._enforce_control(graph, None)
            self._emit(
                "relationship_batch",
                batch=batch_index,
                batch_duration=batch_duration,
                batch_requests=len([req for req in requests if req]),
                session_counter=self.session.counter,
            )

    def iterate(self, iteration: int, graph: Graph, loop):
        graph.start_iteration(iteration)
        self._enforce_control(graph, iteration)

        start = time.time()
        iteration_start_counter = self.session.counter

        logger.info(f"Starting iteration: {iteration}... ({graph.get_processing_count():,} individuals to process)")
        partitioned_request = partition_requests(
            graph.get_ids_to_process(),
            graph.get_processing_count(),
            max_ids_per_request=self.throttle.person_batch_size,
            max_concurrent_requests=self.throttle.max_concurrent_person_requests,
        )
        iteration_count = 0
        batch_index = 0
        for requests in tqdm(partitioned_request.iterator, total=partitioned_request.number_of_partitions,
                             disable=partitioned_request.number_of_partitions == 1):
            iteration_count += 1
            batch_index += 1
            coroutines = [self.get_persons_from_ids(request, graph, iteration) for request in requests]
            batch_start = time.time()
            results = loop.run_until_complete(asyncio.gather(*coroutines, return_exceptions=True))
            batch_duration = time.time() - batch_start
            batch_request_count = sum(len([i for i in request if i]) for request in requests if request)
            for result in results:
                if result:
                    # no return from get_relationships_from_id, so we have an exception
                    if isinstance(result, Exception):
                        raise result
                    else:
                        logger.warning(f"Returned unexpected result of type: {type(result)}. Value: {result}")
            self._enforce_control(graph, iteration)
            if hasattr(graph, "checkpoint"):
                graph.checkpoint(iteration, "batch")
            if iteration_count > PARTIAL_WRITE_THRESHOLD:
                if hasattr(graph, "checkpoint"):
                    graph.checkpoint(iteration, "partial-write")
                iteration_count = 0
            else:
                if self.throttle.delay_between_person_batches:
                    time.sleep(self.throttle.delay_between_person_batches)
                self._enforce_control(graph, iteration)

            self._emit(
                "person_batch",
                iteration=iteration,
                batch=batch_index,
                batch_duration=batch_duration,
                batch_requests=batch_request_count,
                frontier=graph.get_frontier_count(),
                processing=graph.get_processing_count(),
                session_counter=self.session.counter,
            )

        duration = time.time() - start
        graph.end_iteration(iteration, duration)
        self._enforce_control(graph, iteration)
        self._emit(
            "iteration_complete",
            iteration=iteration,
            duration=duration,
            requests=self.session.counter - iteration_start_counter,
            frontier=graph.get_frontier_count(),
            processing=graph.get_processing_count(),
        )
        logger.info(f"\tFinished iteration: {iteration}. Duration: {duration:.2f} s. "
                    f"Graph stats: {graph.get_graph_stats()}")

    def resolve_relationships(self, graph: Graph, loop):
        start = time.time()

        relationships_to_resolve = graph.get_relationships_to_resolve()
        relationship_count = graph.get_count_of_relationships_to_resolve()

        if relationship_count > 0:
            logger.info(f"Resolving {relationship_count} relationships")
            self._resolve_relationships(
                relationships_to_resolve,
                relationship_count,
                graph,
                loop,
                delay=self.throttle.delay_between_relationship_batches,
            )
        duration = time.time() - start
        graph.end_relationship_resolution(relationship_count, duration)
        if hasattr(graph, "checkpoint"):
            graph.checkpoint(graph.starting_iter, "relationships")
        self._emit(
            "relationships_complete",
            count=relationship_count,
            duration=duration,
            session_counter=self.session.counter,
        )
        logger.info(f"\tFinished relationship resolution. Duration: {duration:.2f} s.")
