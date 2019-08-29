"""
A module consisting of pipeline steps that processed events will pass through.
"""

from abc import ABC, abstractmethod
from collections import deque, namedtuple
import logging
from time import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Pipeline(ABC):
    """
    An abstract base class for pipeline steps.
    """

    @abstractmethod
    def handle(self, events):
        """
        Transform the given stream of events into a processed stream of events.
        """
        pass

    def compose(self, other):
        """
        Compose this Pipeline with the other given Pipeline step.
        """
        return ComposedPipeline(self, other)

    def sink(self, sink):
        """
        Funnel events from this Pipeline into the given sink.
        """
        return PipelineWithSink(self, sink)


class ComposedPipeline(Pipeline):
    """
    A Pipeline consisting of the composition of two Pipeline steps.
    """

    def __init__(self, first, second):
        """
        Create the composition of the two given Pipeline steps.
        """
        self.first = first
        self.second = second

    def handle(self, events):
        """
        Handle the given event by having it be handled by the two pipelines in turn.
        """
        return self.second.handle(self.first.handle(events))


class PipelineWithSink(Pipeline):
    """
    A Pipeline with a final processing step (a Sink).
    """

    def __init__(self, pipeline, sink):
        """
        Create a Pipeline with a Sink.
        """
        self.pipeline = pipeline
        self.sink = sink

    def handle(self, events):
        """
        Handle events by first letting the pipeline process them, then 
        passing the result to the sink
        """
        self.sink.handle(self.pipeline.handle(events))


class FixedDurationSource(Pipeline):
    """
    A Pipeline step that processes events for a fixed duration.
    """

    def __init__(self, run_time_seconds):
        """
        Create a FixedDurationSource which will run for the given duration.
        """
        self.run_time_seconds = run_time_seconds
        self.events_processed = 0

    def handle(self, events):
        """
        Pass on all events from the source, but cut it off when the time limit is reached.
        """

        # Calculate the time at which we should stop processing
        end_time = time() + self.run_time_seconds
        logger.info(f'Processing events for {self.run_time_seconds} seconds')

        # Process events for as long as we still have time remaining
        for event in events:
            if time() < end_time:
                logger.debug(f'Procesing event: {event}')
                self.events_processed += 1
                yield event
            else:
                logger.info('Finished processing events')
                return


class LocationFilter(Pipeline):
    """
    A Pipeline step that filters out events which occur at unknown locations.
    """

    def __init__(self, valid_locations):
        """
        Create a LocationFilter which will use the given list of valid locations.
        """
        self.valid_location_ids = set(map(lambda loc: loc.id, valid_locations))
        self.invalid_events_filtered = 0

    def handle(self, events):
        """
        Pass on all events which occur at a known location.
        """
        for event in events:
            if event.location_id in self.valid_location_ids:
                yield event
            else:
                logger.debug(f'Ignoring event with unknown location ID: {event.location_id}')
                self.invalid_events_filtered += 1


class Deduplicator(Pipeline):
    """
    A Pipeline step that filters out duplicated events.
    """

    def __init__(self, cache_expiry_time):
        """
        Create a Deduplicator which will assume no more than the given time between duplicated events.
        """
        self.cache_expiry_time = cache_expiry_time
        self.expiry_queue = deque([])
        self.id_cache = set()
        self.duplicate_events_ignored = 0

    def handle(self, events):
        """
        Check for duplicates among the events and pass on all those which are not duplicated.
        """
        for event in events:

            # Expire old records
            processed_at_time = time()
            while len(self.expiry_queue) > 0 and processed_at_time > self.expiry_queue[0].expiry:
                logger.debug(f'Expiring deduplication record (Cache size: {len(self.id_cache)})')
                self.id_cache.remove(self.expiry_queue.popleft().id)

            # Check for duplicates
            if event.event_id in self.id_cache:
                logger.debug(f'Found duplicated event ID: {event.event_id}')
                self.duplicate_events_ignored += 1

            # Add an entry to the cache and process the event
            else:
                self.id_cache.add(event.event_id)
                self.expiry_queue.append(DeduplicationRecord(expiry=processed_at_time + self.cache_expiry_time, id=event.event_id))
                yield event


class DeduplicationRecord(namedtuple('DeduplicationRecord', 'expiry id')):
    """
    A record that is used to keep track of when IDs should be removed from the deduplication cache.
    """