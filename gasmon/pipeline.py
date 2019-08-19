"""
A module consisting of pipeline steps that processed events will pass through.
"""

from abc import ABC, abstractmethod
from collections import deque, namedtuple
from datetime import datetime
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


class Averager(Pipeline):
    """
    A Pipeline step that produces a moving average of events in a given window.
    """

    def __init__(self, average_period_seconds, expiry_time_seconds):
        """
        Create an Averager which will calculate a moving average over windows of width
        `average_period_seconds`, under the assumption that events may arrive up to
        `expiry_time_seconds` late.
        """

        # Store configuration values
        self.average_period_millis = 1000 * average_period_seconds
        self.expiry_time_millis = 1000 * expiry_time_seconds

        # Create buckets for calculating the moving averages
        current_time_millis = 1000 * time()
        self.buckets = deque([AverageBucket(start=(current_time_millis - self.expiry_time_millis), end=(current_time_millis - self.expiry_time_millis + self.average_period_millis), values=[])])

    def handle(self, events):
        """
        For each event to be processed, add its value to an appropriate bucket. If any bucket
        has expired, then yield its average and remove it from consideration.
        """
        for event in events:
            self.add_to_bucket(event)
            expired_bucket = self.maybe_expire_first_bucket_and_get_average()
            if expired_bucket is not None:
                logger.info(f'Average value for {expired_bucket.start} to {expired_bucket.end} is {expired_bucket.average}')
                yield expired_bucket.average

    def add_to_bucket(self, event):
        """
        Find the bucket that the given event belongs in, and place it there.
        """

        # Check if the event is old and should be ignored
        if self.buckets[0].start > event.timestamp:
            logger.debug(f'Not averaging old event at timestamp {event.timestamp}')
            return

        # Check if we need to add any new buckets to deal with this event
        while self.buckets[-1].end < event.timestamp:
            current_last_start, current_last_end = self.buckets[-1].start, self.buckets[-1].end
            logger.debug(f'Adding new bucket to deal with event at timestamp {event.timestamp} (Current last bucket is {current_last_start} to {current_last_end})')
            self.buckets.append(AverageBucket(start=current_last_end, end=(current_last_end + self.average_period_millis), values=[]))

        # Find the right bucket and add the event value
        bucket_index = int(event.timestamp - self.buckets[0].start) // self.average_period_millis
        self.buckets[bucket_index].values.append(event.value)

    def maybe_expire_first_bucket_and_get_average(self):
        current_time_millis = 1000 * time()
        if current_time_millis - self.expiry_time_millis > self.buckets[0].end:
            return self.buckets.popleft()


class DeduplicationRecord(namedtuple('DeduplicationRecord', 'expiry id')):
    """
    A record that is used to keep track of when IDs should be removed from the deduplication cache.
    """


class AverageBucket(namedtuple('AverageBucket', 'start end values')):
    """
    A bucket that stores values of events occurring within a certain window of time.
    """

    @property
    def average(self):
        start_datetime = datetime.fromtimestamp(self.start / 1000)
        end_datetime = datetime.fromtimestamp(self.end / 1000)
        average_value = (sum(self.values) / len(self.values)) if self.values else 0
        return Average(start=start_datetime, end=end_datetime, value=average_value)


class Average(namedtuple('Average', 'start end value')):
    """
    A record of the average value of events between two timestamps.
    """