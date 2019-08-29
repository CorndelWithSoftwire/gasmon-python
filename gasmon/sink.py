"""
A module consisting of sinks that the processed events will end up at.
"""

from abc import abstractmethod, ABC
from collections import defaultdict, deque, namedtuple
from datetime import datetime
import logging
from time import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Sink(ABC):
    """
    An abstract base class for pipeline sinks.
    """

    @abstractmethod
    def handle(self, events):
        """
        Handle each of the given stream of events.
        """
        pass

    @staticmethod
    def parallel(*sinks):
        """
        A sink consisting of multiple sinks in parallel with each other.
        """
        return ParallelSink(sinks)


class ParallelSink(Sink):
    """
    A Sink consisting of multiple sinks in parallel.
    """

    def __init__(self, sinks):
        """
        Create a parallel Sink from the given Sinks.
        """
        self.sinks = sinks

    def handle(self, events):
        """
        Handle the events by letting each sink process them.
        """
        for event in events:
            for sink in self.sinks:
                sink.handle([event])


class ChronologicalAverager(Sink):
    """
    A Sink that produces a moving average of events in a given window.
    """

    def __init__(self, average_period_seconds, expiry_time_seconds):
        """
        Create an ChronologicalAverager which will calculate a moving average over windows of width
        `average_period_seconds`, under the assumption that events may arrive up to
        `expiry_time_seconds` late.
        """

        # Store configuration values
        self.average_period_millis = 1000 * average_period_seconds
        self.expiry_time_millis = 1000 * expiry_time_seconds

        # Create buckets for calculating the moving averages
        current_time_millis = 1000 * time()
        self.buckets = deque([ChronologicalAverageBucket(start=(current_time_millis - self.expiry_time_millis), end=(current_time_millis - self.expiry_time_millis + self.average_period_millis), values=[])])

    def handle(self, events):
        """
        For each event to be processed, add its value to an appropriate bucket. If any bucket
        has expired, then yield its average and remove it from consideration.
        """
        for event in events:
            self.add_to_bucket(event)
            expired_bucket = self.maybe_expire_first_bucket_and_get_average()
            if expired_bucket is not None:
                average = expired_bucket.average
                logger.info(f'Average value for {average.start} to {average.end} is {average.value}')

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
            self.buckets.append(ChronologicalAverageBucket(start=current_last_end, end=(current_last_end + self.average_period_millis), values=[]))

        # Find the right bucket and add the event value
        bucket_index = int(event.timestamp - self.buckets[0].start) // self.average_period_millis
        self.buckets[bucket_index].values.append(event.value)

    def maybe_expire_first_bucket_and_get_average(self):
        current_time_millis = 1000 * time()
        if current_time_millis - self.expiry_time_millis > self.buckets[0].end:
            return self.buckets.popleft()


class LocationAverager(Sink):
    """
    A Sink that produces the average values of events, grouped by location.
    """

    def __init__(self, observations_required):
        """
        Create a LocationAverager which will produce averages every time `observations_required`
        events are handled.
        """
        self.averages = defaultdict(LocationAverage)
        self.observations = 0
        self.observations_required = observations_required

    def handle(self, events):
        """
        For each event to be processed, insert its value into a by-location dictionary. If
        enough observations have been seen, then produce the averages.
        """
        for event in events:
            self.observations += 1
            self.averages[event.location_id].add_observation(event.value)

            if self.observations >= self.observations_required:
                self.observations = 0
                for (location_id, average) in self.averages.items():
                    logger.info(f'Average value at location {location_id} is {average.average}')


class ChronologicalAverageBucket(namedtuple('ChronologicalAverageBucket', 'start end values')):
    """
    A bucket that stores values of events occurring within a certain window of time.
    """

    @property
    def average(self):
        start_datetime = datetime.fromtimestamp(self.start / 1000)
        end_datetime = datetime.fromtimestamp(self.end / 1000)
        average_value = (sum(self.values) / len(self.values)) if self.values else 0
        return ChronologicalAverage(start=start_datetime, end=end_datetime, value=average_value)


class ChronologicalAverage(namedtuple('ChronologicalAverage', 'start end value')):
    """
    A record of the average value of events between two timestamps.
    """


class LocationAverage:
    """
    A record of the average value of events at a particular location.
    """

    def __init__(self):
        self.total_value = 0
        self.observations = 0

    def add_observation(self, value):
        self.total_value += value
        self.observations += 1

    @property
    def average(self):
        return 0 if self.observations == 0 else (self.total_value / self.observations)