"""
The GasMon application
"""

import logging
import sys
from time import time

from gasmon.configuration import config
from gasmon.locations import get_locations
from gasmon.pipeline import FixedDurationSource, LocationFilter, Deduplicator
from gasmon.receiver import QueueSubscription, Receiver
from gasmon.sink  import Averager

root_logger = logging.getLogger()
log_formatter = logging.Formatter('%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s')

file_handler = logging.FileHandler('GasMon.log')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(log_formatter)
root_logger.addHandler(file_handler)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(log_formatter)
root_logger.addHandler(console_handler)

def main():
    """
    Run the application.
    """

    # Get the list of valid locations from S3
    s3_bucket = config['locations']['s3_bucket']
    locations_key = config['locations']['s3_key']
    locations = get_locations(s3_bucket, locations_key)

    # Create the pipeline steps that events will pass through when being processed
    run_time_seconds = int(config['run_time_seconds'])
    fixed_duration_source = FixedDurationSource(run_time_seconds)
    location_filter = LocationFilter(locations)
    deduplicator = Deduplicator(int(config['deduplicator']['cache_time_to_live_seconds']))
    pipeline = fixed_duration_source.compose(location_filter).compose(deduplicator)

    # Create the sink that will handle the events that come through the pipeline
    averager = Averager(int(config['averager']['average_period_seconds']), int(config['averager']['expiry_time_seconds']))

    # Create an SQS queue that will be subscribed to the SNS topic
    sns_topic_arn = config['receiver']['sns_topic_arn']
    with QueueSubscription(sns_topic_arn) as queue_subscription:

        # Process events as they come in from the queue
        receiver = Receiver(queue_subscription)
        pipeline.sink(averager).handle(receiver.get_events())

        # Show final stats
        print(f'Processed {fixed_duration_source.events_processed} events in {run_time_seconds} seconds')
        print(f'Events/s: {fixed_duration_source.events_processed / run_time_seconds:.2f}')
        print(f'Invalid locations skipped: {location_filter.invalid_events_filtered}')
        print(f'Duplicated events skipped: {deduplicator.duplicate_events_ignored}')