"""
A module that handles subscribing an SQS queue to an SNS topic and receiving events from it
"""

from collections import namedtuple
import json
import logging
from queue import Empty, Queue
import re
import threading
import uuid

import boto3

from gasmon.configuration import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Event(namedtuple('Event', 'location_id event_id value timestamp')):
    """
    A single event that has been produced by a sensor.
    """


class QueueSubscription:
    """
    A class that provides a context manager for a queue subscription, that will create
    a queue when its context is entered, and delete the queue when exited.
    """

    def __init__(self, topic_arn):
        """
        Prepare to create a queue and subscribe it to the topic.
        """

        # Create SQS and SNS clients
        aws_region = config['aws']['region_name']
        self.sqs_client = boto3.client('sqs', region_name=aws_region)
        self.sns_client = boto3.client('sns', region_name=aws_region)

        # Store the topic ARN for later
        self.topic_arn = topic_arn

    def __enter__(self):
        """
        Create an SQS queue and subscribe it to the SNS topic.
        """

        # Create a queue to subscribe to the topic
        queue_name = str(uuid.uuid4())
        self.queue_url = self.sqs_client.create_queue(QueueName=queue_name)['QueueUrl']
        queue_arn = self.sqs_client.get_queue_attributes(QueueUrl=self.queue_url, AttributeNames=['QueueArn'])['Attributes']['QueueArn']
        logger.info(f'Create SQS queue {queue_arn}')

        # Attach a policy to the queue
        iam_policy = QueueSubscription._create_policy(queue_arn, self.topic_arn)
        self.sqs_client.set_queue_attributes(QueueUrl=self.queue_url, Attributes={'Policy': iam_policy})
        logger.info('Attached IAM policy to SQS queue')

        # Subscribe the queue to the topic
        self.subscription_arn = self.sns_client.subscribe(TopicArn=self.topic_arn, Protocol='sqs', Endpoint=queue_arn, ReturnSubscriptionArn=True)['SubscriptionArn']
        logger.info(f'Subscribed SQS queue to topic: {self.topic_arn}')

        return self

    def __exit__(self, *args):
        """
        Delete the SQS queue and SNS subscription.
        """
        
        logger.info(f'Deleting queue {self.queue_url} and subscription {self.subscription_arn}')
        self.sqs_client.delete_queue(QueueUrl=self.queue_url)
        self.sns_client.unsubscribe(SubscriptionArn=self.subscription_arn)
        logger.info('Successfully deleted queue and subscription')
        

    @staticmethod
    def _create_policy(queue_arn, topic_arn):
        """
        Create an IAM policy document that will allow an SNS topic to send messages to an SQS queue.
        """

        policy_document = {
            'Version': '2012-10-17',
            'Statement': [{
                'Sid': f'allow-subscription-{topic_arn}',
                'Effect': 'Allow',
                'Principal': {'AWS': '*'},
                'Action': 'SQS:SendMessage',
                'Resource': f'{queue_arn}',
                'Condition': {
                    'ArnEquals': { 'aws:SourceArn': f'{topic_arn}' }
                }
            }]
        }

        return json.dumps(policy_document)


class SQSEventReceiver:
    """
    A class that retrieves events from SQS, designed to have multiple copies run 
    concurrently in worker threads.
    """

    def __init__(self, queue_subscription):
        """
        Create an SQSEventReceiver that will use the given QueueSubscription
        """
        aws_region = config['aws']['region_name']
        self.sqs_client = boto3.session.Session().client('sqs', region_name=aws_region)
        self.queue_subscription = queue_subscription

    def get_events(self):
        """
        Get events by reading from the QueueSubscription
        """
        return [event for event in map(SQSEventReceiver._convert_message, self._get_messages()) if event is not None]

    @staticmethod
    def _convert_message(message):
        """
        Convert a raw message from the SQS queue into an Event
        """
        try:
            message_body = json.loads(message['Body'])['Message']
            unescaped_message_body = re.sub('^\"|\"$|\\\\', '', message_body)
            json_message_body = json.loads(unescaped_message_body)
            return Event(location_id=json_message_body['locationId'], event_id=json_message_body['eventId'], value=json_message_body['value'], timestamp=json_message_body['timestamp'])
        except Exception as e:
            logger.warning(f'Skipping invalid message {message} - {e}')
            return None

    def _get_messages(self):
        """
        Read, and delete, raw messages from the SQS queue.
        """

        # Read messages from SQS
        logger.debug('Polling SQS for messages')
        messages = self.sqs_client.receive_message(QueueUrl=self.queue_subscription.queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=1).get('Messages', [])
        logger.debug(f'Received {len(messages)} messages from SQS')

        # Delete and return the messages
        self._delete_messages(messages)
        return messages

    def _delete_messages(self, messages):
        """
        Delete the given messages from the queue.
        """
        if len(messages) > 0:

            # Send a request to delete all of the messages
            logger.debug(f'Deleting {len(messages)} messages from SQS')
            batch_entries = list(map(lambda index_and_message: {'Id': str(index_and_message[0]), 'ReceiptHandle': index_and_message[1]['ReceiptHandle']}, enumerate(messages)))
            delete_result = self.sqs_client.delete_message_batch(QueueUrl=self.queue_subscription.queue_url, Entries=batch_entries)

            # Report on each failed deletion
            for failure in delete_result.get('Failed', []):
                logger.warning(f'Failed to delete SQS message: {failure}')

            # Report on the number of successful deletions
            logger.debug(f'Successfully deleted {len(delete_result.get("Successful", []))} of {len(messages)} messages')


class ReceiverThread(threading.Thread):
    """
    A single worker thread working to receive messages from SQS.
    """
    
    def __init__(self, queue_subscription, event_queue, stop_signal):
        """
        Create a worker thread that will receive events using the given queue
        subscription, put them onto the event_queue for processing, and continue 
        processing until it receives a stop signal.
        """
        super().__init__()
        self.event_queue = event_queue
        self.stop_signal = stop_signal
        self.event_receiver = SQSEventReceiver(queue_subscription)

    def run(self):
        """
        Repeatedly pull messages from SQS and push them onto the event queue.
        """
        while True:
            if self.stop_signal.is_set():
                break
            for event in self.event_receiver.get_events():
                self.event_queue.put(event)


class Receiver:
    """
    A class that runs multiple SQSEventReceiver workers concurrently to
    receive events from SQS.
    """

    def __init__(self, queue_subscription, num_threads):
        """
        Create a Receiver that will use the given number of worker threads
        """
        self.event_queue = Queue()
        self.stop_signal = threading.Event()
        self.threads = [ReceiverThread(queue_subscription, self.event_queue, self.stop_signal) for _ in range(num_threads)]

    def __enter__(self):
        """
        Start each of the worker threads.
        """
        for thread in self.threads:
            thread.start()
        return self

    def __exit__(self, *args):
        """
        Signal to each worker thread that it should stop processing.
        """
        self.stop_signal.set()
        for thread in self.threads:
            thread.join()

    def get_events(self):
        """
        Generate events by polling the queue that each worker thread is publishing to.
        """
        while True:
            try:
                yield self.event_queue.get(timeout=1)
            except Empty:
                pass