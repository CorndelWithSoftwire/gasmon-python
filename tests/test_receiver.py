import json
from unittest.mock import ANY, MagicMock, PropertyMock, patch

from pytest import fixture

from gasmon.receiver import Receiver, Event

QUEUE_URL = 'queue-url'

VALID_MESSAGE = """
{ 
    "locationId": "abc",
    "eventId": "def",
    "timestamp": 123456789,
    "value": 2
}
"""

INVALID_MESSAGE = """
{
    "not": "valid"
}
"""

@fixture
def receiver_for_tests():
    queue_subscription = MagicMock()
    type(queue_subscription).queue_url = PropertyMock(return_value=QUEUE_URL)
    return Receiver(queue_subscription)

def test_receiver_parses_valid_message(receiver_for_tests):
    with patch.object(receiver_for_tests, 'sqs_client') as mock_sqs_client:
        mock_sqs_client.receive_message.return_value = _build_messages([VALID_MESSAGE])
        event = receiver_for_tests.get_events().__next__()
        assert event == Event(location_id='abc', event_id='def', timestamp=123456789, value=2)

def test_receiver_ignores_invalid_message(receiver_for_tests):
    with patch.object(receiver_for_tests, 'sqs_client') as mock_sqs_client:
        mock_sqs_client.receive_message.return_value = _build_messages([INVALID_MESSAGE, VALID_MESSAGE])
        event = receiver_for_tests.get_events().__next__()
        assert event == Event(location_id='abc', event_id='def', timestamp=123456789, value=2)

def test_receiver_deletes_messages_from_queue(receiver_for_tests):
    with patch.object(receiver_for_tests, 'sqs_client') as mock_sqs_client:
        mock_sqs_client.receive_message.return_value = _build_messages([VALID_MESSAGE])
        event = receiver_for_tests.get_events().__next__()
        mock_sqs_client.delete_message_batch.assert_called_with(QueueUrl=QUEUE_URL, Entries=ANY)

def _build_messages(message_bodies):
    return {'Messages': [{'ReceiptHandle': 'foo', 'Body': json.dumps({'Message': message_body})} for message_body in message_bodies]}