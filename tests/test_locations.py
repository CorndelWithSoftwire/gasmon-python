from unittest.mock import patch

from pytest import raises

from gasmon.locations import Location, get_locations

VALID_EMPTY_JSON = '[]'
VALID_LOCATIONS_JSON = '[{"x": 1.1, "y": 1.2, "id": "abc"}, {"x": 2.1, "y": 2.2, "id": "def"}]'
INVALID_LOCATIONS_JSON = '[{"foo": "bar"}]'

@patch('gasmon.locations._download_file_from_s3', return_value=VALID_EMPTY_JSON)
def test_parse_empty_locations(patched_download_file):
    assert len(get_locations('bucket', 'key')) == 0

@patch('gasmon.locations._download_file_from_s3', return_value=VALID_LOCATIONS_JSON)
def test_parse_valid_locations(patched_download_file):
    locations = get_locations('bucket', 'key')
    assert len(locations) == 2
    assert locations[0] == Location(x=1.1, y=1.2, id='abc')
    assert locations[1] == Location(x=2.1, y=2.2, id='def')

@patch('gasmon.locations._download_file_from_s3', return_value=INVALID_LOCATIONS_JSON)
def test_parse_invalid_locations(patched_download_file):
    with raises(Exception, match='.*Malformed locations file.*'):
        get_locations('bucket', 'key')