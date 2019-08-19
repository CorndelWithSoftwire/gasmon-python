"""
A module that deals with downloading and parsing the locations from S3.
"""

from collections import namedtuple
import logging
import json

import boto3

from gasmon.configuration import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class Location(namedtuple('Location', 'x y id')):
    """
    A sensor location, consisting of x and y coordinates and a unique ID.
    """

def get_locations(s3_bucket, locations_key):
    """
    Retrieve and parse the locations JSON from S3
    """

    # Download and parse the file
    logger.info(f'Downloading list of locations from key {locations_key} in bucket {s3_bucket}')
    locations_json = _download_file_from_s3(s3_bucket, locations_key)
    logger.info(f'Retrieved locations JSON from S3: {locations_json}')
    return _parse_locations_json(locations_json)

def _download_file_from_s3(bucket, object_key):
    """
    Retrieve the object with the given key from Amazon S3
    """

    # Create an S3 client for downloading files from S3
    s3_region = config['aws']['region_name']
    s3_client = boto3.client('s3', region_name=s3_region)

    # Download the object and decode it from bytes to a string
    return s3_client.get_object(Bucket=bucket, Key=object_key)['Body'].read().decode('utf-8')

def _parse_locations_json(locations):
    """
    Parse the list of locations retrieved from S3 into a JSON object
    """
    try:
        parsed_json = json.loads(locations)
        return list(map(lambda loc: Location(x=loc['x'], y=loc['y'], id=loc['id']), parsed_json))
    except Exception as e:
        raise Exception('Malformed locations file', e)