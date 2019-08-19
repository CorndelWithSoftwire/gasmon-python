"""
Parses the application's configuration file
"""

import yaml

with open('gasmon/config.yaml', 'r') as config_file:
    config = yaml.load(config_file, Loader=yaml.BaseLoader)