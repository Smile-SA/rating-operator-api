import bisect
import errno
import logging
import os
import shutil
import sys
import time
from datetime import datetime as dt
from typing import AnyStr, Dict, Generator, List

import yaml


def envvar(name: AnyStr) -> AnyStr:
    """
    Get the value of an environment variable, or die trying.

    :name (AnyStr) The name of the environment variable to get

    Return the variable or crash
    """
    try:
        return os.environ[name]
    except KeyError:
        logging.error(f'Failed to get {name}, exiting..')
        sys.exit(1)


def envvar_string(name: AnyStr) -> AnyStr:
    """
    Get the value of an environment variable, as a string.

    :name (AnyStr) The name of the environmnet variable to get

    Return the variable or None
    """
    var = os.environ.get(name)
    if var == 'true':
        return True
    elif var == 'false':
        return False
    elif var == 'none':
        return None
    return var


class ConfigurationMissing(Exception):
    """Simple error class to handle missing configuration errors."""

    pass


class Config:
    """Utility class to hold the Flask configuration variables."""

    SQLALCHEMY_DATABASE_URI = envvar('POSTGRES_DATABASE_URI')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    JSON_ADD_STATUS = False
    JSON_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S'
    # if envvar_string('AUTH') == 'true':
    SESSION_COOKIE_DOMAIN = os.environ.get('DOMAIN')
    SESSION_COOKIE_SECURE = envvar_string('COOKIE_SECURE')
    SESSION_COOKIE_SAMESITE = envvar_string('COOKIE_SAMESITE')
    SESSION_COOKIE_HTTPONLY = envvar_string('COOKIE_HTTPONLY')


class Lockfile():
    """
    Utility class disallowing simultaneous read/write actions on configuration directory.

    :path (AnyStr) A path to describe the location of the lock file

    This class is meant to be used as a context manager, see the example below:
        with Lockfile(your_dir):
            your_actions_here
    """

    def __init__(self, path: AnyStr):
        self.lock_path = '{}/.lock'.format(path)
        self.delay = 1
        self.timeout = 60

    def __enter__(self):
        while True:
            try:
                self.fd = os.open(self.lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
            except FileNotFoundError:
                logging.warning(
                    'could not find the lockfile, the folder does not exist')
                break
            except FileExistsError:
                try:
                    time.sleep(self.delay)
                    if time.time() - os.path.getmtime(self.lock_path) >= self.timeout:
                        os.unlink(self.lock_path)
                        logging.info('removed stale lock file')
                except FileNotFoundError:
                    logging.warning('lock file does not exist')
            else:
                break

    def __exit__(self, *args: Dict):
        os.close(self.fd)
        os.unlink(self.lock_path)


def delete_configuration(timestamp: AnyStr) -> AnyStr:
    """
    Delete the configuration folder.

    :timestamp (AnyStr) The name of the configuration to delete

    Return the name of the deleted configuration, or crash
    """
    rating_rates_dir = envvar('RATING_RATES_DIR')

    with Lockfile(rating_rates_dir):
        try:
            shutil.rmtree('{}/{}'.format(rating_rates_dir, timestamp))
        except OSError as err:
            logging.error(
                f'An error happened while removing {timestamp} configuration directory.')
            if err.errno == errno.ENOENT:
                logging.error(
                    f'Configuration directory {timestamp} does not exist.')
            sys.exit(1)
        else:
            logging.info(f'removed {timestamp} configuration')
            return timestamp


def write_configuration(config: Dict, timestamp: AnyStr = '0'):
    """
    Write the configuration in the rating-operator-api configuration folder.

    :config (Dict) The configuration to write, as a dict
    :timestamp (AnyStr) The timestamp representing the name of the configuration

    """
    rating_rates_dir = envvar('RATING_RATES_DIR')

    with Lockfile(rating_rates_dir):
        config_dir = f'{rating_rates_dir}/{timestamp}'
        if os.path.exists(config_dir):
            logging.error(
                f'Configuration for timestamp {timestamp} already exist, aborting.')
            sys.exit(1)
        os.makedirs(config_dir)
        for config_name, configuration in config.items():
            with open(f'{config_dir}/{config_name}.yaml', 'w+') as f:
                yaml.safe_dump({config_name: configuration}, f, default_flow_style=False)


def create_new_config(content: Dict) -> AnyStr:
    """
    Create a new configuration from the content dict.

    :content (Dict) A dictionary with the configuration

    Return the name of the created configuration
    """
    timestamp = dt.strptime(content.pop('timestamp'), '%Y-%m-%dT%H:%M:%SZ')
    write_configuration(content, timestamp=int(timestamp.timestamp()))
    return timestamp


def update_config(content: Dict) -> AnyStr:
    """
    Update the configuration pointed by content['timestamp'].

    :content (Dict) A dictionary with the configuration

    Return the name of the updated configuration
    """
    rating_rates_dir = envvar('RATING_RATES_DIR')
    ts = dt.strptime(content.pop('timestamp'), '%Y-%m-%dT%H:%M:%SZ')
    timestamp = int(ts.timestamp())
    with Lockfile(rating_rates_dir):
        config_dir = f'{rating_rates_dir}/{timestamp}'
        if not os.path.exists(config_dir):
            raise ConfigurationMissing
        for config_name, configuration in content.items():
            with open(f'{config_dir}/{config_name}.yaml', 'w+') as f:
                yaml.safe_dump({config_name: configuration}, f, default_flow_style=False)
    return timestamp


def retrieve_directories(path: AnyStr = envvar('RATING_RATES_DIR'),
                         tenant_id: AnyStr = None) -> List:
    """
    Get the list of configuration directories.

    :path (AnyStr) A string containing the path of the configuration folder
    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility

    Return a sorted list of configuration names
    """
    dir_list = os.listdir(path)
    if '.lock' in dir_list:
        dir_list.remove('.lock')

    if 'lost+found' in dir_list:
        dir_list.remove('lost+found')
    return sorted(dir_list, key=float)


def retrieve_config_as_dict(timestamp: AnyStr, tenant_id: AnyStr = None) -> Dict:
    """
    Retrieve the configuration as a dictionary.

    :timestamp (AnyStr) A string corresponding to the name of the configuration
    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility

    Return the configuration as a dictionary
    """
    rating_rates_dir = envvar('RATING_RATES_DIR')
    config = {}
    with Lockfile(f'{rating_rates_dir}/{timestamp}'):
        for file in ['metrics.yaml', 'rules.yaml']:
            with open(f'{rating_rates_dir}/{timestamp}/{file}', 'r') as f:
                config_type = os.path.splitext(file)[0]
                config[config_type] = yaml.safe_load(f)
    return config


def retrieve_configurations(tenant_id: AnyStr = None) -> List:
    """
    Retrieve all the configurations.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility

    Return a list of configurations dictionaries
    """
    rating_rates_dir = envvar('RATING_RATES_DIR')
    configurations = []

    with Lockfile(rating_rates_dir):
        for timestamp in retrieve_directories(rating_rates_dir):
            config_dict = retrieve_config_as_dict(timestamp)
            config_dict['valid_from'] = timestamp
            configurations.append(config_dict)
    for idx in range(len(configurations) - 1):
        configurations[idx]['valid_to'] = configurations[idx + 1]['valid_from']
    configurations[-1]['valid_to'] = dt(2100, 1, 1, 1, 1).strftime('%s')
    return configurations


def get_closest_configs_bisect(timestamp: AnyStr, timestamps: List) -> AnyStr:
    """
    Get the closest configuration, using bisection.

    :timestamp (AnyStr) A string containing a timestamp
    :timestamps (list) A list of all configurations names

    Return the matched configuration name
    """
    return bisect.bisect_right(timestamps, timestamp) - 1


def format_labels_prometheus(labels: Dict) -> AnyStr:
    """
    Format the labels dictionary as a string for Prometheus exposition.

    :labels (Dict) A dictionary containing the labels

    Return a properly formatted string for Prometheus
    """
    if not labels:
        return ''
    string_labels = '{'
    n_labels = len(labels)
    for key, value in labels.items():
        n_labels -= 1
        string_labels += f'{key}=\"{value}\"'
        if n_labels > 0:
            string_labels += ', '
    string_labels += '}'
    return string_labels


def acquire_labels(rules: Dict) -> Dict:
    """
    Extract labels from rules.

    :rules (Dict) A dictionary containing the rules

    Return a dictionary containing the labels as key:value pairs
    """
    labels = {}
    for ruleset in rules['rules']:
        for label in ruleset.get('labelSet', {}).keys():
            if label not in labels:
                labels[label] = ''
    return labels


def generate_metrics_from_rules(rules: Dict):
    """
    Generate the metric string from rules to expose to Prometheus.

    :rules (Dict) A dictionary containing the rules

    Yield the metric strings to be iterated over
    """
    labels_array = acquire_labels(rules)

    for ruleset in rules['rules']:
        ruleset_labels = (ruleset.get('labelSet', {})).update(labels_array)

        labels = format_labels_prometheus(ruleset_labels)
        for rule in ruleset.get('ruleset'):
            metric, value = rule['metric'], rule['value']
            yield f'{metric}{labels} {value}'


def generate_rules_export() -> Generator[AnyStr, None, None]:
    """
    Create the generator that create Prometheus metric strings.

    Return a generator to be iterated on
    """
    timestamp = int(time.time())
    closest_config = retrieve_closest_config(timestamp)
    return generate_metrics_from_rules(closest_config['rules'])


def retrieve_closest_config(timestamp: AnyStr) -> Dict:
    """
    Retrieve the closest configuration from the given timestamp.

    :timestamp (AnyStr) A string representing the configuration name

    Return the configuration as a dictionary
    """
    timestamp_tuple = tuple(int(ts) for ts in retrieve_directories())
    closest = get_closest_configs_bisect(timestamp, timestamp_tuple)
    return retrieve_config_as_dict(timestamp_tuple[closest])
