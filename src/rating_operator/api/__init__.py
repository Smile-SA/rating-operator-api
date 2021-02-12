"""rating_operator.api."""
import logging

import pkg_resources

# Custom logger
LOG = logging.getLogger(name=__name__)

# PEP 396 style version marker
try:
    __version__ = pkg_resources.get_distribution('rating_operator.api').version
except pkg_resources.DistributionNotFound:
    LOG.warning('Could not get the package version from pkg_resources')
    __version__ = 'unknown'

__author__ = 'AlterWay R&D team'
__author_email__ = 'rnd@alterway.fr'
__license__ = 'Apache-2.0'
