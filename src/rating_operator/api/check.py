import datetime
import re
from functools import wraps
from typing import AnyStr, Callable, Dict

from rating_operator.api.db import db
from rating_operator.api.endpoints import auth as auth

import sqlalchemy as sa

from werkzeug.datastructures import ImmutableDict


class InvalidRequestParameter(Exception):
    """Simple error class to handle incoming request parameter invalidity."""

    pass


class TableNameBadFormat(Exception):
    """
    Simple error class to handle Presto table matching error.

    Used only in metering-operator based rating, it is deprecated.
    """

    pass


class InvalidDateError(Exception):
    """Simple error class to handle wrongly formatted date string."""

    pass


def check_date(date: AnyStr) -> bool:
    """
    Attempt to create a datetime object from received timestamp.

    :date (string) A timestamp exctracted from incoming request

    Return a boolean describing the success of datetime object creation
    """
    try:
        datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%fZ')
    except ValueError:
        return False
    return True


def date_checker_start_end(func: Callable) -> Callable:
    """
    Verify datetime parameter validity of incoming request.

    Meant to be used as a decorator

    :func (Callable) The decorated function to be called

    Return the wrapper function executing the verification
    """
    @wraps(func)
    def wrapper(**kwargs: Dict) -> Callable:
        """
        Assert that the date is valid, before calling the decorated function.

        :kwargs (Dict) A directory containing all the parameters for the function call

        Return the decorated function
        """
        if check_date(kwargs['start']) is False \
                or check_date(kwargs['end']) is False:
            raise InvalidDateError(
                'wrong date formatting, cannot create datetime object')
        return func(**kwargs)
    return wrapper


def round_time(dt: datetime.datetime = None, round_to: int = 60) -> datetime.datetime:
    """
    Round the datetime object to the closest minute.

    :dt (datetime, optional) An existing datetime.datetime object to be rounded
    :round_to (int, optional) A value in second to indicate how much to round

    Return the rounded datetime.datetime object
    """
    if dt is None:
        dt = datetime.datetime.utcnow()
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds + round_to / 2) // round_to * round_to
    # Below, 5 is a magic number, basically accounting for time discrepancy of frames
    return dt + (datetime.timedelta(
        0,
        rounding - seconds,
        -dt.microsecond) - datetime.timedelta(minutes=5))


def validate_request_params(kwargs: Dict, regex: AnyStr = r'[a-zA-Z0-9_,]') -> Dict:
    """
    Take a valid regex and apply it on every key:value couple in kwargs.

    :kwargs (Dict) A directory containing the values to validate
    :regex (AnyStr, optional) A regular expression string

    Return kwargs if no invalid key:value couple was found.
    """
    recomp = re.compile(regex)
    for key, value in kwargs.items():
        if recomp.match(value):
            continue
        raise InvalidRequestParameter(f'Parameter {key}: {value} is invalid.')
    return kwargs


def request_params(args: ImmutableDict) -> Dict:
    """
    Validate the argument of an incoming request.

    :args (ImmutableDict) A uneditable dictionary, containing the values to validate

    Return a validated dictionary
    """
    now = round_time()
    last_hour = now - datetime.timedelta(hours=2, minutes=1)
    args = args.to_dict()
    validated = {
        'start': args.pop('start', last_hour.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + 'Z'),
        'end': args.pop('end', now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] + 'Z')
    }
    validated.update(
        validate_request_params(args)
    )
    return validated


def assert_url_params(func: Callable) -> Callable:
    """
    Assert that url parameter are valid.

    :func (Callable) The function for which the parameter will be asserted

    Return a wrapper function to execute the assertion
    """
    @wraps(func)
    def wrapper(**kwargs: Dict):
        """
        Assert that the parameters matches a simple verification regex.

        :kwargs (Dict) A dictionary containing all the function parameters

        Return the wrapped function
        """
        regex = re.compile(r'[a-zA-Z_]')
        if regex.match(kwargs['table']):
            return func(**kwargs)
        raise TableNameBadFormat(f'Table name {kwargs["table"]} unproperly formatted.')
    return wrapper


def multi_tenant(func: Callable) -> Callable:
    """
    Constraint query execution according to the user.

    :func (Callable) The function to which the constraint should be applied

    Return a wrapper function to execute the constraint
    """
    @wraps(func)
    def wrapper(**kwargs: Dict):
        """
        Constraint the function execution according to the user.

        :kwargs (Dict) A dictionary containing all the function parameters

        Return the wrapped function
        """
        qry = 'SELECT namespace FROM namespaces'
        tenant = kwargs['tenant_id']
        admin_user = False
        if tenant != 'default':
            admin_user = auth.check_admin(tenant)
        if admin_user is False:
            qry = sa.text(qry + ' WHERE tenant_id = :tenant_id').params(
                tenant_id=kwargs['tenant_id'])
        kwargs['namespaces'] = [
            dict(row)['namespace'] for row in db.engine.execute(qry)
        ]
        if 'unspecified' not in kwargs['namespaces'] and len(kwargs['namespaces']) > 0:
            kwargs['namespaces'].append('unspecified')
        return func(**kwargs)
    return wrapper
