import os
from base64 import b64decode
from functools import wraps
from typing import Callable

from flask import request

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from rating_operator.api.config import envvar


def authenticated_client():
    """Generate an authenticated Kubernetes client."""
    configuration = client.Configuration()
    configuration.host = os.environ.get('KUBERNETES_PORT').replace('tcp', 'https')
    token = open('/var/run/secrets/kubernetes.io/serviceaccount/token', 'r').read()
    configuration.api_key['authorization'] = f'Bearer {token}'
    configuration.ssl_ca_cert = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
    return client.ApiClient(configuration=configuration)


def get_client():
    """Generate a Kubernetes client, with authentication if configured this way."""
    if os.environ.get('AUTH', 'false') == 'false':
        return client.ApiClient()
    return authenticated_client()


def authenticated_request():
    """Create a dict containing authentication details."""
    token = open('/var/run/secrets/kubernetes.io/serviceaccount/token', 'r').read()
    return {
        'headers': {'authorization': f'Bearer {token}'},
        'cert': '/var/run/secrets/kubernetes.io/serviceaccount/service-ca.crt'
    }


class InvalidTokenError(Exception):
    """Simple error class to handle invalid tokens."""

    pass


def get_token_from_request(request: request) -> str:
    """
    Recover the token from an incoming request.

    :request (Request) The incoming request

    Return the token as a string
    """
    if request.form:
        return request.form.get('token')
    elif request.args:
        return request.args.to_dict().get('token')
    else:
        return request.get_json().get('token')


def require_admin(func: Callable) -> Callable:
    """
    Verify the admin token on the decorated request.

    :func (Callable) The decorated function

    Return a wrapper to execute the decoration
    """
    @wraps(func)
    def wrapper(**kwargs: dict) -> Callable:
        """
        Execute the verification of the admin token.

        :kwargs (dict) A dictionary containing all the function parameters

        Return the decorated function
        """
        token = get_token_from_request(request)
        admin_api_key = envvar('RATING_ADMIN_API_KEY')
        if token == admin_api_key:
            return func(**kwargs)
        raise InvalidTokenError('Internal token unrecognized')
    return wrapper


def register_admin_key():
    """Register the administrator key from the environment."""
    config.load_incluster_config()
    api = client.CoreV1Api(get_client())
    namespace = envvar('RATING_NAMESPACE')
    secret_name = f'{namespace}-admin'
    try:
        secret_encoded_bytes = api.read_namespaced_secret(secret_name, namespace).data
    except ApiException as exc:
        raise exc
    rating_admin_api_key = list(secret_encoded_bytes.keys())[0]
    os.environ[rating_admin_api_key] = b64decode(
        secret_encoded_bytes[rating_admin_api_key]).decode('utf-8')
    return os.environ[rating_admin_api_key]
