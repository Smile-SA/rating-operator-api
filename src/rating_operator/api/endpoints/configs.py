import json
from typing import AnyStr, Dict

from flask import Blueprint, abort, jsonify, make_response, request
from flask.wrappers import Response

from flask_json import as_json

from kubernetes import client
from kubernetes.client.rest import ApiException

from rating_operator.api import config
from rating_operator.api import schema
from rating_operator.api.config import envvar
from rating_operator.api.endpoints.auth import authenticated_user
from rating_operator.api.secret import get_client, require_admin


configs_routes = Blueprint('configs', __name__)


@configs_routes.route('/ratingrules')
@as_json
def rating_configs_all() -> Response:
    """Get all the RatingRules."""
    user = authenticated_user(request)
    rows = config.retrieve_configurations(tenant_id=user)
    return {
        'total': len(rows),
        'results': rows
    }


@configs_routes.route('/ratingrules/list/local')
@as_json
def rating_configs_list() -> Response:
    """List all the RatingRules names from the local configuration directory."""
    user = authenticated_user(request)
    rows = config.retrieve_directories(tenant_id=user)
    return {
        'total': len(rows),
        'results': rows
    }


@configs_routes.route('/ratingrules/list/cluster')
@as_json
def rating_rules_list() -> Response:
    """List all the RatingRules names from the cluster."""
    try:
        api = client.CustomObjectsApi(get_client())
        response = api.list_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingrules',
            'namespace': envvar('RATING_NAMESPACE')
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return {
        'results': [item['metadata']['name'] for item in response['items']],
        'total': len(response['items'])
    }


@configs_routes.route('/ratingrules/<timestamp>')
@as_json
def rating_config(timestamp: AnyStr) -> Response:
    """
    Get the RatingRule for a given timestamp.

    :timestamp (AnyStr) A string representing the name of the RatingRule.

    Return the configuration or an empty response.
    """
    user = authenticated_user(request)
    rows = config.retrieve_config_as_dict(timestamp=timestamp,
                                          tenant_id=user)
    return {
        'total': len(rows),
        'results': rows
    }

# TODO(vdaviot) Add an endpoint to get a ratingRule from the cluster direclty
# using the resource name and not the timestamp.


@configs_routes.route('/ratingrules/add', methods=['POST'])
@require_admin
@as_json
def new_rating_config() -> Response:
    """Add a new configuration."""
    received = request.get_json()
    try:
        schema.validate_request_content(received)
        rows = config.create_new_config(content=received)
    except schema.ValidationError as exc:
        abort(make_response(jsonify(message=exc.message), 400))
    else:
        return {
            'total': 1,
            'results': rows
        }


@configs_routes.route('/ratingrules/update', methods=['POST'])
@require_admin
@as_json
def update_rating_config() -> Response:
    """Update a configuration."""
    received = request.get_json()
    if not received:
        received = load_from_form(request.form)
    try:
        schema.validate_request_content(received)
        rows = config.update_config(content=received)
    except config.ConfigurationMissing as exc:
        abort(make_response(jsonify(message=exc.message), 404))
    except schema.ValidationError as err:
        abort(make_response(jsonify(message=err.message), 400))
    else:
        return {
            'total': 1,
            'results': rows
        }


@configs_routes.route('/ratingrules/delete', methods=['POST'])
@require_admin
@as_json
def rating_config_delete() -> Response:
    """Delete a configuration."""
    received = request.get_json()
    if not received:
        received = load_from_form(request.form)
    try:
        rows = config.delete_configuration(timestamp=received['timestamp'])
    except config.ConfigurationMissing as exc:
        abort(make_response(jsonify(message=exc.message), 404))
    else:
        return {
            'total': 1,
            'results': rows
        }


def load_from_form(form: Dict) -> Response:
    """
    Load values from a form.

    :form (Dict) A dictionary containing values.

    Return the form content as a dict.
    """
    return {
        'name': form.get('name'),
        'metrics': json.loads(form.get('metrics')),
        'rules': json.loads(form.get('rules'))
    }


@configs_routes.route('/frontend/ratingrules/get/<config_name>')
@as_json
def frontend_rating_config(config_name: AnyStr) -> Response:
    """
    Get the RatingRule for a given configuration name.

    :config_name (AnyStr) A string representing the configuration name.

    Return the configuration or abort.
    """
    try:
        api = client.CustomObjectsApi(get_client())
        response = api.get_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingrules',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': config_name
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return {
        'results': response,
        'total': 1
    }


@configs_routes.route('/frontend/ratingrules/add', methods=['POST'])
def new_frontend_rating_config() -> Response:
    """Add a new configuration, from the frontend."""
    received = load_from_form(request.form)
    try:
        schema.validate_request_content(received)
    except schema.ValidationError as exc:
        abort(make_response(jsonify(message=exc.message), 400))
    body = {
        'apiVersion': 'rating.alterway.fr/v1',
        'kind': 'RatingRule',
        'metadata': {
            'name': received['name'],
            'namespace': envvar('RATING_NAMESPACE')
        },
        'spec': {
            'metrics': received['metrics'],
            'rules': received['rules']
        }
    }

    try:
        api = client.CustomObjectsApi(get_client())
        api.create_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'namespace': envvar('RATING_NAMESPACE'),
            'plural': 'ratingrules',
            'body': body
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response(f'RatingRule {received["body"]["name"]} created', 200)


@configs_routes.route('/frontend/rationgrules/edit', methods=['POST'])
def update_frontend_rating_config() -> Response:
    """Edit a configuration, from the frontend."""
    received = load_from_form(request.form)
    try:
        schema.validate_request_content(received)
    except schema.ValidationError as exc:
        abort(make_response(jsonify(message=exc.message), 400))

    try:
        api = client.CustomObjectsApi(get_client())
        cr = api.get_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingrules',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': received['name']
        })

        cr['spec'] = {
            'metrics': received.get('metrics', cr['spec']['metrics']),
            'rules': received.get('rules', cr['spec']['rules'])
        }
        api.patch_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingrules',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': received['name'],
            'body': cr
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response(f'RatingRule {received["name"]} edited', 200)


@configs_routes.route('/frontend/ratingrules/delete', methods=['POST'])
def delete_frontend_rating_config() -> Response:
    """Delete a configuration, from the frontend."""
    try:
        api = client.CustomObjectsApi(get_client())
        api.delete_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'namespace': envvar('RATING_NAMESPACE'),
            'plural': 'ratingrules',
            'name': request.form['name']
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response(f'RatingRule {request.form["name"]} deleted', 200)
