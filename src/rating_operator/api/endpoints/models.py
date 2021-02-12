from flask import Blueprint, abort, make_response, request
from flask.wrappers import Response

from flask_json import as_json

from kubernetes import client
from kubernetes.client.rest import ApiException

from rating_operator.api.config import envvar
from rating_operator.api.secret import get_client

models_routes = Blueprint('models', __name__)


@models_routes.route('/models/list')
@as_json
def models_rule_list() -> Response:
    """Get the RatingRuleModel."""
    try:
        api = client.CustomObjectsApi(get_client())
        response = api.list_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingrulemodels',
            'namespace': envvar('RATING_NAMESPACE')
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return {
        'results': [item['metadata']['name'] for item in response['items']],
        'total': 1
    }


@models_routes.route('/models/get')
@as_json
def models_rule_get() -> Response:
    """Get a RatingRuleModels."""
    api = client.CustomObjectsApi(get_client())
    try:
        response = api.get_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingrulemodels',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': request.args.to_dict()['name']
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return {
        'results': response,
        'total': 1
    }


@models_routes.route('/models/add', methods=['POST'])
def models_rule_new() -> Response:
    """Create a RatingRuleModels."""
    config = request.form or request.get_json()
    body = {
        'apiVersion': 'rating.alterway.fr/v1',
        'kind': 'RatingRuleModels',
        'metadata': {
            'name': config['name']
        },
        'spec': {
            'timeframe': config['timeframe'],
            'metric': config['metric'],
            'name': config['metric_name']
        }
    }
    api = client.CustomObjectsApi(get_client())
    try:
        api.create_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'namespace': envvar('RATING_NAMESPACE'),
            'plural': 'ratingrulemodels',
            'body': body
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response(f'RatingRuleModels {config["name"]} created', 200)


@models_routes.route('/models/edit', methods=['POST'])
def models_rule_edit() -> Response:
    """Edit a RatingRuleModels."""
    config = request.form or request.get_json()
    api = client.CustomObjectsApi(get_client())
    try:
        cr = api.get_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingrulemodels',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': config['name']
        })

        cr['spec'] = {
            'metric': config.get('metric', cr['spec']['metric']),
            'timeframe': config.get('timeframe', cr['spec']['timeframe']),
            'name': config.get('metric_name', cr['spec']['name'])
        }
        api.patch_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingrulemodels',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': config['name'],
            'body': cr
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response(f'RatingRuleModels {config["name"]} edited', 200)


@models_routes.route('/models/delete', methods=['POST'])
def models_rule_delete() -> Response:
    """Delete a RatingRuleModels."""
    config = request.form or request.get_json()
    api = client.CustomObjectsApi(get_client())
    try:
        api.delete_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'namespace': envvar('RATING_NAMESPACE'),
            'plural': 'ratingrulemodels',
            'name': config['name']
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response(f'RatingRuleModels {config["name"]} deleted', 200)
