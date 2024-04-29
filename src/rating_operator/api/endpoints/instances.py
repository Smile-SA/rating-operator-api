import logging

from flask import Blueprint, abort, make_response, request
from flask.wrappers import Response

from flask_json import as_json

from kubernetes import client
from kubernetes.client.rest import ApiException

from rating_operator.api.config import envvar
from rating_operator.api.secret import get_client

instances_routes = Blueprint('models', __name__)
LOG = logging.getLogger(__name__)


@instances_routes.route('/instances/list')
@as_json
def models_rule_list() -> Response:
    """List all the RatingRuleInstance."""
    try:
        api = client.CustomObjectsApi(get_client())
        response = api.list_namespaced_custom_object(**{
            'group': 'rating.smile.fr',
            'version': 'v1',
            'plural': 'ratingruleinstances',
            'namespace': envvar('RATING_NAMESPACE')
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return {
        'results': [item['metadata']['name'] for item in response['items']],
        'total': len(response['items'])
    }


@instances_routes.route('/instances/get')
@as_json
def models_rule_get() -> Response:
    """Get a RatingRuleInstance."""
    api = client.CustomObjectsApi(get_client())
    try:
        response = api.get_namespaced_custom_object(**{
            'group': 'rating.smile.fr',
            'version': 'v1',
            'plural': 'ratingruleinstances',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': request.args.to_dict()['name']
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return {
        'results': response,
        'total': 1
    }


@instances_routes.route('/instances/add', methods=['POST'])
def models_instance_add() -> Response:
    """Create a new RatingRuleInstance."""
    config = request.form or request.get_json()
    metric_name = config['metric_name']
    name = 'rating-rule-instance-' + metric_name
    body_spec = {}
    api = client.CustomObjectsApi(get_client())

    # Check for template demand, override metric var if exist
    config_vars = list(config)
    if 'template_name' in config_vars:
        template_name = 'rating-rule-template-' + config['template_name']
        try:
            response = api.get_namespaced_custom_object(**{
                'group': 'rating.smile.fr',
                'version': 'v1',
                'plural': 'ratingruletemplates',
                'namespace': envvar('RATING_NAMESPACE'),
                'name': template_name
            })
        except ApiException as exc:
            abort(make_response(str(exc), 400))
        spec = response['spec']
        body_spec['metric'] = spec['query_template']
        config_vars.remove('template_name')

        # Ignore metric variable
        if 'metric' in config_vars:
            config_vars.remove('metric')

        body_spec['name'] = metric_name
        config_vars.remove('metric_name')

    # Register other variables
    for key in config_vars:
        body_spec[key] = config[key]

    body = {
        'apiVersion': 'rating.smile.fr/v1',
        'kind': 'RatingRuleInstance',
        'metadata': {
            'name': name
        },
        'spec': body_spec
    }
    api = client.CustomObjectsApi(get_client())
    try:
        api.create_namespaced_custom_object(**{
            'group': 'rating.smile.fr',
            'version': 'v1',
            'namespace': envvar('RATING_NAMESPACE'),
            'plural': 'ratingruleinstances',
            'body': body
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response(f'RatingRuleInstance {config["metric_name"]} created', 200)


@instances_routes.route('/instances/edit', methods=['POST'])
def models_instance_edit() -> Response:
    """Edit a RatingRuleInstance."""
    config = request.form or request.get_json()
    metric_name = config['metric_name']
    name = 'rating-rule-instance-' + metric_name
    body_spec = {}
    patch_vars = list(config)
    api = client.CustomObjectsApi(get_client())

    # Check for template demand
    if 'template_name' in patch_vars:

        template_name = 'rating-rule-template-' + config['template_name']
        try:
            response = api.get_namespaced_custom_object(**{
                'group': 'rating.smile.fr',
                'version': 'v1',
                'plural': 'ratingruletemplates',
                'namespace': envvar('RATING_NAMESPACE'),
                'name': template_name
            })
        except ApiException as exc:
            abort(make_response(str(exc), 400))

        spec = response['spec']
        body_spec['metric'] = spec['query_template']
        patch_vars.remove('template_name')

        # Ignore metric variable
        if 'metric' in patch_vars:
            patch_vars.remove('metric')

    try:
        cr = api.get_namespaced_custom_object(**{
            'group': 'rating.smile.fr',
            'version': 'v1',
            'plural': 'ratingruleinstances',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': name
        })

        body_spec['name'] = config.get('metric_name')
        patch_vars.remove('metric_name')

        # Register other variables
        for key in patch_vars:
            body_spec[key] = config.get(key)
        cr['spec'] = body_spec

    except ApiException as exc:
        abort(make_response(str(exc), 400))

    try:
        api.patch_namespaced_custom_object(**{
            'group': 'rating.smile.fr',
            'version': 'v1',
            'plural': 'ratingruleinstances',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': name,
            'body': cr
        })
    except ApiException:
        abort(make_response(f'No change detected for RatingRuleInstances \
        {config["metric_name"]}', 200))
    return make_response(f'RatingRuleInstances {config["metric_name"]} edited', 200)


@instances_routes.route('/instances/delete', methods=['POST'])
def models_metric_delete() -> Response:
    """Delete a RatingRuleInstance."""
    config = request.form or request.get_json()
    metric_name = config['metric_name']
    name = 'rating-rule-instance-' + metric_name
    api = client.CustomObjectsApi(get_client())
    try:
        api.delete_namespaced_custom_object(**{
            'group': 'rating.smile.fr',
            'version': 'v1',
            'namespace': envvar('RATING_NAMESPACE'),
            'plural': 'ratingruleinstances',
            'name': name
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response(f'RatingRuleInstances {config["metric_name"]} deleted', 200)
