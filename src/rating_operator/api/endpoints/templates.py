import datetime
import logging

from flask import Blueprint, abort, jsonify, make_response, request
from flask.wrappers import Response

from flask_json import as_json

from kubernetes import client
from kubernetes.client.rest import ApiException

from rating_operator.api.config import envvar
from rating_operator.api.queries import metrics as query
from rating_operator.api.secret import get_client


templates_routes = Blueprint('templates', __name__)
LOG = logging.getLogger(__name__)


@templates_routes.route('/templates/list')
@as_json
def models_template_list() -> Response:
    """List all the RatingRuleTemplate."""
    try:
        api = client.CustomObjectsApi(get_client())
        response = api.list_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingruletemplates',
            'namespace': envvar('RATING_NAMESPACE')
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return {
        'results': [item['metadata']['name'] for item in response['items']],
        'total': len(response['items'])
    }


@templates_routes.route('/templates/get')
@as_json
def models_template_get() -> Response:
    """Get a RatingRuleTemplate."""
    api = client.CustomObjectsApi(get_client())
    template_name = 'rating-rule-template-' + request.args.to_dict()['query_name']
    try:
        response = api.get_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingruletemplates',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': template_name
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return {
        'results': response,
        'total': 1
    }


@templates_routes.route('/templates/add', methods=['POST'])
def models_template_new() -> Response:
    """Create a RatingRuleTemplate."""
    config = request.form or request.get_json()
    name = config['query_name']
    template_group = config['query_group']
    tempalte = config['query_template']
    template_name = 'rating-rule-template-' + name

    datetimeobj = datetime.datetime.now()
    template_id = datetimeobj.strftime('%d-%b-%Y (%H:%M:%S.%f)')
    query.store_template_conf(template_id, name, template_group, tempalte, '')

    body = {
        'apiVersion': 'rating.alterway.fr/v1',
        'kind': 'RatingRuleTemplate',
        'metadata': {
            'name': template_name
        },
        'spec': {
            'query_name': name,
            'query_group': template_group,
            'query_template': tempalte
        }
    }
    api = client.CustomObjectsApi(get_client())
    try:
        api.create_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'namespace': envvar('RATING_NAMESPACE'),
            'plural': 'ratingruletemplates',
            'body': body
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response(f'RatingRuleTemplate {config["query_name"]} created', 200)


@templates_routes.route('/templates/delete', methods=['POST'])
def models_template_delete() -> Response:
    """Delete a RatingRuleTemplate."""
    config = request.form or request.get_json()
    query.delete_template_conf(config['query_name'])
    template_name = 'rating-rule-template-' + config['query_name']
    api = client.CustomObjectsApi(get_client())
    try:
        api.delete_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingruletemplates',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': template_name
        })
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response(f'RatingRuleTemplate {config["query_name"]} deleted', 200)


@templates_routes.route('/templates/edit', methods=['POST'])
def models_template_edit() -> Response:
    """Edit a RatingRuleTemplate."""
    config = request.form or request.get_json()
    name = config['query_name']
    template_name = 'rating-rule-template-' + name

    datetimeobj = datetime.datetime.now()
    template_id = datetimeobj.strftime('%d-%b-%Y (%H:%M:%S.%f)')

    api = client.CustomObjectsApi(get_client())
    try:
        cr = api.get_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingruletemplates',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': template_name
        })

        query_template = config.get('query_template', cr['spec']['query_template'])
        query_name = config.get('query_name', cr['spec']['query_name']),
        query_group = config.get('query_group', cr['spec']['query_group'])

        cr['spec'] = {
            'query_template': query_template,
            'query_name': query_name,
            'query_group': query_group
        }

        api.patch_namespaced_custom_object(**{
            'group': 'rating.alterway.fr',
            'version': 'v1',
            'plural': 'ratingruletemplates',
            'namespace': envvar('RATING_NAMESPACE'),
            'name': template_name,
            'body': cr
        })

        query.store_template_conf(template_id, name, query_group,
                                  query_template, '')
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response(f'RatingRuleTemplate {config["query_name"]} edited', 200)


@templates_routes.route('/templates/metric/list')
def models_metric_list() -> Response:
    """List all RatingRuleInstance configurations."""
    metrics = query.list_metric_conf()
    return make_response(
        jsonify(metrics=metrics, total=len(metrics)), 200)


@templates_routes.route('/templates/metric/get')
def models_metric_get() -> Response:
    """Get RatingRuleInstance configurations."""
    name = request.args.to_dict()['metric_name']
    metrics = query.get_metric_conf(name)
    return make_response(
        jsonify(metrics=metrics, total=len(metrics)), 200)


@templates_routes.route('/templates/metric/delete', methods=['POST'])
def template_metric_delete() -> Response:
    """Delete configurations of RatingRuleInstance in the database."""
    config = request.form or request.get_json()
    query.delete_metric_conf(config['metric_name'])
    return {
        'total': 1,
        'results': f'RatingRuleValues {config["metric_name"]} deleted'
    }


@templates_routes.route('/templates/metric/add', methods=['POST'])
def models_metric_add():
    """Save RatingRuleInstance configurations to database."""
    config = request.form or request.get_json()
    metric_name = config['metric_name']
    template_name = ''
    timeframe = config['timeframe']
    par = str(config['cpu']) + '-' + str(config['memory']) + '-' + str(config['price'])
    datetimeobj = datetime.datetime.now()
    metric_id = datetimeobj.strftime('%d-%b-%Y (%H:%M:%S.%f)')
    query.store_metric_conf(metric_id, metric_name, timeframe,
                            par, template_name)
    return {
        'total': 1,
        'results': f'RatingRuleValues {config["metric_name"]} stored'
    }


@templates_routes.route('/templates/instance/add', methods=['POST'])
def models_db_instance_add():
    """Save the history of the RatingRuleInstances in database."""
    config = request.form or request.get_json()
    instance_name = config['metric_name']
    instance_promql = config['promql']
    datetimeobj = datetime.datetime.now()
    start_time = datetimeobj.strftime('%d-%b-%Y (%H:%M:%S.%f)')
    end_time = None
    body_spec = {}
    config_vars = {'cpu', 'memory', 'price'}
    for key in config_vars:
        body_spec[key] = config[key]
    instance_values = str(body_spec)
    query.store_instance_conf(instance_name, instance_promql,
                              start_time, end_time, instance_values)
    return {
        'total': 1,
        'results': f'RatingRuleInstance {config["metric_name"]} stored'
    }


@templates_routes.route('/templates/instance/delete', methods=['POST'])
def models_db_instance_delete():
    """Delete RatingRuleInstances in database."""
    config = request.form or request.get_json()
    instance_name = config['metric_name']
    datetimeobj = datetime.datetime.now()
    end_time = datetimeobj.strftime('%d-%b-%Y (%H:%M:%S.%f)')
    query.delete_instance_conf(instance_name, end_time)
    return {
        'total': 1,
        'results': f'RatingRuleInstance {config["metric_name"]} stored'
    }
