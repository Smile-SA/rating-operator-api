from typing import Dict

from flask import Blueprint, abort, make_response, request
from flask.wrappers import Response

from flask_json import as_json

from kubernetes import client
from kubernetes.client.rest import ApiException

from rating_operator.api.secret import get_client, require_admin


prometheus_routes = Blueprint('prometheus', __name__)


def prometheus_object() -> Dict:
    """Get a basic Prometheus object."""
    return {
        'group': 'monitoring.coreos.com',
        'version': 'v1',
        'plural': 'prometheusrules',
        'name': 'prometheus-rating.rules',
        'namespace': 'monitoring'
    }


@prometheus_routes.route('/prometheus/get')
@as_json
def prometheus_config_get() -> Response:
    """Get the rating PrometheusRule."""
    try:
        api = client.CustomObjectsApi(get_client())
        response = api.get_namespaced_custom_object(**prometheus_object())
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return {
        'results': response,
        'total': len(response['spec']['groups'])
    }


@prometheus_routes.route('/prometheus/add', methods=['POST'])
@require_admin
def prometheus_metric_add():
    """Add a rule to the rating PrometheusRule."""
    api = client.CustomObjectsApi(get_client())
    prom_object = api.get_namespaced_custom_object(**prometheus_object())
    found = False
    payload = {
        'expr': request.form['expr'],
        'record': request.form['record']
    }
    for group in prom_object['spec']['groups']:
        if group['name'] == request.form['group']:
            if payload in group['rules']:
                abort(make_response('Metric already exist', 400))
            group['rules'].append(payload)
            found = True
    if not found:
        prom_object['spec']['groups'].append({
            'name': request.form['group'],
            'rules': [payload]
        })
    try:
        api.patch_namespaced_custom_object(**prometheus_object(), body=prom_object)
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response('Metric added', 200)


@prometheus_routes.route('/prometheus/edit', methods=['POST'])
@require_admin
def prometheus_metric_edit() -> Response:
    """Edit a rule to the rating PrometheusRule."""
    api = client.CustomObjectsApi(get_client())
    prom_object = api.get_namespaced_custom_object(**prometheus_object())
    for group in prom_object['spec']['groups']:
        if group['name'] == request.form['group']:
            for rule in group['rules']:
                if rule['record'] == request.form['record']:
                    rule['expr'] = request.form['expr']
    try:
        api.patch_namespaced_custom_object(**prometheus_object(), body=prom_object)
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response('Metric edited', 200)


@prometheus_routes.route('/prometheus/delete', methods=['POST'])
@require_admin
def prometheus_metric_delete() -> Response:
    """Delete a rule to the rating PrometheusRule."""
    api = client.CustomObjectsApi(get_client())
    prom_object = api.get_namespaced_custom_object(**prometheus_object())
    for group in prom_object['spec']['groups']:
        if group['name'] == request.form['group']:
            for rule in group['rules']:
                if rule['record'] == request.form['record']:
                    group['rules'].remove(rule)
    try:
        api.patch_namespaced_custom_object(**prometheus_object(), body=prom_object)
    except ApiException as exc:
        abort(make_response(str(exc), 400))
    return make_response('Metric removed', 200)
