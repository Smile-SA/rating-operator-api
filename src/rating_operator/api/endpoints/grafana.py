import datetime
import logging
import os
from typing import AnyStr, Dict, List

from flask import Blueprint, current_app, jsonify, make_response, request, url_for
from flask import Response

from rating_operator.api.config import envvar

import requests

import werkzeug


grafana_routes = Blueprint('grafana', __name__)


def get_backend_url() -> AnyStr:
    """Return the homepage Grafana url."""
    return envvar('GRAFANA_BACKEND_URL')


def format_grafana_frontend_request(url: AnyStr) -> AnyStr:
    """
    Format the URL for a frontend request toward Grafana.

    :url (AnyStr) A string representing the destination of the request.

    Return the formatted url.
    """
    protocol = 'https' if os.environ.get('AUTH', 'false') == 'true' else 'http'
    grafana_frontend_url = envvar('FRONTEND_URL')
    return f'{protocol}://{grafana_frontend_url}{url}'


def format_grafana_admin_request(url: AnyStr) -> AnyStr:
    """
    Format the URL for an administrator request toward Grafana.

    :url (AnyStr) A string representing the destination of the request.

    Return the formatted URL.
    """
    admin_password = envvar('GRAFANA_ADMIN_PASSWORD')
    admin_name = os.environ.get('ADMIN_ACCOUNT', 'admin')
    protocol = 'https' if os.environ.get('AUTH', 'false') == 'true' else 'http'
    grafana_backend_url = get_backend_url()
    return f'{protocol}://{admin_name}:{admin_password}@{grafana_backend_url}{url}'


def get_grafana_dashboards_url(admin: bool) -> List[Dict]:
    """
    Get a list of dashboard available to the tenant.

    :admin (bool) A boolean representing admin status.

    Return a list of dashboards dictionaries.
    """
    urls = []
    req = format_grafana_admin_request('/api/search?query=[Grafonnet]')
    results = requests.get(req).json()
    for item in results:
        folder_title = item.get('folderTitle')
        if admin or not folder_title or folder_title != 'admin':
            item['url'] = format_grafana_frontend_request(item['url'])
            urls.append(item)
    return urls


def get_grafana_users() -> Dict:
    """Return the grafana users."""
    users = {}
    try:
        req = format_grafana_admin_request('/api/users')
        response = requests.get(req)
        for user in response.json():
            users[user['login']] = user['id']
    except requests.exceptions.RequestException as exc:
        raise exc
    return users


def get_grafana_user(tenant: AnyStr) -> AnyStr:
    """
    Return the grafana user that matches the tenant.

    :tenant (AnyStr) A string representing the tenant.
    """
    return get_grafana_users()[tenant]


def logout_grafana_user(tenant):
    grafana_id = get_grafana_user(tenant)
    try:
        req = format_grafana_admin_request(f'/api/admin/users/{grafana_id}/logout')
        requests.post(req)
    except requests.exceptions.RequestException as exc:
        raise exc


def unallowed_routes() -> tuple:
    """
    Define the unallowed routes in Grafana.

    Return the unallowed routes in Grafana.
    """
    return [
        '/',
        '/search',
        '/query',
        '/annotations',
        '/alive',
        '/rules_metrics',
        '/rating/configs/list',
        '/rating/configs/<timestamp>',
        '/presto/<table>/columns',
        '/presto/<table>/frames',
        '/signup',
        '/login',
        '/logout',
        '/current',
        '/tenant',
        '/tenants',
        '/static/<path:filename>',
        '/models/get'
    ]


def create_grafana_user(tenant: AnyStr, password: AnyStr):
    """
    Create the Grafana user that corresponds to the rating operator tenant.

    :tenant (AnyStr) the user username
    :password (AnyStr) the user password
    """
    payload = {
        'name': tenant,
        'login': tenant,
        'password': password
    }
    if len(password) < 4:
        logging.error('The user password must be more than 4 characters in Grafana')
    req = format_grafana_admin_request('/api/admin/users')
    requests.post(req, data=payload)


def update_grafana_password(grafana_id: AnyStr, password: AnyStr):
    """
    Update the Grafana user password.

    :grafana_id (AnyStr) the tenant user ID in Grafana
    :password (AnyStr) the user password
    """
    payload = {
        'password': password
    }
    try:
        req = format_grafana_admin_request(f'/api/admin/users/{grafana_id}/password')
        requests.put(req, data=payload)
    except requests.exceptions.RequestException as exc:
        raise exc


def update_grafana_role(grafana_id: AnyStr, role: AnyStr):
    """
    Update the Grafana role.

    :grafana_id (AnyStr) the tenant user ID in Grafana
    :role (AnyStr) the user role expected to changed
    """
    org_id = 1
    payload = {
        'role': role
    }
    try:
        req = format_grafana_admin_request(f'/api/orgs/{org_id}/users/{grafana_id}')
        requests.patch(req, data=payload)
    except requests.exceptions.RequestException as exc:
        raise exc


def login_grafana_user(tenant: AnyStr, password: AnyStr) -> AnyStr:
    """
    Login the user in Grafana.

    :tenant (AnyStr) the user username
    :password (AnyStr) the user password
    """
    payload = {
        'user': tenant,
        'password': password
    }
    try:
        s = requests.Session()
        protocol = 'https' if os.environ.get('AUTH', 'false') == 'true' else 'http'
        url = f'{protocol}://{get_backend_url()}/login'
        s.post(url, data=payload)
        return s.cookies.get_dict().get('grafana_session')
    except requests.exceptions.RequestException as exc:
        raise exc


@grafana_routes.route('/')
def grafana_routes_handshake() -> Response:
    """Test handshake for the rating operator API."""
    return make_response('Succesful handshake', 200)


@grafana_routes.route('/search', methods=['GET', 'POST'])
def search_grafana_routes() -> jsonify:
    """
    Search the url for the Grafana Query.

    Return the endpoint if the Grafana route is allowed.
    """
    req = request.get_json()

    links = []
    for rule in current_app.url_map.iter_rules():
        string_rule = str(rule)
        if string_rule in unallowed_routes() \
           or not string_rule.endswith('/rating') and req.get('type') == 'timeseries':
            continue
        elif 'GET' in rule.methods and req['target'] in string_rule:
            endpoint = {
                'text': string_rule,
                'value': string_rule
            }
            links.append(endpoint)
    return jsonify(links)


@grafana_routes.route('/query', methods=['GET', 'POST'])
def query_metrics() -> Response:
    """Return the result of the Grafana Query."""
    req = request.get_json()

    payload = []
    time_range = {
        'start': req['range']['from'].replace('T', ' '),
        'end': req['range']['to'].replace('T', ' ')
    }

    for target in req['targets']:
        if not target.get('target'):
            continue

        route = find_matching_route(target['target'])
        if not route:
            continue

        params = {**time_range, **strip_unused_keys(route, target.get('data') or {})}
        try:
            url = url_for(route, **params)
        except werkzeug.routing.BuildError as exc:
            return make_response(str(exc), 422)
        results = requests.get(
            f'http://localhost:5012{url}',
            cookies=request.cookies).json()['results']
        if not results:
            continue

        responses = {
            'table': format_table_response,
            'timeseries': format_timeserie_response
        }[target['type']](results, additionnal=params)

        for response in responses:
            payload.append(response)
    if payload:
        return make_response(jsonify(payload), 200)
    return make_response('No metric found', 404)


def strip_unused_keys(route, params):
    for key in list(params.keys()):
        if key not in route:
            del params[key]
    return params


def match_key_type(key):
    try:
        return {
            'frame_begin': 'time',
            'frame_end': 'time',
            'metric': 'string',
            'tenant': 'string',
            'pod': 'string',
            'node': 'string',
            'namespace': 'string'
        }[key]
    except KeyError:
        return 'number'


def format_table_response(content, additionnal={}):
    columns = []
    if len(content) > 0:
        for key in content[0].keys():
            columns.append({
                'text': key,
                'type': match_key_type(key)
            })

    return [{
        'columns': columns,
        'rows': [list(metric.values()) for metric in content],
        'type': 'table'
    }]


def to_timestamp(epoch):
    return int(
        datetime.datetime.strptime(epoch, '%a, %d %b %Y %H:%M:%S GMT').timestamp() * 1000)


def format_timeserie_response(content, additionnal={}):
    data = {}
    response = []
    for row in content:
        if 'frame_begin' not in row.keys():
            return response

        sort = {}
        for index in ['node', 'namespace', 'pod', 'metric']:
            if index not in row or index in additionnal:
                continue
            sort[index] = row[index]

        label_keys = list(sort.keys())
        if len(label_keys) == 1:
            label = sort[label_keys[0]]
        else:
            label = str(sort)
        if label not in data:
            data[label] = []

        data[label].append(
            [
                row.get('frame_price', 1),
                to_timestamp(row['frame_begin'])
            ])

    for key in data.keys():
        response.append({
            'target': key,
            'datapoints': data[key]
        })
    return response


def find_matching_route(target):
    for rule in current_app.url_map.iter_rules():
        if str(rule) == target:
            return rule.endpoint
    return None
