import json
from typing import AnyStr, Dict, List

from flask import Blueprint, jsonify, make_response, request
from flask.wrappers import Response

from flask_json import as_json


from rating_operator.api.check import assert_url_params, request_params
from rating_operator.api.endpoints.auth import with_session
from rating_operator.api.queries import frames as query
from rating_operator.api.secret import require_admin
from rating_operator.api.write_frames import write_rated_frames


frames_routes = Blueprint('frames', __name__)


@frames_routes.route('/presto/<table>/columns')
@as_json
@require_admin
@assert_url_params
def table_columns(table: AnyStr) -> Response:
    """
    List table columns.

    :table (AnyStr) A string representing the table.

    Return a response or nothing.
    """
    rows = query.get_table_columns(table)
    return {
        'total': len(rows),
        'results': rows
    }


@frames_routes.route('/presto/<table>/frames')
@as_json
@require_admin
@assert_url_params
def unrated_frames(table: AnyStr) -> Response:
    """
    Get unrated frames from presto.

    :table (AnyStr) A string representing the table.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_unrated_frames(
        table=table,
        column=config['column'],
        labels=config['labels'],
        start=config['start'],
        end=config['end'])
    return {
        'total': len(rows),
        'results': rows
    }


def dict_to_list(frames: Dict) -> List[List]:
    """
    Transform a frames dictionary to a list of list.

    :frames (Dict) A dictionary containing the frames.

    Returns the frames as a list of list.
    """
    return [[
        frame['start'],
        frame['end'],
        frame['namespace'],
        frame['node'],
        frame['metric'],
        frame['pod'],
        frame['quantity'],
        frame['quantity'],
        frame['labels']
    ] for frame in json.loads(frames)]


@frames_routes.route('/models/frames/add', methods=['POST'])
@require_admin
def models_frames_add() -> Response:
    """Add models frames to database."""
    received = request.form.to_dict()
    rated_frames = received['rated_frames']
    write_rated_frames(frames=dict_to_list(rated_frames))
    query.update_rated_metrics_object(
        metric=received['metric'],
        last_insert=received['last_insert'])
    return make_response(jsonify(message='models frames added'), 200)


@frames_routes.route('/rated/frames/add', methods=['POST'])
@as_json
@require_admin
def rated_frames_add() -> Response:
    """Add rated frames to database."""
    received = request.get_json()
    write_rated_frames(frames=received['rated_frames'])
    query.update_rated_namespaces(
        namespaces=received['rated_namespaces'],
        last_insert=received['last_insert'])
    query.update_rated_metrics(
        metric=received['metric'],
        report_name=received['report_name'],
        last_insert=received['last_insert'])
    query.update_rated_metrics_object(
        metric=received['metric'],
        last_insert=received['last_insert'])
    return {
        'total': 1,
        'results': 1
    }


@frames_routes.route('/rated/frames/delete', methods=['POST'])
@as_json
@require_admin
def rated_frames_delete() -> Response:
    """Remove rated frames from database."""
    received = request.get_json()
    rows = query.delete_rated_frames(metric=received['metric'])
    return {
        'total': query.clear_rated_metrics(metric=received['metric']),
        'results': rows
    }


@frames_routes.route('/rated/frames/oldest')
@with_session
def rated_frames_oldest(tenant: AnyStr) -> Response:
    """
    Get the oldest rated frames.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    rows = query.get_rated_frames_oldest(tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }
