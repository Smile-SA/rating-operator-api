from typing import AnyStr

from flask import Blueprint, request
from flask.wrappers import Response

from rating_operator.api.check import request_params
from rating_operator.api.queries import nodes as query

from .auth import with_session


nodes_routes = Blueprint('nodes', __name__)


@nodes_routes.route('/nodes')
@with_session
def nodes(tenant: AnyStr) -> Response:
    """
    Get nodes.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    rows = query.get_nodes(tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/rating')
@with_session
def nodes_rating(tenant: AnyStr) -> Response:
    """
    Get nodes rating.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_nodes_rating(start=config['start'],
                                  end=config['end'],
                                  tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/<node>/<aggregator>')
@with_session
def nodes_rating_agg(tenant: AnyStr,
                     node: AnyStr,
                     aggregator: AnyStr) -> Response:
    """
    Get the nodes rating by time aggregation.

    :tenant (AnyStr) A string representing the tenant.
    :node (AnyStr) A string representing the node name.
    :aggregator (AnyStr) A string used as an aggregator.

    Return a response or nothing.
    """
    params = {
        'tenant_id': tenant,
    }

    if node != 'rating':
        params.update({'node': node})

    try:
        rows = {
            (node, 'daily'): query.get_node_rating_daily,
            (node, 'weekly'): query.get_node_rating_weekly,
            (node, 'monthly'): query.get_node_rating_monthly,
            ('rating', 'daily'): query.get_nodes_rating_daily,
            ('rating', 'weekly'): query.get_nodes_rating_weekly,
            ('rating', 'monthly'): query.get_nodes_rating_monthly,
        }[(node, aggregator)](**params)
    except KeyError:
        rows = [{}]
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/metrics/rating')
@with_session
def nodes_metrics_rating(tenant: AnyStr) -> Response:
    """
    Get the nodes metrics.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_nodes_metrics_rating(start=config['start'],
                                          end=config['end'],
                                          tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/total_rating')
@with_session
def nodes_total_rating(tenant: AnyStr) -> Response:
    """
    Get the nodes agglomerated rating.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_nodes_total_rating(start=config['start'],
                                        end=config['end'],
                                        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/<node>/rating')
@with_session
def node_rating(node: AnyStr, tenant: AnyStr) -> Response:
    """
    Get the rating for a given node.

    :node (AnyStr) A string representing the node.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_node_rating(node=node,
                                 start=config['start'],
                                 end=config['end'],
                                 tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/<node>/total_rating')
@with_session
def node_total_rating(node: AnyStr, tenant: AnyStr) -> Response:
    """
    Get the agglomerated rating for the nodes.

    :node (AnyStr) A string representing the node.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_node_total_rating(node=node,
                                       start=config['start'],
                                       end=config['end'],
                                       tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@nodes_routes.route('/nodes/metrics/<metric>/rating')
@with_session
def nodes_metric_rating(metric: AnyStr, tenant: AnyStr) -> Response:
    """
    Get the rating for a given metric.

    :metric (AnyStr) A string representing the namespace.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_nodes_metric_rating(metric=metric,
                                         start=config['start'],
                                         end=config['end'],
                                         tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }
