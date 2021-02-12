from typing import AnyStr

from flask import Blueprint, request
from flask.wrappers import Response

from flask_json import as_json

from rating_operator.api.check import request_params
from rating_operator.api.endpoints.auth import with_session
from rating_operator.api.queries import namespaces as query
from rating_operator.api.secret import require_admin


namespaces_routes = Blueprint('namespaces', __name__)


@namespaces_routes.route('/namespaces')
@with_session
def namespaces(tenant: AnyStr) -> Response:
    """
    Get namespaces.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    rows = query.get_namespaces(tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/tenant', methods=['POST'])
@as_json
@require_admin
def update_namespace_tenant() -> Response:
    """Get tenants and namespaces."""
    config = request.get_json()
    rows = query.update_namespace(namespace=config['namespace'],
                                  tenant_id=config['tenant_id'])
    return {
        'total': 1,
        'results': rows
    }


@namespaces_routes.route('/namespaces/<namespace>/<aggregator>')
@with_session
def namespace_rating_aggregator(namespace: AnyStr,
                                aggregator: AnyStr,
                                tenant: AnyStr):
    """
    Get the rating for a given namespace and aggregator (daily, weekly, monthly).

    :namespace (AnyStr) A string representing the namespace.
    :tenant (AnyStr) A string representing the tenant.

    Return a response of nothing.
    """
    params = {
        'tenant_id': tenant
    }

    if namespace != 'rating':
        params.update({'namespace': namespace})

    try:
        rows = {
            (namespace, 'daily'): query.get_namespace_rating_daily,
            (namespace, 'weekly'): query.get_namespace_rating_weekly,
            (namespace, 'monthly'): query.get_namespace_rating_monthly,
            ('rating', 'daily'): query.get_namespaces_rating_daily,
            ('rating', 'weekly'): query.get_namespaces_rating_weekly,
            ('rating', 'monthly'): query.get_namespaces_rating_monthly,
        }[(namespace, aggregator)](**params)
    except KeyError:
        rows = [{}]
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/rating')
@with_session
def namespaces_rating(tenant: AnyStr) -> Response:
    """
    Get namespaces rating.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_namespaces_rating(
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/total_rating')
@with_session
def namespaces_total_rating(tenant: AnyStr) -> Response:
    """
    Get namespaces aggregated rating.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_namespaces_total_rating(
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/metrics/rating')
@with_session
def namespaces_metrics(tenant: AnyStr) -> Response:
    """
    Get namespaces metrics.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_namespaces_metrics_rating(start=config['start'],
                                               end=config['end'],
                                               tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/<namespace>/rating')
@with_session
def namespace_rating(namespace: AnyStr, tenant: AnyStr) -> Response:
    """
    Get the rating of a namespace.

    :namespace (AnyStr) A string representing the tenant.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_namespace_rating(
        namespace=namespace,
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/<namespace>/total_rating')
@with_session
def namespace_total_rating(namespace: AnyStr, tenant: AnyStr) -> Response:
    """
    Get the aggregated rating of a namespace.

    :namespace (AnyStr) A string representing the tenant.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_namespace_total_rating(
        namespace=namespace,
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@namespaces_routes.route('/namespaces/<namespace>/metrics/<metric>/rating')
@with_session
def namespace_metric_rating(namespace: AnyStr,
                            metric: AnyStr,
                            tenant: AnyStr) -> Response:
    """
    Get the rating of a given namespace and metric.

    :namespace (AnyStr) A string representing the namespace.
    :metric (AnyStr) A string representing the metric.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_namespace_metric_rating(
        namespace=namespace,
        metric=metric,
        start=config['start'],
        end=config['end'],
        tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }
