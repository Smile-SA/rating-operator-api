from typing import AnyStr

from flask import Blueprint, request
from flask.wrappers import Response

from rating_operator.api.check import request_params
from rating_operator.api.queries import pods as query

from .auth import with_session


pods_routes = Blueprint('pods', __name__)


@pods_routes.route('/pods')
@with_session
def pods(tenant: AnyStr) -> Response:
    """
    Get pods.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_pods(start=config['start'],
                          end=config['end'],
                          tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/lifetime')
@with_session
def pod_lifetime(pod: AnyStr, tenant: AnyStr) -> Response:
    """
    Get pod lifetime.

    :pod (AnyStr) A string representing the pod.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    rows = query.get_pod_lifetime(pod=pod,
                                  tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/metrics/rating')
@with_session
def pods_metrics_rating(tenant: AnyStr) -> Response:
    """
    Get pods rating, by metrics.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_pods_metrics_rating(start=config['start'],
                                         end=config['end'],
                                         tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/rating')
@with_session
def pods_rating(tenant: AnyStr) -> Response:
    """
    Get pods rating.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_pods_rating(start=config['start'],
                                 end=config['end'],
                                 tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/total_rating')
@with_session
def pods_total_rating(tenant: AnyStr) -> Response:
    """
    Get pods aggregated rating.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_pods_total_rating(start=config['start'],
                                       end=config['end'],
                                       tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/<aggregator>')
@with_session
def pods_rating_agg(tenant: AnyStr,
                    pod: AnyStr,
                    aggregator: AnyStr) -> Response:
    """
    Get the pods rating by time aggregation.

    :tenant (AnyStr) A string representing the tenant.
    :pod (AnyStr) A string representing the pod name.
    :aggregator (AnyStr) A string used as an aggregator.

    Return a response or nothing.
    """
    params = {
        'tenant_id': tenant,
    }

    if pod != 'rating':
        params.update({'pod': pod})

    try:
        rows = {
            (pod, 'daily'): query.get_pod_rating_daily,
            (pod, 'weekly'): query.get_pod_rating_weekly,
            (pod, 'monthly'): query.get_pod_rating_monthly,
            ('rating', 'daily'): query.get_pods_rating_daily,
            ('rating', 'weekly'): query.get_pods_rating_weekly,
            ('rating', 'monthly'): query.get_pods_rating_monthly,
        }[(pod, aggregator)](**params)
    except KeyError:
        rows = [{}]
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/rating/daily')
@with_session
def pods_rating_daily(tenant: AnyStr) -> Response:
    """
    Get pods daily rating.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    rows = query.get_pods_rating_daily(tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/rating/weekly')
@with_session
def pods_rating_weekly(tenant: AnyStr) -> Response:
    """
    Get pods weekly rating.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    rows = query.get_pods_rating_weekly(tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/rating/monthly')
@with_session
def pods_rating_monthly(tenant: AnyStr) -> Response:
    """
    Get pods monthly rating.

    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    rows = query.get_pods_rating_monthly(tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/rating')
@with_session
def pod_rating(pod: AnyStr, tenant: AnyStr) -> Response:
    """
    Get a pod's rating.

    :pod (AnyStr) A string representing the pod.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_pod_rating(pod=pod,
                                start=config['start'],
                                end=config['end'],
                                tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/total_rating')
@with_session
def pod_total_rating(pod: AnyStr, tenant: AnyStr) -> Response:
    """
    Get a pod's aggregated rating.

    :pod (AnyStr) A string representing the pod.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_pod_total_rating(pod=pod,
                                      start=config['start'],
                                      end=config['end'],
                                      tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/metrics/<metric>/rating')
@with_session
def pod_metric_rating(pod: AnyStr,
                      metric: AnyStr,
                      tenant: AnyStr) -> Response:
    """
    Get the rating for a given pod and metric.

    :pod (AnyStr) A string representing the pod.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_pod_metric_rating(pod=pod,
                                       metric=metric,
                                       start=config['start'],
                                       end=config['end'],
                                       tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@pods_routes.route('/pods/<pod>/metrics/<metric>/total_rating')
@with_session
def pod_metric_total_rating(pod: AnyStr,
                            metric: AnyStr,
                            tenant: AnyStr) -> Response:
    """
    Get the aggregated rating for a given pod and metric.

    :pod (AnyStr) A string representing the pod.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_pod_metric_total_rating(pod=pod,
                                             metric=metric,
                                             start=config['start'],
                                             end=config['end'],
                                             tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }
