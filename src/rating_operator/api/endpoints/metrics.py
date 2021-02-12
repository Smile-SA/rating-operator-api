from typing import AnyStr

from flask import Blueprint, make_response, request
from flask.wrappers import Response

from rating_operator.api import config
from rating_operator.api.check import request_params
from rating_operator.api.queries import metrics as query

from .auth import with_session

metrics_routes = Blueprint('metrics', __name__)


@metrics_routes.route('/alive')
def ping() -> AnyStr:
    """Healthcheck."""
    return "I'm alive!"


@metrics_routes.route('/metrics')
@with_session
def metrics(tenant: AnyStr) -> Response:
    """
    Get metrics wrapper.

    :tenant (AnyStr) A string representing the tenant.

    Return a response of nothing.
    """
    rows = query.get_metrics(tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/metrics/<metric>/max')
@with_session
def metric_rating_max(metric: AnyStr, tenant: AnyStr) -> Response:
    """
    Get the max rating for a given metric.

    :metric (AnyStr) A string representing the metric.
    :tenant (AnyStr) A string representing the tenant.

    Return a response of nothing.
    """
    config = request_params(request.args)
    rows = query.get_metric_rating_max(metric=metric,
                                       start=config['start'],
                                       end=config['end'],
                                       tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/metrics/<metric>/ratio')
@with_session
def metric_ratio(metric: AnyStr, tenant: AnyStr) -> Response:
    """
    Get the rating as a ratio per instance, for a given metric.

    :metric (AnyStr) A string representing the metric.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_metric_ratio(metric=metric,
                                  start=config['start'],
                                  end=config['end'],
                                  tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/metrics/<metric>/<aggregator>')
@with_session
def metric_rating_aggregator(metric: AnyStr,
                             aggregator: AnyStr,
                             tenant: AnyStr) -> Response:
    """
    Get the rating for a given metric and aggregator (daily, weekly, monthly).

    :metric (AnyStr) A string representing the metric.
    :tenant (AnyStr) A string representing the tenant.

    Return a response of nothing.
    """
    params = {
        'tenant_id': tenant
    }

    if metric != 'rating':
        params.update({'metric': metric})

    try:
        rows = {
            (metric, 'daily'): query.get_metric_daily_rating,
            (metric, 'weekly'): query.get_metric_weekly_rating,
            (metric, 'monthly'): query.get_metric_monthly_rating,
            ('rating', 'daily'): query.get_metrics_rating_daily,
            ('rating', 'weekly'): query.get_metrics_rating_weekly,
            ('rating', 'monthly'): query.get_metrics_rating_monthly,
        }[(metric, aggregator)](**params)
    except KeyError:
        rows = [{}]
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/metrics/<metric>/rating')
@with_session
def metric_rating(metric: AnyStr, tenant: AnyStr) -> Response:
    """
    Get the rating for a given metric.

    :metric (AnyStr) A string representing the metric.
    :tenant (AnyStr) A string representing the tenant.

    Return a response of nothing.
    """
    config = request_params(request.args)
    rows = query.get_metric_rating(metric=metric,
                                   start=config['start'],
                                   end=config['end'],
                                   tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/metrics/<metric>/total_rating')
@with_session
def metric_total_rating(metric: AnyStr, tenant: AnyStr) -> Response:
    """
    Get the aggragated rating for a given metric.

    :metric (AnyStr) A string representing the metric.
    :tenant (AnyStr) A string representing the tenant.

    Return a response of nothing.
    """
    config = request_params(request.args)
    rows = query.get_metric_total_rating(metric=metric,
                                         start=config['start'],
                                         end=config['end'],
                                         tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/rules_metrics')
def config_lookup_export() -> Response:
    """Export RatingRules for Prometheus."""
    rows = config.generate_rules_export()
    return make_response('\n'.join(rows) + '\n# EOF\n', 200)


@metrics_routes.route('/metrics/rating')
@with_session
def metrics_rating(tenant: AnyStr) -> Response:
    """
    Get the rating per metrics.

    :tenant (AnyStr) A string representing the namespace.

    Return a response or nothing.
    """
    config = request_params(request.args)
    rows = query.get_metrics_rating(start=config['start'],
                                    end=config['end'],
                                    tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }


@metrics_routes.route('/metrics/<metric>/todate')
@with_session
def metrics_to_date(metric: AnyStr, tenant: AnyStr) -> Response:
    """
    Get the rating, from the start of the month to now.

    :metric (AnyStr) A string representing the metric.
    :tenant (AnyStr) A string representing the tenant.

    Return a response or nothing.
    """
    rows = query.get_metric_to_date(metric=metric,
                                    tenant_id=tenant)
    return {
        'total': len(rows),
        'results': rows
    }
