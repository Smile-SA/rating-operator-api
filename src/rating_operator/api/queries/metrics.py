from typing import AnyStr, Dict, List

from rating_operator.api.check import date_checker_start_end, multi_tenant
from rating_operator.api.db import db
from rating_operator.api.utils import process_query
from rating_operator.api.utils import process_query_get_count

import sqlalchemy as sa
from sqlalchemy.sql.expression import bindparam


def get_tenants() -> List[Dict]:
    """
    Get tenants from namespaces table.

    Return a list of dictionary containing the results.
    """
    qry = sa.text("""
        SELECT tenant_id
        FROM namespaces
        GROUP BY tenant_id
        ORDER BY tenant_id
    """)
    return process_query(qry, {})


def get_tenant_namespace(tenant: AnyStr) -> List[Dict]:
    """
    Get the namespace of a tenant in the namespaces table.

    :tenant (AnyStr) A name reprensenting the tenant.

    Return a list of dictionary containing the results.
    """
    qry = sa.text("""
        SELECT namespace
        FROM namespaces
        WHERE tenant_id = :tenant
        ORDER BY namespace
    """)
    params = {
        'tenant': tenant
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_metric_rating(metric: AnyStr,
                      start: AnyStr,
                      end: AnyStr,
                      tenant_id: AnyStr,
                      namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating value on a timeframe for a given metric.

    :metric (AnyStr) A string representing the metric.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                count(node) as node_count,
                metric,
                sum(frame_price) as price
        FROM frames
        WHERE metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric
        ORDER BY frame_begin
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_metric_total_rating(metric: AnyStr,
                            start: AnyStr,
                            end: AnyStr,
                            tenant_id: AnyStr,
                            namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the agglomerated rating value for a given metric.

    :metric (AnyStr) A string representing the metric.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT sum(frame_price) as frame_price,
                                   metric
        FROM frames
        WHERE metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY metric
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_metrics(tenant_id: AnyStr, namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get a list of the metrics.

    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT metric
        FROM frames
        WHERE namespace IN :namespaces
        GROUP BY metric
        ORDER BY metric
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


def get_metric_report(metric: AnyStr, tenant_id: AnyStr) -> List[Dict]:
    """
    Get the report associated with a metric.

    :metric (AnyStr) A name representing the metric.
    :tenant_id (AnyStr) A name representing the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT report_name
        FROM frame_status
        WHERE metric = :metric
    """)

    params = {
        'metric': metric,
        'tenant_id': tenant_id
    }
    return process_query(qry, params)


def get_last_rated_date(metric: AnyStr, tenant_id: AnyStr) -> List[Dict]:
    """
    Get the latest rating timestamp for a metric.

    :metric (AnyStr) A name representing the metric.
    :tenant_id (AnyStr) A name representing the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT last_insert
        FROM frame_status
        WHERE metric = :metric
    """)

    params = {
        'metric': metric,
        'tenant_id': tenant_id
    }
    return process_query(qry, params)


def get_report_metric(report: AnyStr, tenant_id: AnyStr) -> List[Dict]:
    """
    Get the metric of a report.

    :report (AnyStr) A name representing the report.
    :tenant_id (AnyStr) A name representing the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT metric
        FROM frame_status
        WHERE report_name = :report
    """)

    params = {
        'report': report
    }
    return process_query(qry, params)


def get_last_rated_reports(report: AnyStr, tenant_id: AnyStr) -> List[Dict]:
    """
    Get the time of the last update of a given report.

    :report (AnyStr) A name representing the report.
    :tenant_id (AnyStr) A name representing the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT last_insert
        FROM frame_status
        WHERE report_name = :report
    """)

    params = {
        'report': report
    }
    return process_query(qry, params)


@multi_tenant
def get_metric_rating_max(metric: AnyStr,
                          start: AnyStr,
                          end: AnyStr,
                          tenant_id: AnyStr,
                          namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the max value for the given period and metric.

    :metric (AnyStr) A string representing the metric.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                ceil(max(frame_price)) as frame_price
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end < :end
        AND metric = :metric
        AND namespace IN :namespaces
        GROUP BY frame_begin
        ORDER BY frame_begin
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_metric_instance(metric: AnyStr,
                        start: AnyStr,
                        end: AnyStr,
                        tenant_id: AnyStr,
                        namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the max value for the given metric.

    :metric (AnyStr) A string representing the metric.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text(f"""
        SELECT  frame_begin,
                ceil(max(frame_price)) as {metric}
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end < :end
        AND metric = :metric
        AND namespace IN :namespaces
        GROUP BY frame_begin
        ORDER BY frame_begin
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_metric_ratio(metric: AnyStr,
                     start: AnyStr,
                     end: AnyStr,
                     tenant_id: AnyStr,
                     namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the ratio between nodes and price for the given metric.

    :metric (AnyStr) A string representing the metric.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) / count(node) as ratio,
                metric
        FROM frames
        WHERE metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric
        ORDER BY frame_begin
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_metric_daily_rating(metric: AnyStr,
                            tenant_id: AnyStr,
                            namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the daily price for a metric.

    :metric (AnyStr) A string representing the metric.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT max(frame_price) * 24 AS frame_price
        FROM frames
        WHERE metric = :metric
        AND frame_begin >= date_trunc('day', now())
        AND namespace IN :namespaces
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'metric': metric,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_metric_weekly_rating(metric: AnyStr,
                             tenant_id: AnyStr,
                             namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the weekly price for a metric.

    :metric (AnyStr) A string representing the metric.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  max(frame_price) * 24 * 7 AS frame_price
        FROM frames
        WHERE metric = :metric
        AND frame_begin >= date_trunc('week', now())
        AND namespace IN :namespaces
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'metric': metric,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_metric_monthly_rating(metric: AnyStr,
                              tenant_id: AnyStr,
                              namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the monthly price for a metric.

    :metric (AnyStr) A string representing the metric.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  max(frame_price) * 24 *
        (SELECT extract(days FROM
         date_trunc('month', now()) + interval '1 month - 1 day'))
        AS frame_price
        FROM frames
        WHERE metric = :metric
        AND frame_begin >= date_trunc('month', now())
        AND namespace IN :namespaces
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'metric': metric,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_metrics_rating(start: AnyStr,
                       end: AnyStr,
                       tenant_id: AnyStr,
                       namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating for metrics.

    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                metric
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end < :end
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric
        ORDER BY frame_begin, metric
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_metrics_rating_daily(tenant_id: AnyStr, namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the daily rating of metrics.

    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                metric
        FROM frames
        WHERE frame_begin >= date_trunc('day', now())
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric
        ORDER BY frame_begin, metric
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_metrics_rating_weekly(tenant_id: AnyStr, namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the weekly rating of metrics.

    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                metric
        FROM frames
        WHERE frame_begin >= date_trunc('week', now())
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric
        ORDER BY frame_begin, metric
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_metrics_rating_monthly(tenant_id: AnyStr,
                               namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the monthly rating of metrics.

    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                metric
        FROM frames
        WHERE frame_begin >= date_trunc('month', now())
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric
        ORDER BY frame_begin, metric
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_metric_to_date(metric: AnyStr,
                       tenant_id: AnyStr,
                       namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating of a metric, from start of the month to now.

    :metric (AnyStr) A string representing the metric.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT max(frame_price) *
               (SELECT
                    (EXTRACT(EPOCH from now()) -
                     EXTRACT(EPOCH from date_trunc('month', now()))) / 3600)
        AS frame_price
        FROM frames
        WHERE frame_begin >= date_trunc('month', now())
        AND metric = :metric
        AND namespace IN :namespaces
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'metric': metric,
        'namespaces': namespaces
    }
    return process_query(qry, params)


def store_template_conf(t_id, t_name, t_group, t_query, t_var):
    qry = sa.text("""
        INSERT INTO template (id, t_name, t_group, t_query, t_var)
        VALUES (:t_id, :t_name, :t_group, :t_query, :t_var)
    """)
    params = {
        't_id': t_id,
        't_name': t_name,
        't_group': t_group,
        't_var': t_var,
        't_query': t_query
    }
    return process_query_get_count(qry, params)


def store_metric_conf(m_id, m_name, timeframe, m_var, t_name):
    qry = sa.text("""
        INSERT INTO metric (id, m_name, timeframe, m_var, t_name)
        VALUES (:m_id, :m_name, :timeframe, :m_var, :t_name)
    """)
    params = {
        'm_id': m_id,
        'm_name': m_name,
        'timeframe': timeframe,
        'm_var': m_var,
        't_name': t_name
    }
    return process_query_get_count(qry, params)


def list_metric_conf():
    qry = sa.text("""
        SELECT m_name
        FROM metric
        GROUP BY m_name
    """)
    return [dict(row) for row in db.engine.execute(qry)]


def get_metric_conf(name):
    qry = sa.text("""
        SELECT *
        FROM metric
        WHERE m_name = :name
    """)
    return process_query(qry, {'name': name})


def get_template_variables(name):
    qry = sa.text("""
        SELECT t_var
        FROM template
        WHERE t_name = :name
    """)
    return process_query(qry, {'name': name})


def delete_metric_conf(name):
    qry = sa.text("""
        DELETE FROM metric
        WHERE m_name = :name
    """)
    params = {
        'name': name
    }
    return process_query_get_count(qry, params)


def delete_template_conf(name):
    qry = sa.text("""
        DELETE FROM template
        WHERE t_name = :name
    """)
    params = {
        'name': name
    }
    return process_query_get_count(qry, params)


def store_instance_conf(instance_name, instance_promql, start_time, end_time,
                        instance_values):
    qry = sa.text("""
        INSERT INTO instance (instance_name, instance_promql, start_time, end_time,
                              instance_values)
        VALUES (:instance_name, :instance_promql, :start_time, :end_time,
                :instance_values)
    """)
    params = {
        'instance_name': instance_name,
        'instance_promql': instance_promql,
        'start_time': start_time,
        'end_time': end_time,
        'instance_values': instance_values
    }
    return process_query_get_count(qry, params)


def delete_instance_conf(instance_name, end_time):
    qry = sa.text("""
        UPDATE instance
        SET end_time = :end_time
        WHERE instance_name = :instance_name
    """)

    params = {
        'end_time': end_time,
        'instance_name': instance_name
    }
    return process_query_get_count(qry, params)
