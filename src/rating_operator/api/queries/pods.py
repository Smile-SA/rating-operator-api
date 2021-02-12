from typing import AnyStr, Dict, List

from rating_operator.api.check import date_checker_start_end, multi_tenant
from rating_operator.api.utils import process_query

import sqlalchemy as sa
from sqlalchemy.sql.expression import bindparam


@multi_tenant
def get_pods(start: AnyStr,
             end: AnyStr,
             tenant_id: AnyStr,
             namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get pods.

    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT pod
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end < :end
        AND namespace IN :namespaces
        GROUP BY pod
        ORDER BY pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_pods_metrics_rating(start: AnyStr,
                            end: AnyStr,
                            tenant_id: AnyStr,
                            namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating by pods and namespace.

    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                pod,
                metric
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end < :end
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric, pod
        ORDER BY frame_begin, metric, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_pods_rating(start: AnyStr,
                    end: AnyStr,
                    tenant_id: AnyStr,
                    namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating by pods.

    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                frame_end,
                frame_price,
                metric,
                namespace,
                node,
                pod
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
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
def get_pods_rating_daily(tenant_id: AnyStr, namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the daily pods rating.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                pod
        FROM frames
        WHERE frame_begin >= date_trunc('day', now())
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, pod
        ORDER BY frame_begin, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_pod_rating_daily(tenant_id: AnyStr,
                         pod: AnyStr,
                         namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the daily pods rating.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :pod (AnyStr) A string representing the pod.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price
        FROM frames
        WHERE frame_begin >= date_trunc('day', now())
        AND pod = :pod
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, pod
        ORDER BY frame_begin, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces,
        'pod': pod
    }
    return process_query(qry, params)


@multi_tenant
def get_pods_rating_weekly(tenant_id: AnyStr, namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the weekly pods rating.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                pod
        FROM frames
        WHERE frame_begin >= date_trunc('week', now())
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, pod
        ORDER BY frame_begin, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_pod_rating_weekly(tenant_id: AnyStr,
                          pod: AnyStr,
                          namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the weekly pods rating.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :pod (AnyStr) A string representing the pod.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price
        FROM frames
        WHERE frame_begin >= date_trunc('week', now())
        AND pod = :pod
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, pod
        ORDER BY frame_begin, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces,
        'pod': pod
    }
    return process_query(qry, params)


@multi_tenant
def get_pods_rating_monthly(tenant_id: AnyStr, namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the monthly pods rating.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                pod
        FROM frames
        WHERE frame_begin >= date_trunc('month', now())
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, pod
        ORDER BY frame_begin, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_pod_rating_monthly(tenant_id: AnyStr,
                           pod: AnyStr,
                           namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the monthly pods rating.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :pod (AnyStr) A string representing the pod.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price
        FROM frames
        WHERE frame_begin >= date_trunc('month', now())
        AND pod = :pod
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, pod
        ORDER BY frame_begin, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces,
        'pod': pod
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_pods_total_rating(start: AnyStr,
                          end: AnyStr,
                          tenant_id: AnyStr,
                          namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating by pod.

    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  sum(frame_price) as frame_price,
                                    pod
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY pod
        ORDER BY pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'start': start,
        'end': end,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_pod_total_rating(pod: AnyStr,
                         start: AnyStr,
                         end: AnyStr,
                         tenant_id: AnyStr,
                         namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the aggregated rating for a pod.

    :pod (AnyStr) A string representing the pod.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT sum(frame_price) as frame_price,
                                   namespace,
                                   node,
                                   pod
        FROM frames
        WHERE pod = :pod
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY namespace, node, pod
        ORDER BY namespace, node, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'pod': pod,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_pod_namespace(pod: AnyStr,
                      start: AnyStr,
                      end: AnyStr,
                      tenant_id: AnyStr,
                      namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the namespaces for a pod.

    :pod (AnyStr) A string representing the pod.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  pod,
                namespace
        FROM frames
        WHERE pod = :pod
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY pod, namespace
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'pod': pod,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_pod_node(pod: AnyStr,
                 start: AnyStr,
                 end: AnyStr,
                 tenant_id: AnyStr,
                 namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the nodes for a pod.

    :pod (AnyStr) A string representing the pod.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  pod,
                node
        FROM frames
        WHERE pod = :pod
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY pod, node
        ORDER BY pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'pod': pod,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_pod_rating(pod: AnyStr,
                   start: AnyStr,
                   end: AnyStr,
                   tenant_id: AnyStr,
                   namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating for a pod, by metric.

    :pod (AnyStr) A string representing the pod.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                metric,
                pod
        FROM frames
        WHERE pod = :pod
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric, pod
        ORDER BY frame_begin, metric, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'pod': pod,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_pod_metric_rating(pod: AnyStr,
                          metric: AnyStr,
                          start: AnyStr,
                          end: AnyStr,
                          tenant_id: AnyStr,
                          namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating for a given pod and metric, by namespace and node.

    :pod (AnyStr) A string representing the pod.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                frame_end,
                frame_price,
                namespace,
                node
        FROM frames
        WHERE pod = :pod
        AND metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        ORDER BY frame_begin
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'pod': pod,
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_pod_metric_total_rating(pod: AnyStr,
                                metric: AnyStr,
                                start: AnyStr,
                                end: AnyStr,
                                tenant_id: AnyStr,
                                namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the agglomerated rating for a given pod and metric, by namespace and node.

    :pod (AnyStr) A string representing the pod.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT sum(frame_price) as frame_price,
                                   pod
        FROM frames
        WHERE pod = :pod
        AND metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'pod': pod,
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_pod_lifetime(pod: AnyStr,
                     tenant_id: AnyStr,
                     namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the lifetime for a pod.

    :pod (AnyStr) A string representing the pod.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  min(frame_begin) as start,
                max(frame_end) as end
        FROM frames
        WHERE pod = :pod
        AND namespace IN :namespaces
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'pod': pod,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_by_pod_rating(start: AnyStr,
                      end: AnyStr,
                      tenant_id: AnyStr,
                      namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating by pod.

    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                pod
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end < :end
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, pod
        ORDER BY frame_begin, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)
