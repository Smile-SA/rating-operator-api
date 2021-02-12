from typing import AnyStr, Dict, List

from rating_operator.api.check import date_checker_start_end, multi_tenant
from rating_operator.api.utils import process_query

import sqlalchemy as sa
from sqlalchemy.sql.expression import bindparam


@multi_tenant
def get_nodes(tenant_id: AnyStr, namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the nodes.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT node
        FROM frames
        WHERE namespace IN :namespaces
        GROUP BY node
        ORDER BY node
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_nodes_metrics_rating(start: AnyStr,
                             end: AnyStr,
                             tenant_id: AnyStr,
                             namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating by node and metric.

    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                node,
                metric
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end < :end
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric, node
        ORDER BY frame_begin, metric, node
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
def get_nodes_rating(start: AnyStr,
                     end: AnyStr,
                     tenant_id: AnyStr,
                     namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating by node.

    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                node
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, node
        ORDER BY frame_begin, node
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_nodes_rating_daily(tenant_id: AnyStr, namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the daily nodes rating.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                node
        FROM frames
        WHERE frame_begin >= date_trunc('day', now())
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, node
        ORDER BY frame_begin, node
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_node_rating_daily(tenant_id: AnyStr,
                          node: AnyStr,
                          namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the daily rating for a given node.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :node (AnyStr) A string representing the node.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price
        FROM frames
        WHERE frame_begin >= date_trunc('day', now())
        AND node = :node
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, node
        ORDER BY frame_begin, node
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces,
        'node': node
    }
    return process_query(qry, params)


@multi_tenant
def get_nodes_rating_weekly(tenant_id: AnyStr, namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the weekly nodes rating.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                node
        FROM frames
        WHERE frame_begin >= date_trunc('week', now())
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, node
        ORDER BY frame_begin, node
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_node_rating_weekly(tenant_id: AnyStr,
                           node: AnyStr,
                           namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the weekly rating for a given node.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :node (AnyStr) A string representing the node.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price
        FROM frames
        WHERE frame_begin >= date_trunc('week', now())
        AND node = :node
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, node
        ORDER BY frame_begin, node
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces,
        'node': node
    }
    return process_query(qry, params)


@multi_tenant
def get_nodes_rating_monthly(tenant_id: AnyStr, namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the monthly nodes rating.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                node
        FROM frames
        WHERE frame_begin >= date_trunc('month', now())
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, node
        ORDER BY frame_begin, node
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_node_rating_monthly(tenant_id: AnyStr,
                            node: AnyStr,
                            namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the monthly rating for a given node.

    :tenant_id (AnyStr) A string representing the tenant, only present for compatibility.
    :node (AnyStr) A string representing the node.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price
        FROM frames
        WHERE frame_begin >= date_trunc('month', now())
        AND node = :node
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, node
        ORDER BY frame_begin, node
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces,
        'node': node
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_nodes_total_rating(start: AnyStr,
                           end: AnyStr,
                           tenant_id: AnyStr,
                           namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the agglomerated rating by node.

    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  sum(frame_price) as frame_price,
                                    node
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY node
        ORDER BY node
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
def get_node_namespaces_rating(node: AnyStr,
                               start: AnyStr,
                               end: AnyStr,
                               tenant_id: AnyStr,
                               namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating by namespace, for a given node.

    :node (AnyStr) A string representing the node.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                namespace
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end < :end
        AND node = :node
        AND namespace IN :namespaces
        GROUP BY frame_begin, namespace
        ORDER BY frame_begin, namespace
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'node': node,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_node_namespace_rating(node: AnyStr,
                              namespace: AnyStr,
                              start: AnyStr,
                              end: AnyStr,
                              tenant_id: AnyStr,
                              namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating for a given node and namespace.

    :node (AnyStr) A string representing the node.
    :namespace (AnyStr) A string representing the namespace.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                node,
                namespace
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND node = :node
        AND namespace = :namespace
        AND namespace IN :namespaces
        GROUP BY frame_begin, node, namespace
        ORDER BY frame_begin
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'node': node,
        'namespace': namespace,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_node_namespace_total_rating(node: AnyStr,
                                    namespace: AnyStr,
                                    start: AnyStr,
                                    end: AnyStr,
                                    tenant_id: AnyStr,
                                    namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the agglomerated rating for a given node and namespace.

    :node (AnyStr) A string representing the node.
    :namespace (AnyStr) A string representing the namespace.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  sum(frame_price) as frame_price,
                node,
                namespace
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND node = :node
        AND namespace = :namespace
        AND namespace IN :namespaces
        GROUP BY node, namespace
        ORDER BY namespace
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'node': node,
        'namespace': namespace,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_node_rating(node: AnyStr,
                    start: AnyStr,
                    end: AnyStr,
                    tenant_id: AnyStr,
                    namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating by metrics, for a given node.

    :node (AnyStr) A string representing the node.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                metric,
                sum(frame_price) as frame_price
        FROM frames
        WHERE node = :node
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric
        ORDER BY frame_begin, metric
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'node': node,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_node_total_rating(node: AnyStr,
                          start: AnyStr,
                          end: AnyStr,
                          tenant_id: AnyStr,
                          namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the agglomerated rating by namespace, node and pod, for a given node.

    :node (AnyStr) A string representing the node.
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
        WHERE node = :node
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY namespace, node, pod
        ORDER BY namespace, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'node': node,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_node_metric_rating(node: AnyStr,
                           metric: AnyStr,
                           start: AnyStr,
                           end: AnyStr,
                           tenant_id: AnyStr,
                           namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the complete rating for a given node and metric.

    :node (AnyStr) A string representing the node.
    :metric (AnyStr) A string representing the metric.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                frame_end,
                metric,
                frame_price,
                namespace,
                pod
        FROM frames
        WHERE node = :node
        and metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        ORDER BY frame_begin, namespace, pod, metric
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'node': node,
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_node_metric_total_rating(node: AnyStr,
                                 metric: AnyStr,
                                 start: AnyStr,
                                 end: AnyStr,
                                 tenant_id: AnyStr,
                                 namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the complete, agglomerated rating for a given node and metric.

    :node (AnyStr) A string representing the node.
    :metric (AnyStr) A string representing the metric.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT sum(frame_price) as frame_price,
                                   metric,
                                   namespace,
                                   pod
        FROM frames
        WHERE node = :node
        AND metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY metric, namespace, pod
        ORDER BY namespace, pod, metric
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'node': node,
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_nodes_metric_rating(metric: AnyStr,
                            start: AnyStr,
                            end: AnyStr,
                            tenant_id: AnyStr,
                            namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating of a node, for a given metric.

    :metric (AnyStr) A string representing the metric.
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
                node
        FROM frames
        WHERE metric = :metric
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric, node
        ORDER BY frame_begin, metric, node
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)
