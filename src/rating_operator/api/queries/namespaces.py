import random
import string
from typing import AnyStr, Dict, List

from flask import abort, jsonify, make_response

from kubernetes import client
from kubernetes.client.rest import ApiException

from rating_operator.api.check import date_checker_start_end, multi_tenant
from rating_operator.api.secret import get_client
from rating_operator.api.utils import process_query, process_query_get_count

import sqlalchemy as sa
from sqlalchemy.sql.expression import bindparam


def create_tenant_namespace(tenant: AnyStr, quantity: int):
    """
    Create namespaces for the tenant.

    :tenant (AnyStr) A string to represent the tenant name, to annotate the namespaces.
    :quantity (int) A number of namespaces to create and assign to the tenant.
    """
    api = client.CoreV1Api(get_client())
    labels = {
        'tenant': tenant
    }
    for number in range(int(quantity)):
        rd = ''.join(
            [random.choice(string.ascii_letters + string.digits) for n in range(8)])
        name = f'{tenant}-{number}-{rd}'.lower()
        meta = client.V1ObjectMeta(labels=labels, name=name)
        namespace = client.V1Namespace(metadata=meta)
        try:
            api.create_namespace(namespace)
        except ApiException as exc:
            if exc.status == 403:
                abort(make_response(jsonify(msg=str(exc))), exc.status)
            raise exc


def delete_namespace(namespace: AnyStr):
    """
    Delete the namespace.

    :namespace (AnyStr) A string to represent the namespace.
    """
    try:
        client.CoreV1Api(get_client()).delete_namespace(namespace)
    except ApiException as exc:
        if exc.status == 404:
            abort(make_response(jsonify(msg=str(exc))), exc.status)
        raise exc


def modify_namespace(tenant: AnyStr, namespace: AnyStr):
    """
    Modify the tenant assigned to a namespace.

    :tenant (AnyStr) A string representing the tenant.
    :namespace (AnyStr) A string representing the namespace to assign to the tenant.
    """
    api = client.CoreV1Api(get_client())
    labels = {
        'tenant': tenant
    }
    meta = client.V1ObjectMeta(labels=labels, name=namespace)
    ns = client.V1Namespace(metadata=meta)
    try:
        api.patch_namespace(namespace, ns)
    except ApiException as exc:
        if exc.status == 404:
            abort(make_response(jsonify(msg=str(exc))), exc.status)
        raise exc


def update_namespace(namespace: AnyStr, tenant_id: AnyStr) -> int:
    """
    Update the namespace associated with the tenant.

    :namespace (AnyStr) A string representing the namespace
    :tenant (AnyStr) A string representing the tenant.

    Return the number of row affected.
    """
    qry = sa.text("""
        INSERT INTO namespaces(namespace, tenant_id)
        VALUES (:namespace, :tenant)
        ON CONFLICT ON CONSTRAINT namespaces_pkey
        DO UPDATE
        SET tenant_id = EXCLUDED.tenant_id
    """)

    params = {
        'namespace': namespace,
        'tenant': tenant_id
    }

    return process_query_get_count(qry, params)


@multi_tenant
def get_namespaces(tenant_id: AnyStr, namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get namespaces.

    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr])  A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT namespace, tenant_id
        FROM namespaces
        WHERE namespace IN :namespaces
        ORDER BY namespace
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_namespace_rating(namespace: AnyStr,
                         start: AnyStr,
                         end: AnyStr,
                         tenant_id: AnyStr,
                         namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating of a namespace.

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
                metric
        FROM frames
        WHERE namespace = :namespace
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric
        ORDER BY frame_begin, metric
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'namespace': namespace,
        'namespaces': namespaces,
        'start': start,
        'end': end,
        'tenant_id': tenant_id
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_namespaces_rating(start: AnyStr,
                          end: AnyStr,
                          tenant_id: AnyStr,
                          namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating by namespaces.

    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                frame_price,
                namespace
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        ORDER BY frame_begin, namespace
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_namespaces_rating_daily(tenant_id: AnyStr,
                                namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the daily rating, by namespaces.

    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                namespace
        FROM frames
        WHERE frame_begin >= date_trunc('day', now())
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, namespace
        ORDER BY frame_begin, frame_price
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_namespace_rating_daily(tenant_id: AnyStr,
                               namespace: AnyStr,
                               namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the daily rating for a given namespace.

    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespace (AnyStr) A string representing the namespace.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                namespace
        FROM frames
        WHERE frame_begin >= date_trunc('day', now())
        AND namespace = :namespace
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, namespace
        ORDER BY frame_begin, frame_price
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces,
        'namespace': namespace
    }
    return process_query(qry, params)


@multi_tenant
def get_namespace_rating_weekly(tenant_id: AnyStr,
                                namespace: AnyStr,
                                namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the weekly rating for a given namespace.

    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespace (AnyStr) A string representing the namespace.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                namespace
        FROM frames
        WHERE frame_begin >= date_trunc('week', now())
        AND namespace = :namespace
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, namespace
        ORDER BY frame_begin, frame_price
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces,
        'namespace': namespace
    }
    return process_query(qry, params)


@multi_tenant
def get_namespace_rating_monthly(tenant_id: AnyStr,
                                 namespace: AnyStr,
                                 namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the monthly rating for a given namespace.

    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespace (AnyStr) A string representing the namespace.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                namespace
        FROM frames
        WHERE frame_begin >= date_trunc('month', now())
        AND namespace = :namespace
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, namespace
        ORDER BY frame_begin, frame_price
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces,
        'namespace': namespace
    }
    return process_query(qry, params)


@multi_tenant
def get_namespaces_rating_weekly(tenant_id: AnyStr,
                                 namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the weekly rating, by namespaces.

    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                namespace
        FROM frames
        WHERE frame_begin >= date_trunc('week', now())
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, namespace
        ORDER BY frame_begin, frame_price DESC
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_namespaces_rating_monthly(tenant_id: AnyStr,
                                  namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the monthly rating, by namespaces.

    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                namespace
        FROM frames
        WHERE frame_begin >= date_trunc('month', now())
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, namespace
        ORDER BY frame_begin, frame_price DESC
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@multi_tenant
def get_namespaces_metrics_rating(start: AnyStr,
                                  end: AnyStr,
                                  tenant_id: AnyStr,
                                  namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the rating by namespaces and metric.

    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  frame_begin,
                sum(frame_price) as frame_price,
                namespace,
                metric
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end < :end
        AND namespace != 'unspecified'
        AND pod != 'unspecified'
        AND namespace IN :namespaces
        GROUP BY frame_begin, metric, namespace
        ORDER BY frame_begin, metric, namespace
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
def get_namespace_total_rating(namespace: AnyStr,
                               start: AnyStr,
                               end: AnyStr,
                               tenant_id: AnyStr,
                               namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the agglomerated rating of a namespace.

    :namespace (AnyStr) A string representing the namespace.
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
        WHERE namespace = :namespace
        AND frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY namespace, node, pod
        ORDER BY node, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'namespace': namespace,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }
    return process_query(qry, params)


@date_checker_start_end
@multi_tenant
def get_namespaces_total_rating(start: AnyStr,
                                end: AnyStr,
                                tenant_id: AnyStr,
                                namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the agglomerated rating, by namespace.

    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT  sum(frame_price) as frame_price,
                                    namespace
        FROM frames
        WHERE frame_begin >= :start
        AND frame_end <= :end
        AND namespace IN :namespaces
        GROUP BY namespace
        ORDER BY namespace
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
def get_namespace_metric_rating(namespace: AnyStr,
                                metric: AnyStr,
                                start: AnyStr,
                                end: AnyStr,
                                tenant_id: AnyStr,
                                namespaces: List[AnyStr]) -> List[Dict]:
    """
    Get the pods rating for a given namespace and metric.

    :namespace (AnyStr) A string representing the namespace.
    :metric (AnyStr) A string representing the metric.
    :start (AnyStr) A timestamp, as a string, to represent the starting time.
    :end (AnyStr)  A timestamp, as a string, to represent the ending time.
    :tenant_id (AnyStr) A string representing the tenant, only used by decorators.
    :namespaces (List[AnyStr]) A list of namespaces accessible by the tenant.

    Return the results of the query as a list of dictionary.
    """
    qry = sa.text("""
        SELECT frame_begin,
               sum(frame_price) as frame_price,
               pod
        FROM frames
        WHERE namespace = :namespace
        AND metric = :metric
        AND frame_begin >= :start
        AND frame_end < :end
        AND namespace IN :namespaces
        GROUP BY frame_begin, pod
        ORDER BY frame_begin, pod
    """).bindparams(bindparam('namespaces', expanding=True))

    params = {
        'namespace': namespace,
        'metric': metric,
        'start': start,
        'end': end,
        'tenant_id': tenant_id,
        'namespaces': namespaces
    }

    return process_query(qry, params)
