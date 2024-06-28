from typing import AnyStr, Dict, List

from kubernetes import client, config
from kubernetes.client.rest import ApiException

from rating_operator.api import utils
from rating_operator.api.config import envvar
from rating_operator.api.secret import get_client

import sqlalchemy as sa


def get_table_columns(table: AnyStr) -> List[Dict]:
    """
    Get the column name for a given table.

    :table (AnyStr) A name representing the table

    Return a dict with the results
    """
    qry = sa.text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = :table
        ORDER BY ordinal_position
    """)

    params = {
        'table': table
    }
    return utils.presto_process_query(qry, params)


def get_unrated_frames(table: AnyStr,
                       column: AnyStr,
                       labels: AnyStr,
                       start: AnyStr,
                       end: AnyStr) -> List[Dict]:
    """
    Get unrated frames from presto.

    :table (AnyStr) A string representing the table name
    :column (AnyStr) A string representing the column name
    :labels (AnyStr) A formatted string to include labels (like ", mylabel, mylabel2")
    :start (AnyStr) The start timestamp to use
    :end (AnyStr) The end timestamp to use

    Return the unrated frames as dict
    """
    with_table = f"""
        SELECT period_start,
               period_end,
               namespace,
               node,
               pod,
               {column} {labels}
        FROM {table}
        WHERE period_end >= TIMESTAMP :start
            AND
              period_end < TIMESTAMP :end
    """
    qry = sa.text(with_table)

    params = {
        'start': start,
        'end': end
    }
    return utils.presto_process_query(qry, params)


def update_rated_metrics(metric: AnyStr,
                         report_name: AnyStr,
                         last_insert: AnyStr) -> int:
    """
    Update the frame_status table with new informations.

    :metric (AnyStr) A string representing the metric to update
    :report_name (AnyStr) A string representing the report name
    :last_insert (AnyStr) A time representation of latest update

    Return the number of row updated, 0 in case of fail, 1 otherwise
    """
    qry = sa.text("""
        INSERT INTO frame_status(last_insert, report_name, metric)
        VALUES (:last_insert, :report_name , :metric)
        ON CONFLICT (report_name, metric)
        DO UPDATE SET last_insert = :last_insert
    """)

    params = {
        'last_insert': last_insert,
        'report_name': report_name,
        'metric': metric
    }
    return utils.process_query_get_count(qry, params)


def update_rated_metrics_object(metric: AnyStr, last_insert: AnyStr):
    """
    Update a RatedMetrics object.

    :metric (AnyStr) A name representing the metric to be updated
    :last_insert (AnyStr) The timestamp of the latest data frame rating
    """
    config.load_incluster_config()
    rated_metric = f'rated-{metric.replace("_", "-")}'
    rated_namespace = envvar('RATING_NAMESPACE')
    custom_api = client.CustomObjectsApi(get_client())
    body = {
        'apiVersion': 'rating.smile.fr/v1',
        'kind': 'RatedMetric',
        'metadata': {
            'namespace': rated_namespace,
            'name': rated_metric,
        },
        'spec': {
            'metric': metric,
            'date': last_insert
        }
    }
    try:
        custom_api.create_namespaced_custom_object(group='rating.smile.fr',
                                                   version='v1',
                                                   namespace=rated_namespace,
                                                   plural='ratedmetrics',
                                                   body=body)
    except ApiException as exc:
        if exc.status != 409:
            raise exc
        custom_api.patch_namespaced_custom_object(group='rating.smile.fr',
                                                  version='v1',
                                                  namespace=rated_namespace,
                                                  plural='ratedmetrics',
                                                  name=rated_metric,
                                                  body=body)


def update_rated_namespaces(namespaces: List[AnyStr],
                            last_insert: AnyStr) -> int:
    """
    Update the namespace_status table with latest insert time.

    :namespaces (List[AnyStr]) A list of namespaces
    :last_insert (AnyStr) The timestamp of the latest data frame rating

    Return the number of updated namespaces
    """
    for namespace in namespaces:
        qry = sa.text("""
            INSERT INTO namespace_status (namespace, last_update)
            VALUES (:namespace, :last_update)
            ON CONFLICT ON CONSTRAINT namespace_status_pkey
            DO UPDATE SET last_update = EXCLUDED.last_update
        """)
        params = {
            'namespace': namespace,
            'last_update': last_insert
        }
        utils.process_query(qry, params)
    return len(namespaces)


def clear_rated_metrics(metric: AnyStr) -> int:
    """
    Delete a metrics from the frame_status table.

    :metric (AnyStr) A string representing the metric name

    Return the number of row updated
    """
    qry = sa.text("""
        DELETE FROM frame_status
        WHERE metric = :metric
    """)
    params = {
        'metric': metric
    }
    return utils.process_query_get_count(qry, params)


def delete_rated_frames(metric: AnyStr) -> int:
    """
    Delete rated frames from the database.

    :metric (AnyStr) A string representing the metric name

    Return the number of row deleted
    """
    qry = sa.text("""
        DELETE FROM frames
        WHERE metric = :metric
    """)
    params = {
        'metric': metric
    }
    return utils.process_query_get_count(qry, params)


def get_rated_frames_oldest(tenant_id: AnyStr) -> List[Dict]:
    """
    Get the oldest rated frame timestamp.

    :tenant_id (AnyStr) The tenant name

    Returns the results of the query
    """
    qry = sa.text("""
        SELECT MIN(frame_end)
        FROM frames
    """)
    return utils.process_query(qry, {})
