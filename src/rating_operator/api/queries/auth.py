from typing import AnyStr, Dict, List

from rating_operator.api import utils
from rating_operator.api.db import db

from sqlalchemy import text


def new_tenant(tenant: AnyStr, password: AnyStr) -> int:
    """
    Insert a new tenant in database.

    :tenant (AnyStr) A string representing the name of the tenant
    :password (AnyStr) A string to use as password

    Return the number of row updated, 0 in case of fail, 1 otherwise
    """
    qry = text("""
        INSERT INTO users (tenant_id, password)
        VALUES (:tenant, :password)
    """)
    params = {
        'tenant': tenant,
        'password': password
    }
    return utils.process_query_get_count(qry, params)


def insert_group_tenant(tenant: AnyStr, user_group: AnyStr) -> int:
    """
    Insert the tenant  group in database.

    :tenant (AnyStr) A string representing the name of the tenant
    :admin_user (AnyStr) A string that represents the group of a user (i.e. admin or user)

    Return the number of row updated, 0 in case of fail, 1 otherwise
    """
    qry = text("""
        INSERT INTO group_tenant (tenant_id, user_group)
        VALUES (:tenant, :user_group)
    """)
    params = {
        'tenant': tenant,
        'user_group': user_group
    }
    return utils.process_query_get_count(qry, params)


def update_tenant(tenant: AnyStr, password: AnyStr) -> int:
    """
    Update a tenant in database.

    :tenant (AnyStr) A string representing the name of the tenant
    :password (AnyStr) A string to use as password

    Return the number of row updated, 0 in case of fail, 1 otherwise
    """
    qry = text("""
        UPDATE users
        SET password = :password
        WHERE tenant_id = :tenant
    """)

    params = {
        'tenant': tenant,
        'password': password
    }
    return utils.process_query_get_count(qry, params)


def get_group_tenant(tenant: AnyStr) -> List[Dict]:
    """
    Get the group of the tenant.

    :tenant (AnyStr) A string representing the name of the tenant

    Return the user group in the group_tenant table.
    """
    qry = text("""
        SELECT *
        FROM group_tenant
        WHERE tenant_id = :tenant
    """)
    return utils.process_query(qry, {'tenant': tenant})


def get_tenant(tenant: AnyStr) -> List[Dict]:
    """
    Get a tenant from the namespace table.

    :tenant (AnyStr) A name representing the tenant to get

    Return a dictionary with the table results
    """
    qry = text("""
        SELECT *
        FROM namespaces
        WHERE tenant_id = :tenant
    """)
    return utils.process_query(qry, {'tenant': tenant})


def get_tenant_id(tenant: AnyStr) -> List[Dict]:
    """
    Get a tenant from the users table.

    :tenant (AnyStr) A name representing the tenant to get

    Return a dictionary with the table results
    """
    qry = text("""
        SELECT *
        FROM users
        WHERE tenant_id = :tenant
    """)

    return utils.process_query(qry, {'tenant': tenant})


def link_namespace(tenant: AnyStr, namespace: AnyStr) -> int:
    """
    Assign a namespace to a tenant.

    :tenant (AnyStr) A name representing the tenant to whom the namespace will be assigned
    :namespace (AnyStr) A name representing the namespace to assign to the tenant

    Return the number of row updated, 0 in case of fail, 1 otherwise
    """
    qry = text("""
        UPDATE namespaces
        SET tenant_id = :tenant
        WHERE namespace = :namespace
    """)
    params = {
        'tenant': tenant,
        'namespace': namespace
    }
    return utils.process_query_get_count(qry, params)


def unlink_namespace(namespace: AnyStr) -> int:
    """
    Remove assignation of a namespace to a tenant.

    :namespace (AnyStr) A name representing the namespace

    Return the number of row updated, 0 in case of fail, 1 otherwise
    """
    qry = text("""
        UPDATE namespaces
        SET tenant_id = 'default'
        WHERE namespace = :namespace
    """)
    params = {
        'namespace': namespace
    }
    return utils.process_query_get_count(qry, params)


def delete_tenant(tenant: AnyStr) -> int:
    """
    Remove a tenant from the table.

    :tenant (AnyStr) A name representing the tenant

    Return the number of row updated, 0 in case of fail, 1 otherwise
    """
    qry = text("""
        DELETE FROM namespaces
        WHERE tenant_id = :tenant
    """)
    params = {
        'tenant': tenant
    }
    return utils.process_query_get_count(qry, params)


def get_tenants() -> List[Dict]:
    """
    Get a list of tenant IDs.

    Return a list of tenant IDs, as dictionary
    """
    qry = text("""
        SELECT tenant_id
        FROM namespaces
        GROUP BY tenant_id
    """)
    return [dict(row) for row in db.engine.execute(qry)]


def get_tenant_namespaces(tenant: AnyStr) -> List[AnyStr]:
    """
    Get a list of tenant namespaces.

    :tenant (AnyStr) A name representing the tenant

    Return a list of namespaces
    """
    qry = text("""
        SELECT namespace
        from namespaces
        WHERE tenant_id = :tenant
    """)
    params = {
        'tenant': tenant
    }
    return [row[0] for row in db.engine.execute(qry.params(**params))]
