from flask import Blueprint, abort, jsonify, make_response
from flask import request, session
from flask.wrappers import Response

from rating_operator.api.check import assert_url_params, request_params
from rating_operator.api.queries import auth as query
from rating_operator.api.queries import namespaces as ns
from rating_operator.api.secret import require_admin


tenants_routes = Blueprint('tenants', __name__)


@tenants_routes.route('/current', methods=['GET'])
def request_current_tenant() -> Response:
    """Get the current tenant."""
    return make_response(
        jsonify(results=session.get('tenant', '')), 200)


@tenants_routes.route('/tenant', methods=['GET'])
@assert_url_params
@require_admin
def request_tenant() -> Response:
    """Get the tenant."""
    config = request_params(request.args)
    tenant = config.get('tenant')
    if not tenant:
        abort(make_response(), 404)
    results = query.get_tenant(tenant)
    return make_response(jsonify(results=results), 200)


@tenants_routes.route('/tenants', methods=['GET'])
@require_admin
def get_tenants() -> Response:
    """Get all the tenants."""
    results = query.get_tenants()
    return make_response(
        jsonify(results=results, total=len(results)), 200)


@tenants_routes.route('/tenants/link', methods=['POST'])
@require_admin
def link_namespace() -> Response:
    """Assign a namespace to a tenant."""
    namespaces = request.form.get('namespace')
    tenant = request.form.get('tenant')
    total = 0
    if isinstance(namespaces, str):
        namespaces = [namespaces]
    for namespace in namespaces:
        total += query.link_namespace(tenant, namespace)
        ns.modify_namespace(tenant, namespace)
    if total:
        return make_response(jsonify(total=total), 200)
    abort(make_response(jsonify(total=0)), 404)


@tenants_routes.route('/tenants/unlink', methods=['POST'])
@require_admin
def unlink_namespace() -> Response:
    """Remove the namespace assignation to a tenant."""
    namespace = request.form.get('namespace')
    if namespace:
        results = query.unlink_namespace(namespace)
        ns.modify_namespace(None, namespace)
        return make_response(jsonify(total=results), 200)
    abort(make_response(jsonify(total=0), 404))


@tenants_routes.route('/tenants/delete', methods=['POST'])
@require_admin
def delete_tenants() -> Response:
    """Delete the tenant and its namespaces."""
    tenant = request.form.get('tenant', '')
    if tenant == '':
        return make_response(jsonify(total=0, results=[{}]), 404)
    tenant_namespaces = query.get_tenant_namespaces(tenant)
    for namespace in tenant_namespaces:
        ns.delete_namespace(namespace)
    results = query.delete_tenant(tenant)
    code = 200
    if results == 0:
        code = 404
    return make_response(jsonify(total=results, results=results), code)
