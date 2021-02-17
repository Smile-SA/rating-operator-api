import logging
import os
import time
from typing import AnyStr, Callable, Dict, Text

from flask import Blueprint, abort, jsonify, make_response, redirect
from flask import Response, render_template, request, session, url_for

from keycloak import KeycloakOpenID, exceptions

from passlib.context import CryptContext

from rating_operator.api.config import envvar, envvar_string
from rating_operator.api.endpoints import grafana as grafana
from rating_operator.api.queries import auth as query
from rating_operator.api.queries import namespaces as namespace_qry


auth_routes = Blueprint('authentication', __name__)
pwd_context = CryptContext(
    schemes=['pbkdf2_sha256'],
    default='pbkdf2_sha256',
    pbkdf2_sha256__default_rounds=30000
)

token_keycloak_user = {}


def keycloak_client(**kwargs: Dict) -> KeycloakOpenID:
    """
    Create an authenticated keycloak client.

    :kwargs (Dict) A directory contaning the keycloak client authentication credentials

    Return the KeycloakOpenID object.
    """
    config = {}
    if kwargs:
        config.update(kwargs)
    else:
        config.update({
            'server_url': envvar('KEYCLOAK_URL'),
            'client_id': envvar('KEYCLOAK_CLIENT_ID'),
            'realm_name': envvar('KEYCLOAK_REALM'),
            'client_secret_key': envvar('KEYCLOAK_SECRET_KEY')
        })
    return KeycloakOpenID(**config)


def allow_origin() -> AnyStr:
    """Specify the origin of incoming requests, for credentials acceptance."""
    return os.environ.get('ALLOW_ORIGIN', '*')


def with_session(func: Callable) -> Response:
    """
    Verify and return the tenant session.

    :func (Callable) The function for the tenant session

    Return a wrapper function.
    """
    def wrapper(**kwargs: Dict) -> Callable:
        """
        Verify and return the tenant session.

        :kwargs (Dict) A dictionary containing all the function parameters

        Return the decorated function.
        """
        kwargs['tenant'] = authenticated_user(request)
        res = func(**kwargs)
        if isinstance(res, dict):
            total, results = res['total'], res['results']
            response = make_response(
                jsonify(results=results, total=total),
                200)
        else:
            response = res
        response.headers['Access-Control-Allow-Origin'] = allow_origin()
        response.headers['Access-Control-Allow-Credentials'] = 'true'
        return response
    wrapper.__name__ = func.__name__
    return wrapper


def verify_user_local(tenant: AnyStr, password: AnyStr) -> bool:
    """
    Verify a user locally.

    :tenant (AnyStr) the user username
    :password (AnyStr) the user password

    Return a boolean describing the success of the user authentication.
    """
    results = query.get_tenant_id(tenant)
    if not results or not check_encrypted_password(password, results[0]['password']):
        return False
    return True


def get_keycloak_user_token(tenant: AnyStr, password: AnyStr) -> Dict:
    """
    Get the token of the user authentication in Keycloak.

    :tenant (AnyStr) the user username
    :password (AnyStr) the user password

    Return the keycloak user token if valid credentials.
    """
    keycloak_openid = keycloak_client()
    token = None
    try:
        token = keycloak_openid.token(tenant, password)
        grafana.create_grafana_user(tenant, password)
    except exceptions.KeycloakAuthenticationError or exceptions.KeycloakGetError:
        logging.error(f'Authentication error for user {tenant}')
    finally:
        return token


def verify_user_keycloak(tenant: AnyStr, password: AnyStr) -> bool:
    """
    Verify if a user token is not empty.

    :tenant (AnyStr) the user username
    :password (AnyStr) the user password

    Return a boolean describing the success of the user verification in keycloak.
    """
    return get_keycloak_user_token(tenant, password)


def verify_user(tenant: AnyStr, password: AnyStr) -> bool or Dict:
    """
    Verify the user through keycloak if configured, or locally.

    :tenant (AnyStr) the user username
    :password (AnyStr) the user password

    Return a boolean describing the success of the user verification.
    """
    if envvar('KEYCLOAK') == 'true':
        return verify_user_keycloak(tenant, password)
    return verify_user_local(tenant, password)


def encrypt_password(password: AnyStr) -> AnyStr:
    """
    Encrypt a password.

    :password (AnyStr) the user password

    Return an encrypted password.
    """
    return pwd_context.encrypt(password)


def check_encrypted_password(password: AnyStr, hashed: AnyStr) -> bool:
    """
    Check if a password is correct with its encryption.

    :password (AnyStr) the user password
    :hashed (AnyStr) the user encrypted password

    Return a boolean containing the success of the comparison.
    """
    return pwd_context.verify(password, hashed)


def authenticated_user(request: request) -> AnyStr:
    """
    Check if a user is authenticated and get its username.

    :request (request) flask request

    Return the username if a user is authenticated or empty string if not.
    """
    token = session.get('token')
    tenant = session.get('tenant')
    if tenant and query.get_tenant_id(tenant) or token:
        return tenant
    # Here default implicitly means public
    # e.g. namespaces not declared with tenant=whatever
    return 'default'


@auth_routes.route('/home')
def home() -> Text:
    """Return the html template for the /home of rating-operator."""
    return render_template('home.html')


@auth_routes.route('/login', methods=['POST', 'GET'])
def login() -> Text:
    """Return the html template for the /login of rating-operator."""
    tenant = session.get('tenant')
    return render_template('login.html', tenant=tenant)


@auth_routes.route('/signup')
def signup() -> Text:
    """Return the html template for the /signup of rating-operator."""
    return render_template('signup.html')


@auth_routes.route('/password')
def password() -> Text:
    """Return the html template for the /password of rating-operator."""
    return render_template('password.html')


@auth_routes.route('/dashboards', methods=['POST', 'GET'])
def dashboards() -> Text:
    """Return the html template for the /dashboards of rating-operator."""
    # Get tenant to load or not administrator dashboards.
    admin = session.get('tenant', 'default') == os.environ.get('ADMIN_ACCOUNT', 'admin')

    # Get dashboard list
    dashboards_url = grafana.get_grafana_dashboards_url(admin)

    return render_template('dashboards.html',
                           dashboards=dashboards_url)


@auth_routes.route('/login_user', methods=['POST'])
def login_user() -> Response:
    """
    Login the user into rating operator.

    Return the user session if the credentials are valid or an error message if not.
    """
    tenant = request.form.get('tenant')
    password = request.form.get('password')
    verified = verify_user(tenant, password)
    if verified:
        session.update({
            'tenant': tenant,
            'timestamp': time.time(),
        })
        cookie_settings = {}
        if envvar_string('KEYCLOAK'):
            session.update({'token': verified})
            cookie_settings.update({
                'httponly': envvar_string('COOKIE_HTTPONLY'),
                'secure': envvar_string('COOKIE_SECURE'),
                'samesite': envvar_string('COOKIE_SAMESITE')
            })
        protocol = 'https' if os.environ.get('AUTH', 'false') == 'true' else 'http'
        params = {
            '_scheme': protocol,
            '_external': protocol == 'https'
        }
        to = url_for('.dashboards', **params)
        response = make_response(redirect(to))
        if envvar('GRAFANA') == 'true':
            grafana_session = grafana.login_grafana_user(tenant, password)
            if grafana_session:
                response.set_cookie('grafana_session',
                                    grafana_session,
                                    **cookie_settings)
        return response
    else:
        abort(
            make_response(
                jsonify(
                    message='Invalid credentials'
                            '/ Authentication server cannot be reached'), 401))


def new_user(tenant: AnyStr, password: AnyStr) -> bool:
    """
    Create a tenant if it does not exist.

    :tenant (AnyStr) the tenant username
    :password (AnyStr) the tenant password

    Return a boolean containing weither a new tenant is created or no.
    """
    if not query.get_tenant_id(tenant):
        query.new_tenant(tenant, encrypt_password(password))
        return True
    return False


@auth_routes.route('/add_user', methods=['POST'])
def add_user() -> Response:
    """Create a user if it doesn't already exists."""
    received = request.get_json()
    if new_user(received['body']['tenant'], received['body']['password']):
        return make_response(jsonify(message='User created'), 200)
    return make_response(jsonify(message='User already exist'), 200)


@auth_routes.route('/signup_user', methods=['POST'])
def signup_user() -> Response:
    """Create a user and the associated namespaces."""
    tenant = request.form.get('tenant')
    password = request.form.get('password')
    quantity = request.form.get('quantity', 1)
    if new_user(tenant, password):
        namespace_qry.create_tenant_namespace(tenant, quantity)
        if envvar('GRAFANA') == 'true':
            grafana.create_grafana_user(tenant, password)
        return redirect(url_for('.login'))
    abort(make_response(jsonify(message='User already exists'), 403))


def format_url(path: AnyStr) -> AnyStr:
    """
    Format the url according to environment variables and path.

    :path (AnyStr) A string representing the path to construct.

    Returns the formatted url.
    """
    if os.environ.get('AUTH') == 'true' and 'http' not in path:
        api_url = envvar('RATING_API_URL')
        domain = os.environ.get('DOMAIN', 'svc.cluster.local')
        protocol = 'https' if os.environ.get('AUTH', 'false') == 'true' else 'http'
        return f'{protocol}://{api_url}.{domain}{path}'
    return path


@auth_routes.route('/logout', methods=['POST', 'GET'])
def logout_user() -> Response:
    """Disconnect a user by closing the session."""
    if 'tenant' not in session or 'token' not in session:
        abort(400)
    to = format_url(url_for('.login'))
    resp = make_response(redirect(to))
    if envvar('GRAFANA') == 'true':
        grafana.logout_grafana_user(session['tenant'])
        resp.delete_cookie('grafana_session', domain=os.environ.get('DOMAIN'))
    if session.get('token') is not None:
        keycloak_client().logout(session['token']['refresh_token'])
    session.clear()
    return resp


@auth_routes.route('/password_change', methods=['POST'])
@with_session
def change_password(tenant: AnyStr) -> Response:
    """
    Change the password of the user.

    :tenant (AnyStr) A string representing the tenant.

    Return a response object.
    """
    old, new = request.form['old'], request.form['new']
    if tenant == '' or old == new:
        abort(400)
    if verify_user(tenant, old):
        if envvar('GRAFANA') == 'true':
            grafana.update_grafana_password(grafana.get_grafana_user(tenant), password)
        query.update_tenant(tenant, encrypt_password(new))
        return make_response(jsonify(msg='password updated'), 200)
    abort(make_response(jsonify(message='unrecognized user / password')), 404)
