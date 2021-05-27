import logging
import os
import time
from functools import wraps
from typing import Any, AnyStr, Callable, Dict, Text

from flask import Blueprint, abort, jsonify, make_response, redirect
from flask import Response, render_template, request, session

from keycloak import KeycloakOpenID, exceptions

from kubernetes import client
from kubernetes.client.rest import ApiException

import ldap

from passlib.context import CryptContext

from rating_operator.api.config import envvar, envvar_string
from rating_operator.api.endpoints import grafana as grafana
from rating_operator.api.queries import auth as query
from rating_operator.api.secret import get_client


auth_routes = Blueprint('authentication', __name__)
pwd_context = CryptContext(
    schemes=['pbkdf2_sha256'],
    default='pbkdf2_sha256',
    pbkdf2_sha256__default_rounds=30000
)


def allow_origin() -> AnyStr:
    """Specify the origin of incoming requests, for credentials acceptance."""
    return os.environ.get('ALLOW_ORIGIN', '*')


def with_session(func: Callable) -> Response:
    """
    Verify and return the tenant session.

    :func (Callable) The function for the tenant session

    Return a wrapper function.
    """
    @wraps(func)
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
    return wrapper


def logged_in(func: Callable, **kwargs: Dict) -> Response:
    """
    Verify and return the tenant session.

    :func (Callable) The function for the tenant session

    Return a wrapper function.
    """
    @wraps(func)
    def wrapper(**kwargs) -> Callable:
        """
        Verify and return the tenant session.

        :kwargs (Dict) A dictionary containing all the function parameters

        Return the decorated function.
        """
        tenant = session.get('tenant')
        if tenant:
            response = func(**kwargs)
        else:
            response = make_response(redirect('/login'))
        return response
    return wrapper


def logged_in_admin(func: Callable) -> Response:
    """
    Verify if super-admin and return the tenant session.

    :func (Callable) The function for the tenant session

    Return a wrapper function.
    """
    @wraps(func)
    def wrapper(**kwargs: Dict) -> Callable:
        """
        Verify if super-admin and return the tenant session.

        :kwargs (Dict) A dictionary containing all the function parameters

        Return the decorated function.
        """
        tenant = session.get('tenant')
        if tenant == envvar('ADMIN_ACCOUNT'):
            response = func(**kwargs)
        else:
            response = make_response(redirect('/login'))
        return response
    return wrapper


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
    # if tenant and query.get_tenant_id(tenant) or token:
    if tenant or token:
        return tenant
    # Here default implicitly means public
    # e.g. namespaces not declared with tenant=whatever
    return 'default'


def check_admin(tenant: AnyStr) -> bool:
    """
    Check if a user is admin.

    tenant: (AnyStr) the username

    Return a boolean to express if a user is admin or no.
    """
    if tenant == envvar('ADMIN_ACCOUNT'):
        return True
    else:
        return auth.verify_group_admin(tenant=tenant)


@auth_routes.route('/login', methods=['POST', 'GET'])
def login() -> Text:
    """Return the html template for the /login of rating-operator."""
    tenant = session.get('tenant')
    version = envvar_string('VERSION')
    distribution = envvar_string('DISTRIBUTION')
    return render_template('login.html', tenant=tenant,
                           version=version, dist=distribution)


@auth_routes.route('/signup')
@logged_in_admin
def signup() -> Text:
    """Return the html template for the /signup of rating-operator."""
    tenant = session.get('tenant')
    admin = os.environ.get('ADMIN_ACCOUNT')
    version = envvar_string('VERSION')
    distribution = envvar_string('DISTRIBUTION')
    return render_template('signup.html', tenant=tenant, admin=admin,
                           version=version, dist=distribution)


@auth_routes.route('/password')
@logged_in
def password() -> Text:
    """Return the html template for the /password of rating-operator."""
    tenant = session.get('tenant')
    admin = False
    if tenant == os.environ.get('ADMIN_ACCOUNT'):
        admin = True
    version = envvar_string('VERSION')
    distribution = envvar_string('DISTRIBUTION')
    return render_template('password.html', tenant=tenant, admin=admin,
                           version=version, dist=distribution)


@auth_routes.route('/home', methods=['POST', 'GET'])
@logged_in
def home() -> Text:
    """Return the html template for the /home of rating-operator."""
    tenant = session.get('tenant')
    if not tenant:
        return render_template('login.html', tenant=tenant)
    else:
        super_admin = False
        local = False
        if not hasattr(auth, 'instance'):
            local = True
        if tenant == os.environ.get('ADMIN_ACCOUNT'):
            super_admin = True
        version = envvar_string('VERSION')
        distribution = envvar_string('DISTRIBUTION')
        return render_template('home.html', super_admin=super_admin, local=local,
                               tenant=tenant, version=version, dist=distribution)


@auth_routes.route('/dashboards', methods=['POST', 'GET'])
@logged_in
def dashboards() -> Text:
    """Return the html template for the /dashboards of rating-operator."""
    # Get tenant to load or not administrator dashboards.
    tenant = session.get('tenant')
    admin = False
    if tenant == os.environ.get('ADMIN_ACCOUNT'):
        admin = True
    else:
        admin = auth.verify_group_admin(tenant=tenant)
    # Get dashboard list
    dashboards_url = grafana.get_grafana_dashboards_url(admin)
    version = envvar_string('VERSION')
    distribution = envvar_string('DISTRIBUTION')
    return render_template('dashboards.html', dashboards=dashboards_url,
                           tenant=tenant, admin=admin, version=version, dist=distribution)


def update_tenant_namespaces(tenant: AnyStr, namespaces: AnyStr):
    """
    Create the kubernetes namespaces for the tenant.

    :tenant (AnyStr) A string representing the tenant
    :namespaces (AnyStr) the user namespaces
    """
    api = client.CoreV1Api(get_client())
    for namespace in namespaces.split('-'):
        if not tenant:
            continue
        try:
            ns = api.read_namespace(name=namespace)
            labels = ns.metadata.labels
            if labels:
                if labels.get('tenants'):
                    labels['tenants'] += f'-{tenant}' \
                        if tenant not in labels['tenants'] else ''
                else:
                    labels['tenants'] = tenant
            api.patch_namespace(namespace, body={'metadata': {'labels': labels}})
        except ApiException:
            meta = client.V1ObjectMeta(labels={'tenants': tenant}, name=namespace)
            ns = client.V1Namespace(metadata=meta)
            api.create_namespace(ns)


@auth_routes.route('/login_user', methods=['POST'])
def login_user():
    """
    Login the user into rating operator.

    Return the user session if the credentials are valid or an error message if not.
    """
    tenant = request.form.get('tenant')
    password = request.form.get('password')
    verified = auth.verify(tenant, password)

    if verified:
        logging.info('User logged')
        session.update({
            'tenant': tenant,
            'timestamp': time.time(),
        })
        cookie_settings = {}
        if tenant != envvar_string('ADMIN_ACCOUNT') and hasattr(auth, 'instance'):
            namespaces = auth.get_namespace()
            update_tenant_namespaces(tenant, namespaces)
            if isinstance(auth.instance, Keycloak):
                session.update({'token': verified})
                cookie_settings.update({
                    'httponly': envvar_string('COOKIE_HTTPONLY'),
                    'secure': envvar_string('COOKIE_SECURE'),
                    'samesite': envvar_string('COOKIE_SAMESITE')
                })
                if os.environ.get('AUTH') == 'true':
                    cookie_settings.update({'domain': os.environ.get('DOMAIN')})
        response = make_response(redirect('/home'))
        # protocol = 'https' if os.environ.get('AUTH', 'false') == 'true' else 'http'
        # params = {
        #     '_scheme': protocol,
        #     '_external': protocol == 'https'
        # }
        # to = url_for('.dashboards', **params)
        # response = make_response(redirect(to))

        if envvar('GRAFANA') == 'true':
            grafana_session = grafana.login_grafana_user(tenant, password)
            if grafana_session:
                response.set_cookie('grafana_session',
                                    grafana_session,
                                    **cookie_settings)
        return response
    else:
        message = 'Invalid credentials/Authentication server unreachable'
        return render_template('login.html', message=message)


def new_user(tenant: AnyStr, password: AnyStr) -> bool:
    """Return a boolean containing weither a new tenant is created or no."""
    if not query.get_tenant_id(tenant):
        return True
    return False


def add_user(tenant: AnyStr, password: AnyStr):
    """
    Create a tenant if it does not exist.

    :tenant (AnyStr) the tenant username
    :password (AnyStr) the tenant password
    """
    query.new_tenant(tenant, encrypt_password(password))


@auth_routes.route('/signup_user', methods=['POST'])
def signup_user() -> Response:
    """Create a user and the associated namespaces."""
    tenant = request.form.get('tenant')
    password = request.form.get('password')
    admin_user = request.form.get('admin')
    namespaces = request.form.get('namespaces')
    if new_user(tenant, password) and envvar('ADMIN_ACCOUNT') != tenant:
        if admin_user == 'on':
            query.insert_group_tenant(tenant, 'admin')
        else:
            query.insert_group_tenant(tenant, 'user')
        add_user(tenant, password)
        update_tenant_namespaces(tenant, namespaces)
        if envvar('GRAFANA') == 'true':
            grafana.create_grafana_user(tenant, password)
            if admin_user == 'on':
                grafana.update_grafana_role(grafana.get_grafana_user(tenant), 'Editor')
        return render_template('signup.html', message='User created')
    return render_template('signup.html', message='User already exists')


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
    if 'tenant' not in session:
        abort(400)
    resp = make_response(redirect('/login'))
    if envvar('GRAFANA') == 'true':
        grafana.logout_grafana_user(session['tenant'])
        if os.environ.get('AUTH') == 'true':
            resp.delete_cookie('grafana_session', domain=os.environ.get('DOMAIN'))
        else:
            resp.delete_cookie('grafana_session')
    if session.get('token') is not None:
        auth.client().logout(session['token']['refresh_token'])
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
        return make_response(render_template('password.html',
                                             message='New and old password are similar'))
    elif auth.verify(tenant, old):
        if envvar('GRAFANA') == 'true':
            grafana.update_grafana_password(grafana.get_grafana_user(tenant), password)
        query.update_tenant(tenant, encrypt_password(new))
        return make_response(render_template('password.html',
                                             message='Your password has been updated'))
    else:
        return make_response(render_template('password.html',
                                             message='unrecognized user / password'))


class Keycloak:
    """Keycloak authentication class."""

    def get_keycloak_user_token(self, tenant: AnyStr, password: AnyStr) -> Dict:
        """
        Get the token of the user authentication in Keycloak.

        :tenant (AnyStr) the user username
        :password (AnyStr) the user password

        Return the keycloak user token if valid credentials.
        """
        keycloak_openid = self.client
        token = None
        try:
            token = keycloak_openid.token(tenant, password)
            if envvar('GRAFANA') == 'true':
                user_grafana = grafana.get_grafana_user(tenant)
                if not user_grafana:
                    grafana.create_grafana_user(tenant, password)
                if self.verify_group_admin():
                    grafana.update_grafana_role(user_grafana, 'Editor')
        except exceptions.KeycloakAuthenticationError or exceptions.KeycloakGetError:
            logging.error(f'Authentication error for user {tenant}')
        finally:
            return token

    def client(self, **kwargs: Dict) -> KeycloakOpenID:
        """
        Create an authenticated keycloak client.

        :kwargs (Dict) A directory contaning
        the keycloak client authentication credentials

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
        self.client = KeycloakOpenID(**config)
        return self.client

    def verify(self, tenant: AnyStr, password: AnyStr) -> bool:
        """
        Verify if a user token is not empty.

        :tenant (AnyStr) the user username
        :password (AnyStr) the user password

        Return a boolean describing the success of the user verification in keycloak.
        """
        if tenant == envvar('ADMIN_ACCOUNT'):
            if password == envvar('GRAFANA_ADMIN_PASSWORD'):
                return True
            else:
                return False
        else:
            return self.get_token(tenant, password)

    def verify_group_admin(self, **kwargs: Dict) -> bool:
        """
        Check if a user is admin.

        Return a boolean to express if a user is admin or no.
        """
        tenant_info = self.get_infos()
        group = tenant_info.get('group', '')
        if group == 'admin':
            return True
        else:
            return False

    def get_infos(self) -> Any:
        """Verify users information."""
        return self.client.userinfo(self.token['access_token'])

    def get_token(self, tenant: AnyStr, password: AnyStr) -> Dict:
        """
        Get the token of the user authentication in Keycloak.

        :tenant (AnyStr) the user username
        :password (AnyStr) the user password
        """
        self.token = self.get_keycloak_user_token(tenant, password)
        return self.token

    def get_namespace(self, **kwargs: Dict) -> AnyStr:
        """
        Get the user namespaces attribute from the token.

        Return the user namespaces.
        """
        tenant_info = self.get_infos()
        return tenant_info.get('namespaces', '')


class LDAP:
    """LDAP authentication class."""

    def initialize_ldap_connection(self) -> Dict:
        """Initialize the ldap connection."""
        l_con = None
        try:
            l_con = ldap.initialize(envvar('LDAP_URL'))
        except ldap.LDAPError:
            logging.error('Wrong LDAP URL')
        finally:
            return l_con

    def __init__(self) -> None:
        self.l_schema = envvar_string('LDAP_SCHEMA')
        self.l_schema_login = self.l_schema.split(',')
        self.l_password = envvar_string('LDAP_ADMIN_PASSWORD')

    def client(self, **kwargs: Dict) -> Dict:
        """
        Create a ldap client.

        :kwargs (Dict) A directory contaning
        the keycloak client authentication credentials

        Return the KeycloakOpenID object.
        """
        l_con = self.initialize_ldap_connection()
        l_con.simple_bind_s('cn=admin,{},{}'.format(self.l_schema_login[1],
                            self.l_schema_login[2]), self.l_password)
        self.client = l_con
        return self.client

    def verify(self, tenant: AnyStr, password: AnyStr) -> bool:
        """
        Verify a user using LDAP schema.

        :tenant (AnyStr) the user username
        :password (AnyStr) the user password

        Return a boolean describing the success of the user authentication.
        """
        result = None
        try:
            if tenant == envvar('ADMIN_ACCOUNT'):
                if password == envvar('GRAFANA_ADMIN_PASSWORD'):
                    result = True
                else:
                    result = False
            else:
                result = self.client.compare_s('cn={},{}'.format(tenant, self.l_schema),
                                               'userPassword', password)
                if result and envvar('GRAFANA') == 'true':
                    user_grafana = grafana.get_grafana_user(tenant)
                    if not user_grafana:
                        grafana.create_grafana_user(tenant, password)
                    if self.verify_group_admin():
                        grafana.update_grafana_role(user_grafana, 'Editor')
        except ldap.NO_SUCH_OBJECT:
            logging.error(f'User does not exist {tenant}')
        finally:
            return result

    def verify_group_admin(self, **kwargs: Dict) -> bool:
        """
        Verify if a user is admin.

        :kwargs (Dict) contains username

        Return a boolean describing if the user is in admin group.
        """
        result = None
        if kwargs['tenant']:
            tenant = kwargs['tenant']
        try:
            result = self.client.compare_s('cn={},{}'.format(tenant, self.l_schema),
                                           'sn', 'admin')
        except ldap.NO_SUCH_OBJECT:
            logging.error(f'User does not exist {tenant}')
        finally:
            return result

    def get_namespace(self, **kwargs: Dict) -> AnyStr:
        """
        Get the user namespaces attribute from ldap.

        :kwargs (Dict) contains username

        Return the user namespaces.
        """
        if kwargs['tenant']:
            tenant = kwargs['tenant']
        namespaces = self.client.search_s('{}'.format(self.l_schema), ldap.SCOPE_SUBTREE,
                                          '(cn={})'.format(tenant), ['uid'])
        result_namespaces = namespaces[0][1]['uid'][0]
        result_namespaces_string = result_namespaces.decode('utf-8')
        return result_namespaces_string


class Authenticator:
    """Authenticate and communicate with the configured user management system."""

    def client(self, **kwargs: Dict):
        """Return a local client."""
        pass

    def verify(self, tenant: AnyStr, password: AnyStr) -> bool:
        """
        Verify a user in local database.

        :tenant (AnyStr) the user username
        :password (AnyStr) the user password

        Return a boolean describing the success of the user authentication.
        """
        if tenant == envvar('ADMIN_ACCOUNT'):
            if password == envvar('GRAFANA_ADMIN_PASSWORD'):
                return True
            else:
                return False
        else:
            results = query.get_tenant_id(tenant)
            if not results or \
               not check_encrypted_password(password, results[0]['password']):
                return False
            else:
                return True

    def get_namespace(self, **kwargs: Dict):
        """Get the user namespaces attribute from local database."""

    def verify_group_admin(self, **kwargs) -> bool:
        """
        Verify if a user is admin.

        :kwargs (Dict) contains username

        Return a boolean describing if the user is in admin group.
        """
        if kwargs['tenant']:
            tenant = kwargs['tenant']
        res = query.get_group_tenant(tenant)
        if res[0]['user_group'] == 'admin':
            return True
        else:
            return False

    def __init__(self, method: AnyStr = None, **kwargs):
        try:
            instance = {
                'ldap': LDAP,
                'keycloak': Keycloak
            }[method]()
        except KeyError:
            return

        self.instance = instance
        self.client = instance.client
        self.verify = instance.verify
        self.get_namespace = instance.get_namespace
        self.verify_group_admin = instance.verify_group_admin


# envvar('AUTH_METHOD'))
auth = Authenticator(method=envvar('AUTH_METHOD'))
auth.client()
