import os
from datetime import timedelta

from flask import Flask

from flask_cors import CORS

from rating_operator.api import db
from rating_operator.api.endpoints.auth import auth_routes
from rating_operator.api.endpoints.configs import configs_routes
from rating_operator.api.endpoints.frames import frames_routes
from rating_operator.api.endpoints.grafana import create_grafana_user, grafana_routes
from rating_operator.api.endpoints.metrics import metrics_routes
from rating_operator.api.endpoints.models import models_routes
from rating_operator.api.endpoints.namespaces import namespaces_routes
from rating_operator.api.endpoints.nodes import nodes_routes
from rating_operator.api.endpoints.pods import pods_routes
from rating_operator.api.endpoints.prometheus import prometheus_routes
from rating_operator.api.endpoints.tenants import tenants_routes
from rating_operator.api.postgres import engine
from rating_operator.api.secret import register_admin_key


def initialize_app():
    """
    Initialize the Flask application for rating-operator.

    Includes blueprint registering and Flask app configuration

    Return an initialized Flask object
    """
    app = Flask(__name__)
    CORS(app, supports_credentials=True, origins=os.environ.get('ALLOW_ORIGIN', '*'))
    app.secret_key = register_admin_key()
    app.permanent_session_lifetime = timedelta(hours=1)
    app.config.from_object('src.rating_operator.api.config.Config')

    app.register_blueprint(auth_routes)
    app.register_blueprint(configs_routes)
    app.register_blueprint(frames_routes)
    app.register_blueprint(grafana_routes)

    # TODO(VDAVIOT) Include if metering-operator is set to TRUE
    app.register_blueprint(metrics_routes)
    app.register_blueprint(namespaces_routes)
    app.register_blueprint(nodes_routes)
    app.register_blueprint(pods_routes)
    app.register_blueprint(prometheus_routes)
    app.register_blueprint(models_routes)
    app.register_blueprint(tenants_routes)
    return app


def create_app():
    """
    Create and return the factory function for rating-operator.

    Initialize the database and the connexion to Grafana
    Replaces __main__ function, is called by flask with:

        export FLASK_APP=rating_operator.api.app:create_app
        ./venv/bin/python -m flask run --host "${ipaddr}" --port 5042

    Return the application for the Flask server to use.
    """
    app = initialize_app()
    db.setup_database(app)
    engine.update_postgres_schema()

    if os.environ.get('GRAFANA') == 'true':
        create_grafana_user(tenant='default', password='default')
    return app
