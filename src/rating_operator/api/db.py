from flask.app import Flask

from flask_sqlalchemy import SQLAlchemy

from pyhive import sqlalchemy_presto

from rating_operator.api import config

import sqlalchemy
from sqlalchemy import create_engine


def setup_database(app: Flask):
    """
    Initialize the SQLAlchemy database driver with the Flask object.

    :app (Flask) An initialized Flask object
    """
    db.init_app(app)


def presto_engine():
    """
    Initialize the Presto database engine.

    Returns the created engine
    """
    sqlalchemy.dialects.presto = sqlalchemy_presto
    presto_database_uri = config.envvar('PRESTO_DATABASE_URI')
    return create_engine(presto_database_uri)


db = SQLAlchemy()
presto_db = presto_engine()
