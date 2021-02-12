from rating_operator.api import config

from sqlalchemy import create_engine

from .schema import db_update


db_updated = False


def update_postgres_schema():
    """Verify and update the postgreSQL schema."""
    postgres_database_uri = config.envvar('POSTGRES_DATABASE_URI')
    engine = create_engine(postgres_database_uri)
    global db_updated
    if not db_updated:
        db_update(engine)
        db_updated = True
