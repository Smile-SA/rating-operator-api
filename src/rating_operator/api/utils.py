from typing import Dict, List

from rating_operator.api.db import db, presto_db

from sqlalchemy.sql.elements import TextClause


def process_query(qry: TextClause, params: Dict) -> List[Dict]:
    """
    Execute the given query with parameters.

    :qry (TextClause) A SQL query
    :params (Dict) A dictionary containing any parameters to be interpolated in the query

    Return the result of the query as a list of dictionary
    """
    return [dict(row) for row in db.engine.execute(qry.params(**params))]


def process_query_get_count(qry: TextClause, params: Dict) -> int:
    """
    Execute the given query with parameter and get the number of row affected.

    :qry (TextClause) A SQL query
    :params (Dict) A dictionary containing any parameters to be interpolated in the query

    Return the number of row affected by the query
    """
    res = db.engine.execute(qry.params(**params))
    return res.rowcount


def presto_process_query(qry: TextClause, params: Dict) -> List[Dict]:
    """
    Execute the given query in presto with parameters.

    :qry (TextClause) A SQL query
    :params (Dict) A dictionary containing any parameters to be interpolated in the query

    Return the result of the query as a list of dictionary
    """
    return [dict(row) for row in presto_db.execute(qry.params(**params))]
