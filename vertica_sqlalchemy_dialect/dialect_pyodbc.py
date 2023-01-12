from __future__ import absolute_import, print_function, division

from sqlalchemy.connectors.pyodbc import PyODBCConnector

from .base import VerticaDialect as BaseVerticaDialect


# noinspection PyAbstractClass, PyClassHasNoInit
class VerticaDialect(PyODBCConnector, BaseVerticaDialect):
    driver = 'pyodbc'
    # TODO: support SQL caching, for more info see: https://docs.sqlalchemy.org/en/14/core/connections.html#caching-for-third-party-dialects
    supports_statement_cache = False

    @classmethod
    def dbapi(cls):
        pyodbc = __import__('pyodbc')
        return pyodbc
