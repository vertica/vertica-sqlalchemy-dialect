from __future__ import absolute_import, print_function, division

from sqlalchemy.connectors.pyodbc import PyODBCConnector

from .base import VerticaDialect as BaseVerticaDialect


# noinspection PyAbstractClass, PyClassHasNoInit
class VerticaDialect(PyODBCConnector, BaseVerticaDialect):
    driver = 'pyodbc'
    supports_statement_cache = True

    @classmethod
    def dbapi(cls):
        pyodbc = __import__('pyodbc')
        return pyodbc
