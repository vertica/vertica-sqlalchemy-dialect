from __future__ import absolute_import, print_function, division

from .base import VerticaDialect as BaseVerticaDialect


# noinspection PyAbstractClass, PyClassHasNoInit
class VerticaDialect(BaseVerticaDialect):
    driver = 'vertica_python'
    # TODO: support SQL caching, for more info see: https://docs.sqlalchemy.org/en/14/core/connections.html#caching-for-third-party-dialects
    supports_statement_cache = False


    @classmethod
    def dbapi(cls):
        vertica_python = __import__('vertica_python')
        return vertica_python
