Vertica dialect for SQLalchemy.

Forked from the sqlalchemy-vertica repository <https://github.com/startappdev/sqlalchemy-vertica>.
Unfortunately, sqlalchemy-vertica was removed from pypi. 

import sqlalchemy as sa
import urllib

# for pyodbc connection
sa.create_engine('vertica+pyodbc:///?odbc_connect=%s' % (urllib.quote('DSN=dsn'),))

# for turbodbc connection
sa.create_engine('vertica+turbodbc:///?DSN=dsn')

# for vertica-python connection
sa.create_engine('vertica+vertica_python://user:pwd@host:port/database')
Installation
From PyPI:

pip install sqlalchemy-vertica-dialect[pyodbc,turbodbc,vertica-python]  # choose the relevant engines
From git:

git clone https://github.com/vertica/sqlalchemy-vertica
cd sqlalchemy-vertica
pip install pyodbc turbodbc vertica-python  # choose the relevant engines
python setup.py install