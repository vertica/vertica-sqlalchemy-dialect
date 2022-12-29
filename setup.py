from setuptools import setup

with open("README.rst", "r") as f:
    description = f.read()


version_info = (0, 1, 4)

version = '.'.join(map(str, version_info))

setup(
    name='vertica-sqlalchemy-dialect',
    version=version,
    description='Vertica dialect for SQLalchemy',
    long_description=description,
    license='MIT',
    url='https://github.com/vertica/vertica-sqlalchemy-dialect.git',
    download_url='https://github.com/vertica/vertica-sqlalchemy-dialect.git/tarball/%s' % (version,),
    author='Narendra Prabhu',
    packages=(
        'vertica_sqlalchemy_dialect',
    ),
    install_requires=(
        'six >= 1.10.0',

        'SQLAlchemy >= 1.4.44',
        'vertica-python>=1.1.1'
    ),
    extras_require={
        'pyodbc': [
            'pyodbc>=4.0.16',
        ],
        'vertica-python': [
            'vertica-python>=1.1.1',
        ],
    },
    entry_points={
        'sqlalchemy.dialects': [
            'vertica.pyodbc = '
            'vertica_sqlalchemy_dialect.dialect_pyodbc:VerticaDialect [pyodbc]',
            'vertica.vertica_python = '
            'vertica_sqlalchemy_dialect.dialect_vertica_python:VerticaDialect [vertica-python]',
        ]
    }
)