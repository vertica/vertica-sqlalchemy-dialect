from setuptools import setup

with open("README.rst", "r") as f:
    description = f.read()

version_info = (0, 1, 0)
version = '.'.join(map(str, version_info))

setup(
    name='sqlalchemy-vertica-dialect',
    version=version,
    description='Vertica dialect for sqlalchemy',
    long_description=description,
    license='MIT',
    url='https://github.com/vishalkSimplify/sqlalchemy-vertica-dialect.git',
    download_url='https://github.com/vishalkSimplify/sqlalchemy-vertica-dialect.git/tarball/%s' % (version,),
    author='Vishal Kumar',
    author_email='vishal.kr5896@gmail.com',
    packages=(
        'sqlalchemy_vertica',
    ),
    install_requires=(
        'six >= 1.10.0',
        'sqlalchemy >= 1.4.44',
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
            'sqlalchemy_vertica.dialect_pyodbc:VerticaDialect [pyodbc]',
            'vertica.vertica_python = '
            'sqlalchemy_vertica.dialect_vertica_python:VerticaDialect [vertica-python]',
        ]
    }
)