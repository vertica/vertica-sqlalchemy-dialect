# Copyright (c) 2018-2023 Micro Focus or one of its affiliates.
# Copyright (c) 2017 StartApp Inc.
# Copyright (c) 2015 Locus Energy
# Copyright (c) 2013 James Casbon
# Copyright (c) 2010 Bo Shi

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup

with open("README.rst", "r") as f:
    description = f.read()


version_info = (0, 0, 1)

version = '.'.join(map(str, version_info))

setup(
    name='vertica-sqlalchemy-dialect',
    version=version,
    description='Official Vertica dialect for SQLalchemy',
    long_description=description,
    license='Apache License 2.0',
    url='https://github.com/vertica/vertica-sqlalchemy-dialect',
    author='Narendra Prabhu',
    packages=(
        'vertica_sqlalchemy_dialect',
    ),
    install_requires=(
        'six >= 1.10.0',
        'sqlalchemy<2.0.0,>=1.4.0',
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
