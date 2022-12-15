

from __future__ import annotations
from __future__ import absolute_import, unicode_literals, print_function, division

from typing import Any, Dict, Optional, List, Tuple

from sqlalchemy import exc
from sqlalchemy import sql
from sqlalchemy import util
from textwrap import dedent
import math
import re
from collections import defaultdict
from functools import lru_cache
import re


from sqlalchemy.dialects.postgresql import BYTEA, DOUBLE_PRECISION, INTERVAL
from sqlalchemy.dialects.postgresql.base import PGDialect, PGDDLCompiler
from sqlalchemy.engine import default
from sqlalchemy.engine import reflection
from sqlalchemy.types import INTEGER, BIGINT, SMALLINT, VARCHAR, CHAR, \
    NUMERIC, FLOAT, REAL, DATE, DATETIME, BOOLEAN, BLOB, TIMESTAMP, TIME
from sqlalchemy.sql.sqltypes import TIME, TIMESTAMP, String
from sqlalchemy.sql import sqltypes

ischema_names = {
    'INT': INTEGER,
    'INTEGER': INTEGER,
    'INT8': INTEGER,
    'BIGINT': BIGINT,
    'SMALLINT': SMALLINT,
    'TINYINT': SMALLINT,
    'CHAR': CHAR,
    'VARCHAR': VARCHAR,
    'VARCHAR2': VARCHAR,
    'TEXT': VARCHAR,
    'NUMERIC': NUMERIC,
    'DECIMAL': NUMERIC,
    'NUMBER': NUMERIC,
    'MONEY': NUMERIC,
    'FLOAT': FLOAT,
    'FLOAT8': FLOAT,
    'REAL': REAL,
    'DOUBLE': DOUBLE_PRECISION,
    'TIMESTAMP': TIMESTAMP,
    'TIMESTAMP WITH TIMEZONE': TIMESTAMP,
    'TIMESTAMPTZ': TIMESTAMP(timezone=True),
    'TIME': TIME,
    'TIME WITH TIMEZONE': TIME,
    'TIMETZ': TIME(timezone=True),
    'INTERVAL': INTERVAL,
    'DATE': DATE,
    'DATETIME': DATETIME,
    'SMALLDATETIME': DATETIME,
    'BINARY': BLOB,
    'VARBINARY': BLOB,
    'RAW': BLOB,
    'BYTEA': BYTEA,
    'BOOLEAN': BOOLEAN,
    'LONG VARBINARY': BLOB,
    'LONG VARCHAR': VARCHAR,
    'GEOMETRY': BLOB,
}

class UUID(String):
    """The SQL UUID type."""

    __visit_name__ = "UUID"
    
    
class TIMESTAMP_WITH_PRECISION(TIMESTAMP):
    """The SQL TIMESTAMP With Precision type.

    Since Vertica supports precision values for timestamp this allows ingestion
    of timestamp fields with precision values.
    PS: THIS DATA IS CURRENTLY UNUSED, IT JUST FIXES INGESTION PROBLEMS
    TODO: Should research the possibility of reflecting the precision in the schema

    """

    __visit_name__ = "TIMESTAMP"

    def __init__(self, timezone=False, precision=None):
        """Construct a new :class:`_types.TIMESTAMP_WITH_PRECISION`.

        :param timezone: boolean.  Indicates that the TIMESTAMP type should
         enable timezone support, if available on the target database.
         On a per-dialect basis is similar to "TIMESTAMP WITH TIMEZONE".
         If the target database does not support timezones, this flag is
         ignored.
        :param precision: integer.  Indicates the PRECISION field when provided


        """
        super(TIMESTAMP, self).__init__(timezone=timezone)
        self.precision = precision


def TIMESTAMP_WITH_TIMEZONE(*args, **kwargs):
    kwargs["timezone"] = True
    return TIMESTAMP_WITH_PRECISION(*args, **kwargs)


def TIME_WITH_TIMEZONE(*args, **kwargs):
    kwargs["timezone"] = True
    return TIME(*args, **kwargs)


class VerticaDDLCompiler(PGDDLCompiler):
    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        # noinspection PyUnusedLocal
        impl_type = column.type.dialect_impl(self.dialect)
        # noinspection PyProtectedMember
        if column.primary_key and column is column.table._autoincrement_column:
            colspec += " AUTO_INCREMENT"
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type)
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec


class VerticaInspector(reflection.Inspector):
    dialect:  VerticaDialect

    def get_projection_names(
        self, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        r"""Return all Models names within a particular schema. """

        return self.dialect.get_projection_names(
            self.bind, schema, info_cache=self.info_cache, **kw
        )

    def get_models_names(self, schema=None):
        """Return all Ml models in `schema`.

        :param schema: Optional, retrieve names from a non-default schema.
         For special quoting, use :class:`.quoted_name`.

        """

        return self.dialect.get_models_names(
            self.bind, schema, info_cache=self.info_cache
        )

    def _get_extra_tags(self, table, schema=None):
        """Return owner name for table as a Tag .

        :param schema: Optional.
        :param: table: Name of the table

        """
        return self.dialect._get_extra_tags(
            self.bind, table, schema
        )

    def get_projection_comment(self, projection_name, schema=None, **kw):
        """Return information about the table properties for ``table_name``.
            as key and value.

        :param: projection_name
        :param: schema
        return dictionary
        """

        return self.dialect.get_projection_comment(
            self.bind, projection_name, schema, info_cache=self.info_cache, **kw
        )

    def _get_properties_keys(self, db_name: str, schema: str, level: str) -> dict:
        """ will return all properties as key : value 
            which should come under the given schema on the given level 
            eg. schema or database level

        Args:
            db_name (str): _description_
            schema (str): Name of the schema to return
        """
        return self.dialect._get_properties_keys(
            self.bind, db_name, schema, level
        )

    def get_model_comment(self, model_name, schema=None, **kw):
        """Return information about the ML Model properties for ``schema``.
            as key and value.

        :param: Model_name
        :param: schema
        return dictionary
        """

        return self.dialect.get_model_comment(
            self.bind, model_name, schema, info_cache=self.info_cache, **kw
        )

    def get_Oauth_names(self, schema=None):
        """Return all O auth names .

        :param schema: Optional, retrieve names from a non-default schema.
         For special quoting, use :class:`.quoted_name`.

        """

        return self.dialect.get_Oauth_names(
            self.bind, schema, info_cache=self.info_cache
        )

    def get_oauth_comment(self, _name, schema=None, **kw):
        """Return information about the O Auth properties .
            as key and value.

        :param: oauth_names
        :param: schema
        return dictionary
        """

        return self.dialect.get_oauth_comment(
            self.bind, _name, schema, info_cache=self.info_cache, **kw
        )


# noinspection PyArgumentList,PyAbstractClass
class VerticaDialect(default.DefaultDialect):

    name = 'vertica'
    ischema_names = ischema_names
    ddl_compiler = VerticaDDLCompiler
    inspector = VerticaInspector

    def __init__(self, json_serializer=None, json_deserializer=None, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)

        self._json_deserializer = json_deserializer
        self._json_serializer = json_serializer

    def initialize(self, connection):
        super().initialize(connection)

    def _get_default_schema_name(self, connection):
        return connection.scalar("SELECT current_schema()")

    def _get_server_version_info(self, connection):
        v = connection.scalar("SELECT version()")
        m = re.match(r".*Vertica Analytic Database v(\d+)\.(\d+)\.(\d)+.*", v)
        if not m:
            raise AssertionError(
                "Could not determine version from string '%(ver)s'" % {'ver': v})
        return tuple([int(x) for x in m.group(1, 2, 3) if x is not None])

    # noinspection PyRedeclaration
    def _get_default_schema_name(self, connection):
        return connection.scalar("SELECT current_schema()")

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        return [], opts

    def has_schema(self, connection, schema):
        has_schema_sql = sql.text(dedent("""
            SELECT EXISTS (
            SELECT schema_name
            FROM v_catalog.schemata
            WHERE lower(schema_name) = '%(schema)s')
        """ % {'schema': schema.lower()}))

        c = connection.execute(has_schema_sql)
        return bool(c.scalar())

    def has_table(self, connection, table_name, schema=None):
        if schema is None:
            schema = self._get_default_schema_name(connection)

        has_table_sql = sql.text(dedent("""
            SELECT EXISTS (
            SELECT table_name
            FROM v_catalog.all_tables
            WHERE lower(table_name) = '%(table)s'
            AND lower(schema_name) = '%(schema)s')
        """ % {'schema': schema.lower(), 'table': table_name.lower()}))

        c = connection.execute(has_table_sql)
        return bool(c.scalar())

    def has_sequence(self, connection, sequence_name, schema=None):
        if schema is None:
            schema = self._get_default_schema_name(connection)

        has_seq_sql = sql.text(dedent("""
            SELECT EXISTS (
            SELECT sequence_name
            FROM v_catalog.sequences
            WHERE lower(sequence_name) = '%(sequence)s'
            AND lower(sequence_schema) = '%(schema)s')
        """ % {'schema': schema.lower(), 'sequence': sequence_name.lower()}))

        c = connection.execute(has_seq_sql)
        return bool(c.scalar())

    def has_type(self, connection, type_name, schema=None):
        has_type_sql = sql.text(dedent("""
            SELECT EXISTS (
            SELECT type_name
            FROM v_catalog.types
            WHERE lower(type_name) = '%(type)s')
        """ % {'type': type_name.lower()}))

        c = connection.execute(has_type_sql)
        return bool(c.scalar())

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        get_schemas_sql = sql.text(dedent("""
            SELECT schema_name
            FROM v_catalog.schemata
        """))

        c = connection.execute(get_schemas_sql)
        return [row[0] for row in c if not row[0].startswith('v_')]

    @reflection.cache
    def get_table_comment(self, connection, table_name, schema=None, **kw):

        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {'schema': schema.lower()}
        else:
            schema_condition = "1"

        sct = sql.text(dedent("""
                SELECT create_time , table_name
                FROM v_catalog.tables
                WHERE lower(table_name) = '%(table)s'
                AND %(schema_condition)s
                UNION ALL
                SELECT create_time , table_name
                FROM V_CATALOG.VIEWS
                WHERE lower(table_name) = '%(table)s'
                AND %(schema_condition)s
                
            """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))

        sts = sql.text(dedent("""
                SELECT ROUND(SUM(used_bytes) / 1024 ) AS table_size
                FROM v_monitor.column_storage
                WHERE lower(anchor_table_name) = '%(table)s'
                
            """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))

        columns = ""
        for column in connection.execute(sct):
            columns = column['create_time']

        for table_size in connection.execute(sts):
            if table_size[0] is None:
                TableSize = 0
            else:
                TableSize = math.trunc(table_size['table_size'])

        return {"text": "This Vertica module is still is development Process", "properties": {"create_time": str(columns), "Total_Table_Size": str(TableSize) + " KB"}}



    @reflection.cache
    def get_table_oid(self, connection, table_name, schema=None, **kw):
        if schema is None:
            schema = self._get_default_schema_name(connection)

        get_oid_sql = sql.text(dedent("""
            SELECT A.table_id
            FROM
                (SELECT table_id, table_name, table_schema FROM v_catalog.tables
                    UNION
                 SELECT table_id, table_name, table_schema FROM v_catalog.views) AS A
            WHERE lower(A.table_name) = '%(table)s'
            AND lower(A.table_schema) = '%(schema)s'
        """ % {'schema': schema.lower(), 'table': table_name.lower()}))

        c = connection.execute(get_oid_sql)
        table_oid = c.scalar()

        if table_oid is None:
            raise exc.NoSuchTableError(table_name)
        return table_oid

    def get_projection_names(self, connection, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(projection_schema) = '%(schema)s'" % {
                'schema': schema.lower()}
        else:
            schema_condition = "1"

        get_projection_sql = sql.text(dedent("""
            SELECT projection_name
            from v_catalog.projections
            WHERE lower(projection_schema) =  '%(schema)s'
            ORDER BY projection_name
            """ % {'schema': schema}))

        c = connection.execute(get_projection_sql)
        return [row[0] for row in c]

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {
                'schema': schema.lower()}
        else:
            schema_condition = "1"

        get_tables_sql = sql.text(dedent("""
            SELECT table_name
            FROM v_catalog.tables
            WHERE %(schema_condition)s
            ORDER BY table_schema, table_name
        """ % {'schema_condition': schema_condition}))

        c = connection.execute(get_tables_sql)
        return [row[0] for row in c]

    @reflection.cache
    def get_temp_table_names(self, connection, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {
                'schema': schema.lower()}
        else:
            schema_condition = "1"

        get_tables_sql = sql.text(dedent("""
                SELECT table_name
                FROM v_catalog.tables
                WHERE %(schema_condition)s
                AND IS_TEMP_TABLE
                ORDER BY table_schema, table_name
            """ % {'schema_condition': schema_condition}))

        c = connection.execute(get_tables_sql)
        return [row[0] for row in c]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {
                'schema': schema.lower()}
        else:
            schema_condition = "1"

        get_views_sql = sql.text(dedent("""
            SELECT table_name
            FROM v_catalog.views
            WHERE %(schema_condition)s
            ORDER BY table_schema, table_name
        """ % {'schema_condition': schema_condition}))

        c = connection.execute(get_views_sql)
        return [row[0] for row in c]

    
    def get_view_definition(self, connection, view_name, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {
                "schema": schema.lower()
            }
        else:
            schema_condition = "1"

        view_def = connection.scalar(
            sql.text(
                dedent(
                    """
                    SELECT VIEW_DEFINITION
                    FROM V_CATALOG.VIEWS
                    WHERE table_name='%(view_name)s' AND %(schema_condition)s
                    """
                    % {"view_name": view_name, "schema_condition": schema_condition}
                )
            )
        )

        return view_def

    @reflection.cache
    def get_temp_view_names(self, connection, schema=None, **kw):
        return []

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {
                'schema': schema.lower()}
        else:
            schema_condition = "1"

        s = sql.text(dedent("""
            SELECT column_name, data_type, column_default, is_nullable
            FROM v_catalog.columns
            WHERE lower(table_name) = '%(table)s'
            AND %(schema_condition)s
            UNION ALL
            SELECT column_name, data_type, '' as column_default, true as is_nullable
            FROM v_catalog.view_columns
            WHERE lower(table_name) = '%(table)s'
            AND %(schema_condition)s
            UNION ALL
            SELECT projection_column_name,data_type,'' as column_default, true as is_nullable
            FROM PROJECTION_COLUMNS
            WHERE lower(projection_name) = '%(table)s'
            AND %(schema_condition)s
        """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))

        spk = sql.text(dedent("""
            SELECT column_name
            FROM v_catalog.primary_keys
            WHERE lower(table_name) = '%(table)s'
            AND constraint_type = 'p'
            AND %(schema_condition)s
        """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))

        pk_columns = [x[0] for x in connection.execute(spk)]
        columns = []
        for row in connection.execute(s):
            name = row.column_name
            dtype = row.data_type.lower()
            primary_key = name in pk_columns
            default = row.column_default
            nullable = row.is_nullable

            column_info = self._get_column_info(
                name,
                dtype,
                default,
                nullable,
                schema,
            )
            column_info.update({'primary_key': primary_key})
            columns.append(column_info)
        return columns

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        if schema is None:
            schema = self._get_default_schema_name(connection)

        get_constrains_sql = sql.text(dedent("""
            SELECT constraint_name, column_name
            FROM v_catalog.constraint_columns
            WHERE lower(table_name) = '%(table)s'
            -- AND constraint_type IN ('p', 'u')
            AND lower(table_schema) = '%(schema)s'
        """ % {'schema': schema.lower(), 'table': table_name.lower()}))

        c = connection.execute(get_constrains_sql)
        if c.rowcount <= 0:
            return []

        constraints, columns = zip(*c)
        result_dict = {
            unique_con: [col for con, col in zip(
                constraints, columns) if con == unique_con]
            for unique_con in set(constraints)
        }

        return [{"name": name, "column_names": cols} for name, cols in result_dict.items()]

    @reflection.cache
    def get_check_constraints(
            self, connection, table_name, schema=None, **kw):
        table_oid = self.get_table_oid(connection, table_name, schema,
                                       info_cache=kw.get('info_cache'))

        constraints_sql = sql.text(dedent("""
            SELECT constraint_name, column_name
            FROM v_catalog.constraint_columns
            WHERE table_id = %(oid)s
            AND constraint_type = 'c'
        """ % {'oid': table_oid}))

        c = connection.execute(constraints_sql)

        return [{'name': name, 'sqltext': col} for name, col in c.fetchall()]

    def normalize_name(self, name):
        name = name and name.rstrip()
        if name is None:
            return None
        return name.lower()

    def denormalize_name(self, name):
        return name

    # methods allows table introspection to work
    @reflection.cache
    def get_pk_constraint(self, bind, table_name, schema=None, **kw):
        return {'constrained_columns': [], 'name': 'undefined'}

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    @reflection.cache
    def get_indexes(self, connection, table_name, schema, **kw):
        return []

    # Disable index creation since that's not a thing in Vertica.
    # noinspection PyUnusedLocal
    def visit_create_index(self, create):
        return None

    
    def _get_column_info(  # noqa: C901
        self, name, data_type, default, is_nullable, schema=None
    ):

        attype: str = re.sub(r"\(.*\)", "", data_type)

        charlen = re.search(r"\(([\d,]+)\)", data_type)
        if charlen:
            charlen = charlen.group(1)  # type: ignore
        args = re.search(r"\((.*)\)", data_type)
        if args and args.group(1):
            args = tuple(re.split(r"\s*,\s*", args.group(1)))  # type: ignore
        else:
            args = ()  # type: ignore
        kwargs: Dict[str, Any] = {}

        if attype == "numeric":
            if charlen:
                prec, scale = charlen.split(",")  # type: ignore
                args = (int(prec), int(scale))  # type: ignore
            else:
                args = ()  # type: ignore
        elif attype == "integer":
            args = ()  # type: ignore
        elif attype in ("timestamptz", "timetz"):
            kwargs["timezone"] = True
            if charlen:
                kwargs["precision"] = int(charlen)  # type: ignore
            args = ()  # type: ignore
        elif attype in ("timestamp", "time"):
            kwargs["timezone"] = False
            if charlen:
                kwargs["precision"] = int(charlen)  # type: ignore
            args = ()  # type: ignore
        elif attype.startswith("interval"):
            field_match = re.match(r"interval (.+)", attype, re.I)
            if charlen:
                kwargs["precision"] = int(charlen)  # type: ignore
            if field_match:
                kwargs["fields"] = field_match.group(1)  # type: ignore
            attype = "interval"
            args = ()  # type: ignore
        elif attype == "date":
            args = ()  # type: ignore
        elif charlen:
            args = (int(charlen),)  # type: ignore

        while True:
            if attype.upper() in self.ischema_names:
                coltype = self.ischema_names[attype.upper()]
                break
            else:
                coltype = None
                break

        self.ischema_names["UUID"] = UUID
        self.ischema_names["TIMESTAMP"] = TIMESTAMP_WITH_PRECISION
        self.ischema_names["TIMESTAMPTZ"] = TIMESTAMP_WITH_TIMEZONE
        self.ischema_names["TIMETZ"] = TIME_WITH_TIMEZONE

        if coltype:
            coltype = coltype(*args, **kwargs)
        else:
            util.warn("Did not recognize type '%s' of column '%s'" % (attype, name))
            coltype = sqltypes.NULLTYPE
        # adjust the default value
        autoincrement = False
        if default is not None:
            match = re.search(r"""(nextval\(')([^']+)('.*$)""", default)
            if match is not None:
                if issubclass(coltype._type_affinity, sqltypes.Integer):
                    autoincrement = True
                # the default is related to a Sequence
                sch = schema
                if "." not in match.group(2) and sch is not None:
                    # unconditionally quote the schema name.  this could
                    # later be enhanced to obey quoting rules /
                    # "quote schema"
                    default = (
                        match.group(1)
                        + ('"%s"' % sch)
                        + "."
                        + match.group(2)
                        + match.group(3)
                    )

        column_info = dict(
            name=name,
            type=coltype,
            nullable=is_nullable,
            default=default,
            autoincrement=autoincrement,
        )
        return column_info

    @reflection.cache
    def get_models_names(self, connection, schema=None, **kw):

        if schema is not None:
            schema_condition = "lower(schema_name) = '%(schema)s'" % {
                'schema': schema.lower()}
        else:
            schema_condition = "1"

        get_models_sql = sql.text(dedent("""
            SELECT model_name 
            FROM models
            WHERE lower(schema_name) =  '%(schema)s'
            ORDER BY model_name
        """ % {'schema': schema}))

        c = connection.execute(get_models_sql)

        return [row[0] for row in c]

    def get_pk_constraint(self, connection, table_name, schema: None, **kw):
        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {
                'schema': schema.lower()}
        else:
            schema_condition = "1"

        spk = sql.text(dedent("""
                SELECT column_name
                FROM v_catalog.primary_keys
                WHERE lower(table_name) = '%(table)s'
                AND constraint_type = 'p'
                AND %(schema_condition)s
            """ % {'table': table_name.lower(), 'schema_condition': schema_condition}))

        pk_columns = []

        for row in connection.execute(spk):
            columns = row['column_name']
            pk_columns.append(columns)

        return {'constrained_columns': pk_columns, 'name': pk_columns}
    

    def _get_properties_keys(self, connection, db_name, schema, level=None):
        try:
            properties_keys = dict()
            if level == "schema":
             
                properties_keys = self._get_schema_keys(
                    connection, db_name, schema)
            if level == "database":
               
                properties_keys = self._get_database_keys(
                    connection, db_name)
          
            return properties_keys
        except Exception as e:
            import traceback
            traceback.print_exe()
            print("Error in finding schema keys in vertica ")

    def _get_schema_keys(self, connection, db_name, schema):
        try:
            
            if schema is not None:
                schema_condition = "lower(table_schema) = '%(schema)s'" % {
                    'schema': schema.lower()}

            # Projection count
            projection_count_query = sql.text(dedent("""
                SELECT 
                    COUNT(projection_name)  as pc
                from 
                    v_catalog.projections 
                WHERE lower(projection_schema) = '%(schema)s'
            """ % {'schema_condition': schema_condition, "schema": schema}))

            projection_count = None
            for each in connection.execute(projection_count_query):
                projection_count = each.pc

            UDL_LANGUAGE = sql.text(dedent("""
                SELECT lib_name , description 
                    FROM USER_LIBRARIES
                WHERE lower(schema_name) = '%(schema)s'
            """ % {'schema_condition': schema_condition, "schema": schema}))

            # UDX list
            UDX_functions_qry = sql.text(dedent("""
                SELECT 
                    function_name 
                FROM 
                    USER_FUNCTIONS
                Where schema_name  = '%(schema)s'
            """ % {'schema': schema, 'schema_condition': schema_condition}))
            udx_list = ""
            for each in connection.execute(UDX_functions_qry):
                udx_list += each.function_name + ", "

            # UDX Language
            user_defined_library = ""
            for data in connection.execute(UDL_LANGUAGE):
                user_defined_library += f"{data['lib_name']} -- {data['description']} |  "

            return {"projection_count": projection_count,
                    'udx_list': udx_list, 'Udx_langauge': user_defined_library}

        except Exception as e:
            print("Exception in _get_schema_keys from vertica ")
            

    def _get_database_keys(self, connection, db_name):
        try:
            print("")
            # Query for CLUSTER TYPE
            cluster_type_qry = sql.text(dedent("""
                SELECT 
                CASE COUNT(*) 
                    WHEN 0 
                    THEN 'Enterprise' 
                    ELSE 'Eon' 
                END AS database_mode 
                FROM v_catalog.shards
            """))

            communal_storage_path = sql.text(dedent("""
                SELECT location_path from storage_locations 
                    WHERE sharing_type = 'COMMUNAL'
            """))

            cluster_type = ""
            communical_path = ""
            cluster_type_res = connection.execute(cluster_type_qry)
            for each in cluster_type_res:
                cluster_type = each.database_mode
                if cluster_type.lower() == 'eon':
                    for each in connection.execute(communal_storage_path):
                        communical_path += str(each.location_path) + " | "

            SUBCLUSTER_SIZE = sql.text(dedent("""
                            SELECT subclusters.subcluster_name , CAST(sum(disk_space_used_mb // 1024) as varchar(10)) as subclustersize from subclusters  
                            inner join disk_storage using (node_name) 
                            group by subclusters.subcluster_name
                            """))

            subclusters = " "
            for data in connection.execute(SUBCLUSTER_SIZE):
                subclusters += f"{data['subcluster_name']} -- {data['subclustersize']} GB |  "

            cluster__size = sql.text(dedent("""
                select ROUND(SUM(disk_space_used_mb) //1024 ) as cluster_size
                from disk_storage
            """))
            cluster_size = ""
            for each in connection.execute(cluster__size):
                cluster_size = str(each.cluster_size) + " GB"

            return {"cluster_type": cluster_type, "cluster_size": cluster_size, 'Subcluster': subclusters,
                    "communinal_storage_path": communical_path}

        except Exception as e:
            print("Exception in _get_database_keys")
            
    # @reflection.cache
    def _get_extra_tags(
        self, connection, name, schema=None
    ) -> Optional[Dict[str, str]]:

        if schema is not None:
            schema_condition = "lower(table_schema) = '%(schema)s'" % {
                'schema': schema.lower()}
        else:
            schema_condition = "1"

        owner_res = None
        if name == "table":
            table_owner_command = sql.text(dedent("""
                SELECT table_name, owner_name
                FROM v_catalog.tables
                WHERE %(schema_condition)s
                """ % {'schema_condition': schema_condition}))

            owner_res = connection.execute(table_owner_command)

        elif name == "projection":
            table_owner_command = sql.text(dedent("""
                SELECT projection_name as table_name, owner_name
                FROM v_catalog.projections
                WHERE lower(projection_schema) = '%(schema)s'
                """ % {'schema': schema.lower()}))
            owner_res = connection.execute(table_owner_command)

        elif name == "view":
            table_owner_command = sql.text(dedent("""
                SELECT table_name, owner_name
                FROM v_catalog.views
                WHERE %(schema_condition)s
                """ % {'schema_condition': schema_condition}))
            owner_res = connection.execute(table_owner_command)

        final_tags = dict()
        for each in owner_res:
            final_tags[each['table_name']] = each['owner_name']
        return final_tags
    
    
    @reflection.cache
    def get_projection_comment(self, connection, projection_name, schema=None, **kw):
        if schema is not None:
            schema_condition = "lower(projection_schema) = '%(schema)s'" % {
                'schema': schema.lower()}
        else:
            schema_condition = "1"

        src = sql.text(dedent("""
                SELECT ros_count 
                FROM v_monitor.projection_storage
                WHERE lower(projection_name) = '%(table)s'

            """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

        sig = sql.text(dedent("""
                SELECT is_segmented 
                FROM v_catalog.projections 
                WHERE lower(projection_name) = '%(table)s'
            """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

        spk = sql.text(dedent("""
                SELECT   partition_key
                FROM v_monitor.partitions
                WHERE lower(projection_name) = '%(table)s'
                LIMIT 1
            """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

        spt = sql.text(dedent("""
                SELECT is_super_projection,is_key_constraint_projection,is_aggregate_projection,has_expressions
                FROM v_catalog.projections
                WHERE lower(projection_name) = '%(table)s'
            """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

        snp = sql.text(dedent("""
                SELECT Count(ros_id) as np
                FROM v_monitor.partitions
                WHERE lower(projection_name) = '%(table)s'
            """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

        ssk = sql.text(dedent("""
                SELECT  segment_expression 
                FROM v_catalog.projections
                WHERE lower(projection_name) = '%(table)s'
            """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

        sps = sql.text(dedent("""
                SELECT ROUND(used_bytes // 1024)   AS used_bytes 
                from v_monitor.projection_storage
                WHERE lower(projection_name) = '%(table)s'
            """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

        depot_pin_policy = sql.text(dedent("""
                SELECT COUNT(*)
                FROM DEPOT_PIN_POLICIES
                WHERE lower(object_name) = '%(table)s'
            """ % {'table': projection_name.lower(), 'schema_condition': schema_condition}))

        ros_count = ""
        partition_key = ""
        is_segmented = ""
        projection_type = []
        partition_number = ""
        segmentation_key = ""
        projection_size = ""
        cached_projection = ""

        for data in connection.execute(sig):
            is_segmented = data['is_segmented']
            if is_segmented:
                for data in connection.execute(ssk):
                    segmentation_key = str(data)

        for data in connection.execute(src):
            ros_count = data['ros_count']

        for data in connection.execute(spk):
            partition_key = data['partition_key']

        for data in connection.execute(spt):
            lst = ["is_super_projection", "is_key_constraint_projection",
                   "is_aggregate_projection", "is_shared"]

            i = 0
            for d in range(len(data)):
                if data[i]:
                    projection_type.append(lst[i])
                i += 1

        for data in connection.execute(snp):
            partition_number = data.np

        for data in connection.execute(sps):
            projection_size = data['used_bytes']

        for data in connection.execute(depot_pin_policy):
            if data[0] > 0:
                cached_projection = "True"
            else:
                cached_projection = "False"

        return {"text": "This Vertica module is still is development Process for Projections",
                "properties": {"ROS Count": str(ros_count), "is_segmented": str(is_segmented),
                               "Projection Type": str(projection_type), "Partition Key": str(partition_key),
                               "Number of Partition": str(partition_number),
                               "Segmentation_key": segmentation_key,
                               "Projection SIze": str(projection_size) + " KB",
                               "Projection Cached": str(cached_projection)}}
        
    @reflection.cache
    def get_model_comment(self, connection, model_name, schema=None, **kw):

        if schema is not None:
            schema_condition = "lower(schema_name) = '%(schema)s'" % {
                'schema': schema.lower()}
        else:
            schema_condition = "1"

        model_used_by = sql.text(dedent("""
                select owner_name from models
                where model_name = '%(model)s'
                
            """ % {'model': model_name}))

        model_attr_name = sql.text(dedent("""
                SELECT 
                    GET_MODEL_ATTRIBUTE 
                        ( USING PARAMETERS model_name='%(schema)s.%(model)s');
                
            """ % {'model': model_name, 'schema': schema}))

        used_by = ""
        attr_name = []
        attr_details = []
        for data in connection.execute(model_used_by):
            used_by = data['owner_name']

        for data in connection.execute(model_attr_name):

            attributes = {
                "attr_name": data[0], "attr_fields": data[1], "#_of_rows": data[2]}

            attr_name.append(attributes)

        attributes_details = []
        for data in attr_name:
            attr_details_dict = dict()
            attr_names = data['attr_name']
            attr_fields = str(data['attr_fields']).split(',')

            get_attr_details = sql.text(dedent("""
                    SELECT 
                        GET_MODEL_ATTRIBUTE 
                            ( USING PARAMETERS model_name='%(schema)s.%(model)s', attr_name='%(attr_name)s');
                    
                """ % {'model': model_name, 'schema': schema, 'attr_name': attr_names}))

            value_final = dict()
            attr_details_dict = {"attr_name": attr_names}
            for data in connection.execute(get_attr_details):

                if len(attr_fields) > 1:

                    for index, each in enumerate(attr_fields):
                        if each not in value_final:
                            value_final[each] = list()
                        value_final[each].append(data[index])

                else:
                    if attr_fields[0] not in value_final:
                        value_final[attr_fields[0]] = list()
                    value_final[attr_fields[0]].append(data[0])

            attr_details_dict.update(value_final)
            attributes_details.append(attr_details_dict)

        return {"text": "This Vertica module is still is development Process", "properties": {"used_by": str(used_by),
                "Model Attrributes ": str(attr_name), "Model Specifications": str(attributes_details)}}
        
    @reflection.cache
    def get_oauth_comment(self, connection, model_name, schema=None, **kw):

        get_oauth_comments = sql.text(dedent("""
                            SELECT auth_oid ,is_auth_enabled, is_fallthrough_enabled,auth_parameters ,auth_priority ,address_priority from v_catalog.client_auth
                                WHERE auth_method = 'OAUTH'

                                """))
        client_id = ""
        client_secret = ""
        for data in connection.execute(get_oauth_comments):

            whole_data = str(data['auth_parameters']).split(", ")
            client_id_data = whole_data[0].split("=")
            if client_id_data:
                # client_data.update({client_id_data[0] : client_id_data[1]})
                client_id = client_id_data[1]

            client_secret_data = whole_data[1].split("=")
            if client_secret_data:
                # client_data.update({client_secret_data[0] : client_secret_data[1]})
                client_secret = client_secret_data[1]

            client_discorvery_url = whole_data[2].split("=")
            if client_discorvery_url:
                # client_data.update({client_secret_data[0] : client_secret_data[1]})
                discovery_url = client_discorvery_url[1]

            client_introspect_url = whole_data[3].split("=")
            if client_introspect_url:
                # client_data.update({client_secret_data[0] : client_secret_data[1]})
                introspect_url = client_introspect_url[1]

            auth_oid = data['auth_oid']
            is_auth_enabled = data['is_auth_enabled']
            auth_priority = data['auth_priority']
            address_priority = data['address_priority']
            is_fallthrough_enabled = data['is_fallthrough_enabled']

        return {"text": "This Vertica module is still is development Process", "properties": {"discovery_url ": str(discovery_url),
                "client_id  ": str(client_id), "introspect_url ": str(introspect_url), "auth_oid ": str(auth_oid), "client_secret ": str(client_secret),
                "is_auth_enabled": str(is_auth_enabled), "auth_priority ": str(auth_priority), "address_priority ": str(address_priority), "is_fallthrough_enabled": str(is_fallthrough_enabled), }}

    