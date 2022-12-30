
from __future__ import annotations

import random
import string
from typing import Sequence

from sqlalchemy import exc
import re
from typing import Any
from urllib.parse import quote_plus

from sqlalchemy.types import (
    BIGINT,
    BINARY,
    BOOLEAN,
    CHAR,
    DATE,
    DATETIME,
    DECIMAL,
    FLOAT,
    INTEGER,
    REAL,
    SMALLINT,
    TIME,
    VARCHAR,
)

from vertica_sqlalchemy_dialect.base import TIMESTAMP_WITH_TIMEZONE, TIMESTAMP

ischema_names_baseline = {
    "BIGINT": BIGINT,
    "BINARY": BINARY,
    # 'BIT': BIT,
    "BOOLEAN": BOOLEAN,
    "CHAR": CHAR,
    "CHARACTER": CHAR,
    "DATE": DATE,
    "DATETIME": DATETIME,
    "DEC": DECIMAL,
    "DECIMAL": DECIMAL,
    "DOUBLE": FLOAT,
    "FIXED": DECIMAL,
    "FLOAT": FLOAT,
    "INT": INTEGER,
    "INTEGER": INTEGER,
    "NUMBER": DECIMAL,
    # 'OBJECT': ?
    "REAL": REAL,
    "BYTEINT": SMALLINT,
    "SMALLINT": SMALLINT,
    "STRING": VARCHAR,
    "TEXT": VARCHAR,
    "TIME": TIME,
    "TIMESTAMP": TIMESTAMP,
    "TIMESTAMP_TZ": TIMESTAMP_WITH_TIMEZONE,
    "TINYINT": SMALLINT,
    "VARBINARY": BINARY,
    "VARCHAR": VARCHAR,
}


def random_string(
    length: int,
    prefix: str = "",
    suffix: str = "",
    choices: Sequence[str] = string.ascii_lowercase,
) -> str:
    """Our convenience function to generate random string for object names.
    Args:
        length: How many random characters to choose from choices.
        prefix: Prefix to add to random string generated.
        suffix: Suffix to add to random string generated.
        choices: A generator of things to choose from.
    """
    random_part = "".join([random.choice(choices) for _ in range(length)])
    return "".join([prefix, random_part, suffix])


def _rfc_1738_quote(text):
    return re.sub(r"[:@/]", lambda m: "%%%X" % ord(m.group(0)), text)

def _url(**db_parameters):
    """
    Composes a SQLAlchemy connect string from the given database connection
    parameters.
    Password containing special characters (e.g., '@', '%') need to be encoded to be parsed correctly.
    Unescaped password containing special characters might lead to authentication failure.
    Please follow the instructions to encode the password:
    https://github.com/snowflakedb/snowflake-sqlalchemy#escaping-special-characters-such-as---signs-in-passwords
    """
    specified_parameters = []
    if "host" in db_parameters:
        ret = "vertica+vertica_python://{user}:{password}@{host}:{port}/{dbname}".format(
            user=db_parameters.get("user", ""),
            password=_rfc_1738_quote(db_parameters.get("password", "")),
            host=db_parameters["host"],
            port=db_parameters["port"] if "port" in db_parameters else 5433,
            dbname=db_parameters["database"],
        )
        specified_parameters += ["user", "password", "host", "port", "dbname"]
    else:
        exc.ArgumentError("Host is a required arg")

    def sep(is_first_parameter):
        return "?" if is_first_parameter else "&"

    is_first_parameter = True
    for p in sorted(db_parameters.keys()):
        v = db_parameters[p]
        if p not in specified_parameters:
            encoded_value = quote_plus(v) if v else str(v)
            ret += sep(is_first_parameter) + p + "=" + encoded_value
            is_first_parameter = False
    return ret
