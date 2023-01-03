from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions

#@TODO Check what these do
class Requirements(SuiteRequirements):
    @property
    def table_ddl_if_exists(self):
        return exclusions.open()
    @property
    def views(self):
        return exclusions.open()
    @property
    def nullable_booleans(self):
        """Target database allows boolean columns to store NULL."""
        return exclusions.open()
    @property
    def bound_limit_offset(self):
        """target database can render LIMIT and/or OFFSET using a bound
        parameter
        """
        return exclusions.closed()

