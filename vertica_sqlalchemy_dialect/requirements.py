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

from sqlalchemy.testing.requirements import SuiteRequirements

from sqlalchemy.testing import exclusions

# @TODO These are just sample tests. We plan to add more refined tests in future releases
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

