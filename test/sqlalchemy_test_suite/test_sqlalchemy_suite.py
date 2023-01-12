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


# # These are placecholder tests that need to be improved in future releases. The integration test should be pretty ok
# # for straightforward tests with Metadata
# import pytest
# from sqlalchemy import Integer, testing
# from sqlalchemy.schema import Column, Sequence, Table
# from sqlalchemy.testing import config
# from sqlalchemy.testing.assertions import eq_
# from sqlalchemy.testing.suite import (
#     CompositeKeyReflectionTest as _CompositeKeyReflectionTest,
# )
# from sqlalchemy.testing.suite import FetchLimitOffsetTest as _FetchLimitOffsetTest
# from sqlalchemy.testing.suite import HasSequenceTest as _HasSequenceTest
# from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
# from sqlalchemy.testing.suite import LikeFunctionsTest as _LikeFunctionsTest
# from sqlalchemy.testing.suite import LongNameBlowoutTest as _LongNameBlowoutTest
# from sqlalchemy.testing.suite import SimpleUpdateDeleteTest as _SimpleUpdateDeleteTest
# from sqlalchemy.testing.suite import *  # noqa

# # Unsupported by Vertica

# del ComponentReflectionTest  # require indexes not supported by Vertica
# del HasIndexTest  # require indexes not supported by Vertica


# class InsertBehaviorTest(_InsertBehaviorTest):
#     @pytest.mark.skip(
#         "Vertica does not support inserting empty values"
#     )
#     def test_empty_insert(self, connection):
#         pass

#     @pytest.mark.skip(
#         "Vertica does not support inserting empty values"
#     )
#     def test_empty_insert_multiple(self, connection):
#         pass



# class HasSequenceTest(_HasSequenceTest):
#     # Example of overriding tests
#     @classmethod
#     def define_tables(cls, metadata):
#         Sequence("seq1", metadata=metadata)
#         Sequence("seq2", metadata=metadata)
#         if testing.requires.schemas.enabled:
#             Sequence("seq1", schema=config.test_schema, metadata=metadata)
#             Sequence("seq2", schema=config.test_schema, metadata=metadata)
#         Table(
#             "user_id_table",
#             metadata,
#             Column("id", Integer, primary_key=True),
#         )