# # test/test_suite.py
# #@TODO Check what these do
# from sqlalchemy.testing.suite import *

# from sqlalchemy.testing.suite import IntegerTest as _IntegerTest

# from sqlalchemy.testing.suite import (
#     CastTypeDecoratorTest as _CastTypeDecoratorTest,
# )
# from sqlalchemy.testing.suite import (
#     ComponentReflectionTest as _ComponentReflectionTest,
# )
# from sqlalchemy.testing.suite import (
#     ComponentReflectionTestExtra as _ComponentReflectionTestExtra,
# )
# from sqlalchemy.testing.suite import DateTimeTest as _DateTimeTest
# from sqlalchemy.testing.suite import (
#     DifficultParametersTest as _DifficultParametersTest,
# )
# from sqlalchemy.testing.suite import ExistsTest as _ExistsTest
# from sqlalchemy.testing.suite import (
#     ExpandingBoundInTest as _ExpandingBoundInTest,
# )
# from sqlalchemy.testing.suite import (
#     FetchLimitOffsetTest as _FetchLimitOffsetTest,
# )
# from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
# from sqlalchemy.testing.suite import IntegerTest as _IntegerTest
# from sqlalchemy.testing.suite import JoinTest as _JoinTest
# from sqlalchemy.testing.suite import LikeFunctionsTest as _LikeFunctionsTest
# from sqlalchemy.testing.suite import (
#     LongNameBlowoutTest as _LongNameBlowoutTest,
# )
# from sqlalchemy.testing.suite import NumericTest as _NumericTest
# from sqlalchemy.testing.suite import OrderByLabelTest as _OrderByLabelTest
# from sqlalchemy.testing.suite import (
#     QuotedNameArgumentTest as _QuotedNameArgumentTest,
# )
# from sqlalchemy.testing.suite import TableDDLTest as _TableDDLTest

# class IntegerTest(_IntegerTest):
#     @testing.skip("vertica")
#     def test_special_type(self):
#         # Access SQL does not do CAST in the conventional way
#         return