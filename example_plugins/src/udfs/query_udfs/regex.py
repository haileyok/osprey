import re
from typing import Dict, Optional

from osprey.engine.ast import grammar
from osprey.engine.ast_validator.validation_context import ValidationContext
from osprey.engine.query_language.udfs.registry import register
from osprey.engine.udf.arguments import ArgumentsBase, ConstExpr
from osprey.engine.udf.base import QueryUdfBase


class RegexArguments(ArgumentsBase):
    f: Optional[str]
    """The feature that is being queried"""

    r: ConstExpr[str]
    """The regex pattern to use"""


@register
class Regex(QueryUdfBase[RegexArguments, bool]):
    """
    Similar to the base RegexMatch query UDF in Osprey std, but with support
    for Clickhouse
    """

    def __init__(self, validation_context: ValidationContext, arguments: RegexArguments):
        super().__init__(validation_context, arguments)

        regex = arguments.r

        with regex.attribute_errors():
            re.compile(regex.value)
            self.regex = regex.value

        item_node = arguments.get_argument_ast('f')
        if isinstance(item_node, grammar.Name):
            self.item = item_node.identifier
        else:
            self.item = ''
            validation_context.add_error(
                message='expected variable', span=item_node.span, hint='argument `f` (feature) must be a valid variable'
            )

    def to_druid_query(self) -> Dict[str, object]:
        return {'type': 'regex', 'dimension': self.item, 'pattern': self.regex}

    def to_clickhouse_query(self) -> str:
        """
        Generate the Clickhouse query for the pattern using
        """

        # escape single quotes for sql literals
        escaped_regex = self.regex.replace("'", "\\'")
        return f"match({self.item}, '{escaped_regex}')"
