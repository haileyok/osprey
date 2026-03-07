from dataclasses import dataclass
from typing import List, Optional, Self, cast

from ddtrace.internal.logger import get_logger
from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.effects import EffectToCustomExtractedFeatureBase
from osprey.engine.language_types.rules import RuleT
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.utils.types import add_slots

logger = get_logger('atproto_list')


class AtprotoListArguments(ArgumentsBase):
    did: str
    list_uri: str
    apply_if: Optional[RuleT] = None


@dataclass
class AtprotoListEffect(EffectToCustomExtractedFeatureBase[List[str]]):
    """Stores a list effect of a WhenRules(...) invocation, which stores the label mutations that should occur once
    a given action has finished classification."""

    did: str
    """The entity that the effect will be applied on."""

    list_uri: str
    """The AT-URI of the list that the entity is being added to."""

    dependent_rule: Optional[RuleT] = None
    """If set, the effect will only be applied if the dependent rule evaluates to true."""

    suppressed: bool = False
    """If set to true, the effect should not be applied."""

    def to_str(self) -> str:
        return f'{self.did}|{self.list_uri}'

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[List[str]]:
        return AtprotoListEffectsExtractedFeature(effects=cast(List[AtprotoListEffect], values))


@add_slots
@dataclass
class AtprotoListEffectsExtractedFeature(CustomExtractedFeature[List[str]]):
    effects: List[AtprotoListEffect]

    @classmethod
    def feature_name(cls) -> str:
        return 'atproto_list'

    def get_serializable_feature(self) -> List[str] | None:
        return [effect.to_str() for effect in self.effects]


def synthesize_effect(arguments: AtprotoListArguments) -> AtprotoListEffect:
    return AtprotoListEffect(
        did=arguments.did,
        list_uri=arguments.list_uri,
        dependent_rule=arguments.apply_if,
        suppressed=arguments.has_argument_ast('apply_if') and arguments.apply_if is None,
    )


class AtprotoList(UDFBase[AtprotoListArguments, AtprotoListEffect]):
    def execute(self, execution_context: ExecutionContext, arguments: AtprotoListArguments) -> AtprotoListEffect:
        return synthesize_effect(arguments)
