from dataclasses import dataclass
from typing import List, Optional, Self, cast

from ddtrace.internal.logger import get_logger
from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.effects import EffectToCustomExtractedFeatureBase
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.utils.types import add_slots

logger = get_logger('atproto_tags')


class AtprotoTagArguments(ArgumentsBase):
    entity: str
    tag: str
    comment: str
    neg: Optional[bool] = False


@dataclass
class AtprotoTagEffect(EffectToCustomExtractedFeatureBase[List[str]]):
    """Stores a tag effect of a WhenRules(...) invocation, which stores the tag mutations that should occur once
    a given action has finished classification."""

    entity: str
    """The entity that the effect will be applied on."""

    tag: str
    """The tag that will be applied to or removed from the entity."""

    comment: str
    """The comment to add to the tag event."""

    neg: bool = False
    """If True, the tag will be removed instead of added."""

    def to_str(self) -> str:
        prefix = '-' if self.neg else '+'
        return f'{self.entity}|{prefix}{self.tag}|{self.comment}'

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[List[str]]:
        return AtprotoTagEffectsExtractedFeature(effects=cast(List[AtprotoTagEffect], values))


@add_slots
@dataclass
class AtprotoTagEffectsExtractedFeature(CustomExtractedFeature[List[str]]):
    effects: List[AtprotoTagEffect]

    @classmethod
    def feature_name(cls) -> str:
        return 'atproto_tag'

    def get_serializable_feature(self) -> List[str] | None:
        return [effect.to_str() for effect in self.effects]


def synthesize_effect(arguments: AtprotoTagArguments) -> AtprotoTagEffect:
    return AtprotoTagEffect(
        entity=arguments.entity,
        tag=arguments.tag,
        comment=arguments.comment,
        neg=arguments.neg or False,
    )


class AtprotoTag(UDFBase[AtprotoTagArguments, AtprotoTagEffect]):
    def execute(self, execution_context: ExecutionContext, arguments: AtprotoTagArguments) -> AtprotoTagEffect:
        return synthesize_effect(arguments)
