from dataclasses import dataclass
from typing import List, Optional, Self, cast

from ddtrace.internal.logger import get_logger
from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.language_types.effects import EffectToCustomExtractedFeatureBase
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.engine.utils.types import add_slots

logger = get_logger('atproto_labels')


class AtprotoLabelArguments(ArgumentsBase):
    entity: str
    cid: Optional[str] = ''
    label: str
    comment: str
    expiration_in_hours: Optional[int]


@dataclass
class AtprotoLabelEffect(EffectToCustomExtractedFeatureBase[List[str]]):
    """Stores a label effect of a WhenRules(...) invocation, which stores the label mutations that should occur once
    a given action has finished classification."""

    entity: str
    """The entity that the effect will be applied on."""

    cid: Optional[str]
    """The cid of the entity that is being labeled, if it is a record"""

    label: str
    """The label that will be applied to the entity."""

    comment: str
    """The comment to add to the label event."""

    expiration_in_hours: Optional[int] = None
    """If set to true, the effect should not be applied."""

    def to_str(self) -> str:
        return f'{self.entity}|{self.label}|{self.comment}|{self.expiration_in_hours}'

    @classmethod
    def build_custom_extracted_feature_from_list(cls, values: List[Self]) -> CustomExtractedFeature[List[str]]:
        return AtprotoLabelEffectsExtractedFeature(effects=cast(List[AtprotoLabelEffect], values))


@add_slots
@dataclass
class AtprotoLabelEffectsExtractedFeature(CustomExtractedFeature[List[str]]):
    effects: List[AtprotoLabelEffect]

    @classmethod
    def feature_name(cls) -> str:
        return 'atproto_label'

    def get_serializable_feature(self) -> List[str] | None:
        return [effect.to_str() for effect in self.effects]


def synthesize_effect(arguments: AtprotoLabelArguments) -> AtprotoLabelEffect:
    return AtprotoLabelEffect(
        entity=arguments.entity,
        cid=arguments.cid,
        label=arguments.label,
        comment=arguments.comment,
        expiration_in_hours=arguments.expiration_in_hours,
    )


class AtprotoLabel(UDFBase[AtprotoLabelArguments, AtprotoLabelEffect]):
    def execute(self, execution_context: ExecutionContext, arguments: AtprotoLabelArguments) -> AtprotoLabelEffect:
        return synthesize_effect(arguments)
