from dataclasses import dataclass
from typing import List, Optional

import requests
from osprey.engine.ast_validator.validation_context import ValidationContext
from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.singletons import CONFIG


class ToxicityScoreArguments(ArgumentsBase):
    text: str
    """The text to perform toxicity analysis on"""

    when_all: List[bool]
    """Toxicity will only be analyzed when all of the boolean values in this list are True"""


@dataclass
class ToxicityToxicScoreCAF(CustomExtractedFeature[float]):
    score: float

    @classmethod
    def feature_name(cls) -> str:
        return 'toxicity_toxic_score'

    def get_serializable_feature(self) -> float | None:
        return self.score


@dataclass
class ToxicityNeutralScoreCAF(CustomExtractedFeature[float]):
    score: float

    @classmethod
    def feature_name(cls) -> str:
        return 'toxicity_neutral_score'

    def get_serializable_feature(self) -> float | None:
        return self.score


class AnalyzeToxicity(UDFBase[ToxicityScoreArguments, Optional[float]]):
    execute_async = True

    def __init__(self, validation_context: 'ValidationContext', arguments: ToxicityScoreArguments):
        super().__init__(validation_context, arguments)

        try:
            config = CONFIG.instance()
            self.analyze_endpoint = config.get_optional_str('OSPREY_TOXICITY_ENDPOINT')
        except Exception:
            pass

    def execute(self, execution_context: ExecutionContext, arguments: ToxicityScoreArguments) -> Optional[float]:
        if not self.analyze_endpoint:
            return None

        if not arguments.text:
            return None

        for v in arguments.when_all:
            if not v:
                return None

        response = requests.post(self.analyze_endpoint, json={'text': arguments.text})
        response.raise_for_status()

        json = response.json()

        if not isinstance(json, dict):
            return None

        # put all the individual scores in the extracted features for looking at in i.e. clickhouse
        execution_context.add_custom_extracted_features(
            custom_extracted_features=[
                ToxicityToxicScoreCAF(score=json['toxic']),
                ToxicityNeutralScoreCAF(score=json['neutral']),
            ]
        )

        # calculate a single polarity score from -1 to +1
        polarity = json['neutral'] * 1.0 + json['toxic'] * -1.0

        return polarity
