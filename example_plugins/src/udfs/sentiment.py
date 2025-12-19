from dataclasses import dataclass
from typing import List, Optional

import requests
from osprey.engine.ast_validator.validation_context import ValidationContext
from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.singletons import CONFIG


class AnalyzeSentimentArguments(ArgumentsBase):
    text: str
    """The text to perform sentiment analysis on"""

    when_all: List[bool]
    """Sentiment will only be analyzed when all of the boolean values in this list are True"""


@dataclass
class VeryNegativeSentimentScoreCAF(CustomExtractedFeature[float]):
    score: float

    @classmethod
    def feature_name(cls) -> str:
        return 'very_negative_sentiment_score'

    def get_serializable_feature(self) -> float | None:
        return self.score


@dataclass
class NegativeSentimentScoreCAF(CustomExtractedFeature[float]):
    score: float

    @classmethod
    def feature_name(cls) -> str:
        return 'negative_sentiment_score'

    def get_serializable_feature(self) -> float | None:
        return self.score


@dataclass
class NeutralSentimentScoreCAF(CustomExtractedFeature[float]):
    score: float

    @classmethod
    def feature_name(cls) -> str:
        return 'negative_sentiment_score'

    def get_serializable_feature(self) -> float | None:
        return self.score


@dataclass
class PositiveSentimentScoreCAF(CustomExtractedFeature[float]):
    score: float

    @classmethod
    def feature_name(cls) -> str:
        return 'positive_sentiment_score'

    def get_serializable_feature(self) -> float | None:
        return self.score


@dataclass
class VeryPositiveSentimentScoreCAF(CustomExtractedFeature[float]):
    score: float

    @classmethod
    def feature_name(cls) -> str:
        return 'very_positive_sentiment_score'

    def get_serializable_feature(self) -> float | None:
        return self.score


class AnalyzeSentiment(UDFBase[AnalyzeSentimentArguments, None]):
    execute_async = True

    def __init__(self, validation_context: 'ValidationContext', arguments: AnalyzeSentimentArguments):
        super().__init__(validation_context, arguments)

        config = CONFIG.instance()

        self.analyze_endpoint = config.get_optional_str('OSPREY_SENTIMENT_ENDPOINT')

    def execute(self, execution_context: ExecutionContext, arguments: AnalyzeSentimentArguments) -> None:
        if not self.analyze_endpoint:
            return

        if not arguments.text:
            return

        for v in arguments.when_all:
            if not v:
                return

        response = requests.post(self.analyze_endpoint, json={'text': arguments.text})
        response.raise_for_status()

        json = response.json()

        if not isinstance(json, dict):
            return

        execution_context.add_custom_extracted_features(
            custom_extracted_features=[
                VeryNegativeSentimentScoreCAF(score=json['very_negative']),
                NegativeSentimentScoreCAF(score=json['negative']),
                NeutralSentimentScoreCAF(score=json['neutral']),
                PositiveSentimentScoreCAF(score=json['positive']),
                VeryPositiveSentimentScoreCAF(score=json['very_positive']),
            ]
        )


class VeryNegativeSentimentScore(UDFBase[ArgumentsBase, Optional[float]]):
    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> Optional[float]:
        score = execution_context.get_extracted_features().get('very_negative_sentiment_score')
        if not isinstance(score, float):
            return None
        return score


class NegativeSentimentScore(UDFBase[ArgumentsBase, Optional[float]]):
    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> Optional[float]:
        score = execution_context.get_extracted_features().get('negative_sentiment_score')
        if not isinstance(score, float):
            return None
        return score
