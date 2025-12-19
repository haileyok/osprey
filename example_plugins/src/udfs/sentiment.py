from dataclasses import dataclass
from time import time
from typing import List, Optional

import requests
from osprey.engine.ast_validator.validation_context import ValidationContext
from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.singletons import CONFIG

logger = get_logger('sentiment_udf')

# Create a session with a larger connection pool for async requests
_sentiment_session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=1)
_sentiment_session.mount('http://', adapter)
_sentiment_session.mount('https://', adapter)


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

        try:
            config = CONFIG.instance()
            self.analyze_endpoint = config.get_optional_str('OSPREY_SENTIMENT_ENDPOINT')
        except Exception:
            pass

    def execute(self, execution_context: ExecutionContext, arguments: AnalyzeSentimentArguments) -> None:
        start_time = time()
        logger.info(f'==> Sentiment analysis STARTED')

        if not self.analyze_endpoint:
            logger.info('==> No analyze_endpoint, skipping')
            return

        if not arguments.text:
            logger.info('==> No text, skipping')
            return

        for v in arguments.when_all:
            if not v:
                logger.info('==> when_all failed, skipping')
                return

        logger.info(f'==> Making HTTP POST to {self.analyze_endpoint}')
        http_start = time()

        try:
            response = _sentiment_session.post(
                self.analyze_endpoint,
                json={'text': arguments.text},
                timeout=5.0
            )
            logger.info(f'==> Got HTTP response, status={response.status_code}')
            response.raise_for_status()
        except Exception as e:
            logger.error(f'==> HTTP request failed: {e}')
            raise

        http_duration = (time() - http_start) * 1000
        logger.info(f'==> HTTP completed in {http_duration:.1f}ms')

        json_start = time()
        json = response.json()
        json_duration = (time() - json_start) * 1000
        logger.info(f'==> JSON parsing took {json_duration:.1f}ms')

        if not isinstance(json, dict):
            logger.warning(f'==> Response not dict: {type(json)}')
            return

        features_start = time()
        execution_context.add_custom_extracted_features(
            custom_extracted_features=[
                VeryNegativeSentimentScoreCAF(score=json['very_negative']),
                NegativeSentimentScoreCAF(score=json['negative']),
                NeutralSentimentScoreCAF(score=json['neutral']),
                PositiveSentimentScoreCAF(score=json['positive']),
                VeryPositiveSentimentScoreCAF(score=json['very_positive']),
            ]
        )
        features_duration = (time() - features_start) * 1000
        logger.info(f'==> add_custom_extracted_features took {features_duration:.1f}ms')

        total_duration = (time() - start_time) * 1000
        logger.info(f'==> Sentiment COMPLETED: http={http_duration:.1f}ms json={json_duration:.1f}ms features={features_duration:.1f}ms total={total_duration:.1f}ms')


class VeryNegativeSentimentScore(UDFBase[ArgumentsBase, Optional[float]]):
    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> Optional[float]:
        score = execution_context.get_extracted_features().get('__very_negative_sentiment_score')
        if not isinstance(score, float):
            return None
        return score


class NegativeSentimentScore(UDFBase[ArgumentsBase, Optional[float]]):
    def execute(self, execution_context: ExecutionContext, arguments: ArgumentsBase) -> Optional[float]:
        score = execution_context.get_extracted_features().get('__negative_sentiment_score')
        if not isinstance(score, float):
            return None
        return score
