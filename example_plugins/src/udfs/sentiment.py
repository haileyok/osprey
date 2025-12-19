from dataclasses import dataclass
from typing import List, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import json as json_lib

from osprey.engine.ast_validator.validation_context import ValidationContext
from osprey.engine.executor.custom_extracted_features import CustomExtractedFeature
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.singletons import CONFIG

logger = get_logger('sentiment_udf', dynamic_log_sampler=None)

# Shared thread pool for HTTP requests (urllib doesn't yield properly in gevent)
import gevent.threadpool
_http_threadpool = gevent.threadpool.ThreadPool(maxsize=100)


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
        return 'neutral_sentiment_score'  # Fixed: was 'negative_sentiment_score'

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
        # DEBUG: Check if gevent is actually patched
        import socket
        logger.info(f'socket module: {socket.socket.__module__}')

        if not self.analyze_endpoint:
            return

        if not arguments.text:
            return

        for v in arguments.when_all:
            if not v:
                return

        # urllib doesn't yield properly with gevent, so run in threadpool
        from urllib.request import Request, urlopen
        from urllib.error import HTTPError
        import json as json_lib

        def _do_http_request():
            data = json_lib.dumps({'text': arguments.text}).encode('utf-8')
            req = Request(self.analyze_endpoint, data=data, headers={'Content-Type': 'application/json'})
            with urlopen(req, timeout=5.0) as response:
                return json_lib.loads(response.read().decode('utf-8'))

        try:
            # Run in thread pool so it doesn't block greenlets
            json = _http_threadpool.spawn(_do_http_request).get()
        except HTTPError as e:
            logger.error(f'HTTP request failed with status {e.code}')
            raise
        except Exception as e:
            logger.error(f'HTTP request failed: {e}')
            raise

        if not isinstance(json, dict):
            logger.warning(f'Response not dict: {type(json)}')
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
