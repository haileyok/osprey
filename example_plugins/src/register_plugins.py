from typing import Any, Sequence, Type

from input_streams.kafka_input_stream import KafkaInputStream
from osprey.engine.executor.execution_context import Action
from osprey.engine.udf.base import UDFBase
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.lib.singletons import CONFIG
from osprey.worker.lib.storage.labels import LabelsServiceBase
from osprey.worker.sinks.sink.input_stream import BaseInputStream
from osprey.worker.sinks.sink.output_sink import BaseOutputSink
from osprey.worker.sinks.utils.acking_contexts import BaseAckingContext
from output_sinks.clickhouse_execution_results_sink import ClickhouseExecutionResultsSink
from output_sinks.ozone_label_sink import OzoneLabelSink
from services.ozone_labels_service import OzoneLabelsService
from shared.metrics import prom_metrics
from udfs.atproto.diduri import DidFromUri
from udfs.atproto.facets import LinksFromFacets, MentionsFromFacets, TagsFromFacets
from udfs.atproto.label import AtprotoLabel
from udfs.atproto.list import AtprotoList
from udfs.cache import (
    CacheGetFloat,
    CacheGetInt,
    CacheGetStr,
    CacheSetFloat,
    CacheSetInt,
    CacheSetStr,
    GetWindowCount,
    IncrementWindow,
    cache_client,
)
from udfs.censorize import CheckCensorized, CleanString
from udfs.domain import RootDomain
from udfs.list import (
    CensorizedListMatch,
    ConcatStringLists,
    ListContains,
    ListContainsCount,
    RegexListMatch,
    SimpleListContains,
)
from udfs.query_udfs.regex import Regex
from udfs.sentiment import AnalyzeSentiment
from udfs.string import ExtractDomains, ExtractEmoji, ExtractListDomains, ForceString, StringContains, SubstrCount
from udfs.tokenize import Tokenize
from udfs.toxicity import AnalyzeToxicity


@hookimpl_osprey
def register_udfs() -> Sequence[Type[UDFBase[Any, Any]]]:
    return [
        StringContains,
        Tokenize,
        CleanString,
        ExtractDomains,
        Tokenize,
        CleanString,
        ExtractDomains,
        ExtractEmoji,
        ListContains,
        ListContainsCount,
        SimpleListContains,
        RegexListMatch,
        CensorizedListMatch,
        ForceString,
        CacheGetStr,
        CacheGetInt,
        CacheGetFloat,
        CacheSetStr,
        CacheSetInt,
        CacheSetFloat,
        IncrementWindow,
        GetWindowCount,
        CheckCensorized,
        RootDomain,
        SubstrCount,
        ExtractListDomains,
        MentionsFromFacets,
        LinksFromFacets,
        TagsFromFacets,
        DidFromUri,
        ConcatStringLists,
        AtprotoLabel,
        AtprotoList,
        AnalyzeSentiment,
        AnalyzeToxicity,
        # Query UDFs
        Regex,
    ]


@hookimpl_osprey
def register_input_stream() -> BaseInputStream[BaseAckingContext[Action]]:
    config = CONFIG.instance()
    prom_metrics.start_http(
        port=config.get_int('OSPREY_PROM_METRICS_PORT', 9090),
        addr=config.get_str('OSPREY_PROM_METRICS_ADDR', '127.0.0.1'),
    )
    return KafkaInputStream(config=config)


@hookimpl_osprey
def register_output_sinks(config: Config) -> Sequence[BaseOutputSink]:
    cache_client.initialize(config)

    return [ClickhouseExecutionResultsSink(config=config), OzoneLabelSink(config=config)]


@hookimpl_osprey
def register_labels_service_or_provider(config: Config) -> LabelsServiceBase:
    """Register a PostgreSQL-backed labels service."""
    return OzoneLabelsService(config=config)
