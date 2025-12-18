from typing import Any, Sequence, Type

from osprey.engine.udf.base import UDFBase
from osprey.worker.adaptor.plugin_manager import hookimpl_osprey
from osprey.worker.lib.config import Config
from osprey.worker.lib.storage.labels import LabelsServiceBase
from osprey.worker.sinks.sink.output_sink import BaseOutputSink
from output_sinks.clickhouse_execution_results_sink import ClickhouseExecutionResultsSink
from output_sinks.ozone_label_sink import OzoneLabelSink
from services.ozone_labels_service import OzoneLabelsService
from udfs.atproto.diduri import DidFromUri
from udfs.atproto.facets import LinksFromFacets, MentionsFromFacets, TagsFromFacets
from udfs.atproto.label import AtprotoLabel
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
from udfs.string import ExtractDomains, ExtractEmoji, ExtractListDomains, ForceString, StringContains, SubstrCount
from udfs.tokenize import Tokenize


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
        # Query UDFs
        Regex,
    ]


@hookimpl_osprey
def register_output_sinks(config: Config) -> Sequence[BaseOutputSink]:
    cache_client.initialize(config)

    return [ClickhouseExecutionResultsSink(config=config), OzoneLabelSink(config=config)]


@hookimpl_osprey
def register_labels_service_or_provider(config: Config) -> LabelsServiceBase:
    """Register a PostgreSQL-backed labels service."""
    return OzoneLabelsService(config=config)
