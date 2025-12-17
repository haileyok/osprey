from typing import Any, Dict, List, Optional

from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.stdlib.udfs.categories import UdfCategories
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger
from udfs.atproto.lib.uri import AtUri

logger = get_logger('facets')


class DidFromUriArguments(ArgumentsBase):
    uri: Optional[str]


class DidFromUri(UDFBase[DidFromUriArguments, Optional[str]]):
    category = UdfCategories.STRING

    def execute(self, execution_context: ExecutionContext, arguments: DidFromUriArguments) -> Optional[str]:
        if arguments.uri is None:
            return None

        try:
            aturi = AtUri.from_str(arguments.uri)
            return aturi.did
        except Exception as e:
            logger.error(f'Failed to get DID from input aturi {arguments.uri}. {e}')
            raise e


class FacetsArguments(ArgumentsBase):
    pass


class LinksFromFacets(UDFBase[FacetsArguments, List[str]]):
    def execute(self, execution_context: ExecutionContext, arguments: FacetsArguments) -> List[str]:
        facets = get_facets_from_execution_context(execution_context)
        if not facets:
            return []

        links: List[str] = []

        for facet in facets:
            features = get_features_from_facet(facet)

            if not features:
                return links

            for feature in features:
                feature_type = get_feature_type(feature)

                if feature_type == 'app.bsky.richtext.facet#link' and 'uri' in feature:
                    links.append(feature['uri'].lower())

        return links


class MentionsFromFacets(UDFBase[FacetsArguments, List[str]]):
    def execute(self, execution_context: ExecutionContext, arguments: FacetsArguments) -> List[str]:
        facets = get_facets_from_execution_context(execution_context)
        if not facets:
            return []

        dids: List[str] = []

        for facet in facets:
            features = get_features_from_facet(facet)

            if not features:
                return dids

            for feature in features:
                feature_type = get_feature_type(feature)

                if feature_type == 'app.bsky.richtext.facet#mention' and 'did' in feature:
                    dids.append(feature['did'].lower())

        return dids


class TagsFromFacets(UDFBase[FacetsArguments, List[str]]):
    def execute(self, execution_context: ExecutionContext, arguments: FacetsArguments) -> List[str]:
        facets = get_facets_from_execution_context(execution_context)
        if not facets:
            return []

        tags: List[str] = []

        for facet in facets:
            features = get_features_from_facet(facet)

            if not features:
                return tags

            for feature in features:
                feature_type = get_feature_type(feature)

                if feature_type == 'app.bsky.richtext.facet#tag' and 'tag' in feature:
                    tags.append(feature['tag'].lower())

        return tags


def get_facets_from_execution_context(execution_context: ExecutionContext) -> Optional[List[Dict[str, Any]]]:
    data = execution_context.get_data()

    if (
        'operation' in data
        and 'record' in data['operation']
        and 'facets' in data['operation']['record']
        and isinstance(data['operation']['record']['facets'], list)
    ):
        return data['operation']['record']['facets']

    return None


def get_features_from_facet(facet: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    if 'features' not in facet:
        logger.warning(f'attempted to extract features from facet without features: {facet}')
        return None

    return facet['features']


def get_feature_type(feature: Dict[str, Any]) -> Optional[str]:
    if '$type' not in feature:
        logger.warning(f'attempted to extract type from feature without type: {feature}')
        return None

    return feature['$type']
