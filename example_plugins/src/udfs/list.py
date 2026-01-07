import os
import re
from typing import Dict, List, Optional, Set, cast

import yaml
from osprey.engine.ast.sources import Sources
from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase
from osprey.worker.lib.osprey_shared.logging import get_logger
from udfs.censorize import censor_cache

logger = get_logger('list')

RULES_PATH = os.getenv('OSPREY_RULES_PATH', 'example_rules/')


class ListCache:
    def __init__(self) -> None:
        # a cache of lists for checking against, so we avoid having to read the YAML files all the time
        self._cache: Dict[str, Set[str]] = {}
        # same as _cache, but for lists of regex patterns rather than strings
        self._regex_cache: Dict[str, List[re.Pattern[str]]] = {}
        # an optional Sources class that allows for specifying i.e. etcd sources instead of the base rules directory
        # currently unused...may be implemented if upstreaming these changes
        self._sources: Optional[Sources] = None

    def set_sources(self, sources: Sources) -> None:
        """
        Set the sources object and clear cache to reload from new sources.

        This should be called any time that the ruleset gets re-loaded, i.e. whenever new changes are
        deployed via etcd.
        """
        # update the sources then clear each list
        self._sources = sources
        self._cache.clear()
        self._regex_cache.clear()
        logger.info('ListCache sources updated, cache cleared')

    def _load_list_data(self, list_name: str):
        """
        Load list data from sources or fall back to disk.

        If no source is defined, we will only load the lists that are present at runtime
        """
        if self._sources:
            try:
                list_path = f'lists/{list_name}.yaml'
                # check if the list exists in sources
                matching_sources = [s for s in self._sources if s.path.endswith(list_path)]
                if matching_sources:
                    source = matching_sources[0]
                    data = yaml.safe_load(source.contents)
                    logger.info(f'Loaded {list_name} from etcd sources')
                    return data
            except Exception as e:
                logger.warning(f'Failed to load {list_name} from etcd sources: {e}, falling back to disk')

        try:
            file_path = f'{RULES_PATH}lists/{list_name}.yaml'
            with open(file_path, 'r') as f:
                data = yaml.safe_load(f)
            logger.info(f'Loaded {list_name} from disk')
            return data
        except Exception as e:
            logger.error(f'Failed to load {list_name} from disk: {e}')
            return None

    def get_list(self, list_name: str, case_sensitive: bool) -> Set[str]:
        """
        Get a list from the cache, or load it in if not already present in the cache.
        """
        try:
            cache_key = list_name
            if case_sensitive:
                cache_key = list_name + '-case-sensitive'

            if cache_key not in self._cache:
                data = self._load_list_data(list_name)
                if data is None:
                    return set()

                processed_set: Set[str] = set()

                if isinstance(data, list):
                    items = cast(List[str], data)
                    for item in items:
                        if not case_sensitive:
                            processed_set.add(item.lower())
                        else:
                            processed_set.add(item)

                self._cache[cache_key] = processed_set

                if len(processed_set) > 20:
                    logger.info(f'Cached {cache_key}. List length exceeded twenty items.')
                else:
                    logger.info(f'Cached {cache_key}. Values are {list(processed_set)}')

            return self._cache[cache_key]
        except Exception as e:
            logger.error(f'Error loading list cache for {list_name}: {e}')
            return set()

    def get_simple_list(self, cache_name: str, list: List[str], case_sensitive: bool) -> Set[str]:
        cache_name = f'{cache_name}-simple'
        if case_sensitive:
            cache_name = f'{cache_name}-case-sensitive'

        if cache_name not in self._cache:
            processed_set: Set[str] = set()

            for item in list:
                if not case_sensitive:
                    processed_set.add(item.lower())
                else:
                    processed_set.add(item)

            self._cache[cache_name] = processed_set

            logger.info(f'Created cache list {cache_name}')

        return self._cache[cache_name]

    def get_regex_list(self, list_name: str, case_sensitive: bool) -> List[re.Pattern[str]]:
        cache_key = list_name if case_sensitive else list_name + '-case-insensitive'

        if cache_key not in self._regex_cache:
            data = self._load_list_data(list_name)
            if data is None:
                return []

            compiled_patterns: List[re.Pattern[str]] = []
            if isinstance(data, list):
                items = cast(List[str], data)
                for item in items:
                    try:
                        item = item.strip()
                        flags = 0 if case_sensitive else re.IGNORECASE
                        pattern = re.compile(item, flags)
                        compiled_patterns.append(pattern)
                    except re.error as e:
                        logger.warning(f'Invalid regex pattern "{item}": {e}')

            self._regex_cache[cache_key] = compiled_patterns
            logger.info(
                f'Loaded and compiled {len(compiled_patterns)} regex patterns from {list_name}. {compiled_patterns}'
            )

        return self._regex_cache[cache_key]

    def get_censorized_regex_list(self, list_name: str, plurals: bool, substrings: bool) -> List[re.Pattern[str]]:
        cache_key = f'{list_name}-cen'
        if plurals:
            cache_key = f'{cache_key}-yp'
        if substrings:
            cache_key = f'{cache_key}-ysbs'

        if cache_key not in self._regex_cache:
            data = self._load_list_data(list_name)
            if data is None:
                return []

            compiled_patterns: List[re.Pattern[str]] = []
            if isinstance(data, list):
                items = cast(List[str], data)
                for item in items:
                    pattern = censor_cache.get_censorized_regex(item, plurals=plurals, substrings=substrings)
                    compiled_patterns.append(pattern)

            self._regex_cache[cache_key] = compiled_patterns
            if len(compiled_patterns) > 20:
                logger.info(
                    f'Loaded and censorized {len(compiled_patterns)} regex patterns from {list_name}. Length exceeded twenty.'
                )
            else:
                logger.info(
                    f'Loaded and censorized {len(compiled_patterns)} regex patterns from {list_name}. {compiled_patterns}'
                )

        return self._regex_cache[cache_key]


list_cache = ListCache()


class ListContainsArgumentsBase(ArgumentsBase):
    phrases: List[Optional[str]]
    """List of strings to check for in the list"""

    case_sensitive = False
    """Whether the phrases should be checked with exact casing or not"""

    word_boundaries = True
    """Whether to use word boundaries or not when checking phrases"""


class ListContainsArguments(ListContainsArgumentsBase):
    list: str
    """Name of the list, which is the name of the YAML file minus the extension. I.e. toxic_words.yaml would be named toxic_words"""


class SimpleListContainsArguments(ListContainsArgumentsBase):
    cache_name: str
    """A cache name for the simple list that you are creating. Should be unique to other simple lists created in the ruleset."""

    list: List[str]
    """A list of strings that are included in the simple list"""


class ListContains(UDFBase[ListContainsArguments, Optional[str]]):
    """
    Checks if a list (YAML-based) contains any of the given input strings.
    """

    def execute(self, execution_context: ExecutionContext, arguments: ListContainsArguments) -> Optional[str]:
        list_items = list_cache.get_list(arguments.list, case_sensitive=arguments.case_sensitive)

        for phrase in arguments.phrases:
            if phrase is None:
                continue

            phrase = phrase if arguments.case_sensitive else phrase.lower()

            if arguments.word_boundaries:
                for word in list_items:
                    escaped_word = re.escape(word)
                    flags = 0 if arguments.case_sensitive else re.IGNORECASE
                    if re.search(r'\b' + escaped_word + r'\b', phrase, flags):
                        return word
            else:
                for word in list_items:
                    if word in phrase:
                        return word

        return None


class ListContainsCount(UDFBase[ListContainsArguments, int]):
    """
    The number of "hits" found in the list for the given input string
    """

    def execute(self, execution_context: ExecutionContext, arguments: ListContainsArguments) -> int:
        list_items = list_cache.get_list(arguments.list, case_sensitive=arguments.case_sensitive)

        count = 0

        for phrase in arguments.phrases:
            if phrase is None:
                continue

            phrase = phrase if arguments.case_sensitive else phrase.lower()

            if arguments.word_boundaries:
                for word in list_items:
                    escaped_word = re.escape(word)
                    flags = 0 if arguments.case_sensitive else re.IGNORECASE
                    if re.search(r'\b' + escaped_word + r'\b', phrase, flags):
                        count += 1
            else:
                for word in list_items:
                    if word in phrase:
                        count += 1

        return count


class SimpleListContains(UDFBase[SimpleListContainsArguments, Optional[str]]):
    """
    Similar to ListContains, but allows you to supply the list of strings in an SML rule instead of in a YAML file.
    """

    def execute(self, execution_context: ExecutionContext, arguments: SimpleListContainsArguments) -> Optional[str]:
        list_items = list_cache.get_simple_list(arguments.cache_name, arguments.list, arguments.case_sensitive)

        for phrase in arguments.phrases:
            if phrase is None:
                continue

            phrase = phrase if arguments.case_sensitive else phrase
            if arguments.word_boundaries:
                for word in list_items:
                    escaped_word = re.escape(word)
                    flags = 0 if arguments.case_sensitive else re.IGNORECASE
                    if re.search(r'\b' + escaped_word + r'\b', phrase, flags):
                        return word
            else:
                for word in list_items:
                    if word in phrase:
                        return word

        return None


class RegexListContainsArguments(ArgumentsBase):
    list: str
    phrases: List[str]
    case_sensitive = False


class RegexListContains(UDFBase[RegexListContainsArguments, Optional[str]]):
    """
    Similar to ListContains, but the input YAML-based list should contain regex patterns rather than simple strings.
    The input string is checked against each compiled regex.
    """

    def execute(self, execution_context: ExecutionContext, arguments: RegexListContainsArguments) -> Optional[str]:
        patterns = list_cache.get_regex_list(arguments.list, case_sensitive=arguments.case_sensitive)

        for phrase in arguments.phrases:
            for pattern in patterns:
                if pattern.search(phrase):
                    return pattern.pattern

        return None


class CensorizedListContainsArguments(ListContainsArguments):
    plurals: bool = False
    """Whether the censorized regex that are created should include plural matches or not"""

    must_be_censorized: bool = False
    """
    Whether matches must be a censored version of a string, i.e. "cat" would only match something like "<4t"
    """


class CensorizedListContains(UDFBase[CensorizedListContainsArguments, Optional[str]]):
    """
    Similar to ListContains, but each string in the YAML-based list will first be "censorized". This means that
    when given the word "cat" in the list, variations like c@t or <4t will also be matched.
    """

    def execute(self, execution_context: ExecutionContext, arguments: CensorizedListContainsArguments) -> Optional[str]:
        patterns = list_cache.get_censorized_regex_list(
            list_name=arguments.list,
            plurals=arguments.plurals,
            substrings=arguments.word_boundaries,
        )

        for phrase in arguments.phrases:
            if phrase is None:
                continue

            for pattern in patterns:
                match = pattern.search(phrase)
                if not match:
                    continue

                if arguments.must_be_censorized:
                    if match.group().lower() != phrase.lower():
                        return pattern.pattern
                else:
                    return pattern.pattern

        return None


class ConcatStringListsArguments(ArgumentsBase):
    lists: List[List[str]]
    """Lists of strings to combine into a single list"""

    optional_lists: List[List[Optional[str]]] = []
    """Lists of strings or None values to combine into a single list. None values will be excluded from the result."""


class ConcatStringLists(UDFBase[ConcatStringListsArguments, List[str]]):
    """
    Combines two lists of strings to create a single list, which can then be passed to one of the ListContains `phrase` parameters.
    """

    def execute(self, execution_context: ExecutionContext, arguments: ConcatStringListsArguments) -> List[str]:
        final: List[str] = []

        for list in arguments.lists:
            for item in list:
                final.append(item)

        for list in arguments.optional_lists:  # type: ignore[assignment]
            for item in list:
                if item is None:
                    continue
                final.append(item)

        return final
