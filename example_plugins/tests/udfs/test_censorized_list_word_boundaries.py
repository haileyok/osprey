from example_plugins.src.udfs.censorize import CensorCache, create_censorize_regex


class TestCreateCensorizeRegex:
    """Validates that create_censorize_regex boundary markers work correctly."""

    def test_rejects_substring_when_boundaries_enforced(self) -> None:
        pattern = create_censorize_regex('retard', include_plural=False, include_substrings=False)
        assert pattern.search('firetarded people') is None

    def test_accepts_standalone_when_boundaries_enforced(self) -> None:
        pattern = create_censorize_regex('retard', include_plural=False, include_substrings=False)
        assert pattern.search('you are a retard') is not None

    def test_accepts_start_of_string(self) -> None:
        pattern = create_censorize_regex('retard', include_plural=False, include_substrings=False)
        assert pattern.search('retard detected') is not None

    def test_accepts_end_of_string(self) -> None:
        pattern = create_censorize_regex('retard', include_plural=False, include_substrings=False)
        assert pattern.search('total retard') is not None

    def test_substring_mode_allows_embedded_match(self) -> None:
        pattern = create_censorize_regex('retard', include_plural=False, include_substrings=True)
        assert pattern.search('firetarded people') is not None

    def test_plurals_respect_word_boundaries(self) -> None:
        pattern = create_censorize_regex('retard', include_plural=True, include_substrings=False)
        assert pattern.search('retards') is not None
        assert pattern.search('firetards') is None

    def test_censorized_chars_respect_word_boundaries(self) -> None:
        pattern = create_censorize_regex('retard', include_plural=False, include_substrings=False)
        assert pattern.search('you are a r3tard') is not None


class TestCensorCacheSubstringsParam:
    """CensorCache.get_censorized_regex is the intermediary between
    CensorizedListContains and create_censorize_regex. Its `substrings`
    parameter must correctly map to `include_substrings`."""

    def test_substrings_false_enforces_word_boundaries(self) -> None:
        cache = CensorCache()
        pattern = cache.get_censorized_regex('retard', plurals=False, substrings=False)
        assert pattern.search('you are a retard') is not None
        assert pattern.search('firetarded people') is None

    def test_substrings_true_allows_substring_matches(self) -> None:
        cache = CensorCache()
        pattern = cache.get_censorized_regex('retard', plurals=False, substrings=True)
        assert pattern.search('firetarded people') is not None

    def test_word_boundaries_default_should_map_to_substrings_false(self) -> None:
        """Simulates the CensorizedListContains call site. word_boundaries
        defaults to True, meaning 'enforce boundaries'. This must map to
        substrings=False. Before the fix, it was passed as substrings=True."""
        word_boundaries = True

        cache = CensorCache()
        # After fix: substrings=not word_boundaries (i.e. False)
        pattern = cache.get_censorized_regex(
            'retard', plurals=False, substrings=not word_boundaries
        )
        assert pattern.search('you are a retard') is not None
        assert pattern.search('firetarded people') is None
