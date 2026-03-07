from unittest.mock import MagicMock

from osprey.engine.language_types.rules import RuleT

from example_plugins.src.udfs.atproto.list import (
    AtprotoListArguments,
    synthesize_effect,
)


class TestAtprotoListApplyIfAC2:
    """Tests for atproto-apply-if.AC2: AtprotoList accepts apply_if parameter"""

    def test_ac2_1_apply_if_passing_rule(self) -> None:
        """atproto-apply-if.AC2.1: apply_if with passing rule produces dependent_rule.value=True"""
        passing_rule = RuleT(
            name='gate',
            value=True,
            description='',
            features={},
        )

        arguments = MagicMock(spec=AtprotoListArguments)
        arguments.did = 'test_did'
        arguments.list_uri = 'test_uri'
        arguments.apply_if = passing_rule
        arguments.has_argument_ast.return_value = False

        effect = synthesize_effect(arguments)

        assert effect.dependent_rule is passing_rule
        assert effect.dependent_rule.value is True
        assert effect.suppressed is False

    def test_ac2_2_apply_if_failing_rule(self) -> None:
        """atproto-apply-if.AC2.2: apply_if with failing rule produces dependent_rule.value=False"""
        failing_rule = RuleT(
            name='gate',
            value=False,
            description='',
            features={},
        )

        arguments = MagicMock(spec=AtprotoListArguments)
        arguments.did = 'test_did'
        arguments.list_uri = 'test_uri'
        arguments.apply_if = failing_rule
        arguments.has_argument_ast.return_value = False

        effect = synthesize_effect(arguments)

        assert effect.dependent_rule is failing_rule
        assert effect.dependent_rule.value is False
        assert effect.suppressed is False

    def test_ac2_3_no_apply_if(self) -> None:
        """atproto-apply-if.AC2.3: no apply_if produces dependent_rule=None and suppressed=False"""
        arguments = MagicMock(spec=AtprotoListArguments)
        arguments.did = 'test_did'
        arguments.list_uri = 'test_uri'
        arguments.apply_if = None
        arguments.has_argument_ast.return_value = False

        effect = synthesize_effect(arguments)

        assert effect.dependent_rule is None
        assert effect.suppressed is False


class TestAtprotoListApplyIfAC3:
    """Tests for atproto-apply-if.AC3: Fail-closed suppression (AtprotoList portion)"""

    def test_ac3_2_apply_if_rule_error(self) -> None:
        """atproto-apply-if.AC3.2: apply_if where rule errors produces suppressed=True"""
        arguments = MagicMock(spec=AtprotoListArguments)
        arguments.did = 'test_did'
        arguments.list_uri = 'test_uri'
        arguments.apply_if = None
        arguments.has_argument_ast.return_value = True

        effect = synthesize_effect(arguments)

        assert effect.suppressed is True
