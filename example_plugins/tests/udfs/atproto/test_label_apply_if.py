from unittest.mock import MagicMock

from osprey.engine.language_types.rules import RuleT

from example_plugins.src.udfs.atproto.label import (
    AtprotoLabelArguments,
    synthesize_effect,
)


class TestAtprotoLabelApplyIfAC1:
    """Tests for atproto-apply-if.AC1: AtprotoLabel accepts apply_if parameter"""

    def test_ac1_1_apply_if_passing_rule(self) -> None:
        """atproto-apply-if.AC1.1: apply_if with passing rule produces dependent_rule.value=True"""
        passing_rule = RuleT(
            name='gate',
            value=True,
            description='',
            features={},
        )

        arguments = MagicMock(spec=AtprotoLabelArguments)
        arguments.entity = 'test_entity'
        arguments.cid = 'test_cid'
        arguments.label = 'test_label'
        arguments.comment = 'test_comment'
        arguments.expiration_in_hours = None
        arguments.apply_if = passing_rule
        arguments.has_argument_ast.return_value = False

        effect = synthesize_effect(arguments)

        assert effect.dependent_rule is passing_rule
        assert effect.dependent_rule.value is True
        assert effect.suppressed is False

    def test_ac1_2_apply_if_failing_rule(self) -> None:
        """atproto-apply-if.AC1.2: apply_if with failing rule produces dependent_rule.value=False"""
        failing_rule = RuleT(
            name='gate',
            value=False,
            description='',
            features={},
        )

        arguments = MagicMock(spec=AtprotoLabelArguments)
        arguments.entity = 'test_entity'
        arguments.cid = 'test_cid'
        arguments.label = 'test_label'
        arguments.comment = 'test_comment'
        arguments.expiration_in_hours = None
        arguments.apply_if = failing_rule
        arguments.has_argument_ast.return_value = False

        effect = synthesize_effect(arguments)

        assert effect.dependent_rule is failing_rule
        assert effect.dependent_rule.value is False
        assert effect.suppressed is False

    def test_ac1_3_no_apply_if(self) -> None:
        """atproto-apply-if.AC1.3: no apply_if produces dependent_rule=None and suppressed=False"""
        arguments = MagicMock(spec=AtprotoLabelArguments)
        arguments.entity = 'test_entity'
        arguments.cid = 'test_cid'
        arguments.label = 'test_label'
        arguments.comment = 'test_comment'
        arguments.expiration_in_hours = None
        arguments.apply_if = None
        arguments.has_argument_ast.return_value = False

        effect = synthesize_effect(arguments)

        assert effect.dependent_rule is None
        assert effect.suppressed is False


class TestAtprotoLabelApplyIfAC3:
    """Tests for atproto-apply-if.AC3: Fail-closed suppression (AtprotoLabel portion)"""

    def test_ac3_1_apply_if_rule_error(self) -> None:
        """atproto-apply-if.AC3.1: apply_if where rule errors produces suppressed=True"""
        arguments = MagicMock(spec=AtprotoLabelArguments)
        arguments.entity = 'test_entity'
        arguments.cid = 'test_cid'
        arguments.label = 'test_label'
        arguments.comment = 'test_comment'
        arguments.expiration_in_hours = None
        arguments.apply_if = None
        arguments.has_argument_ast.return_value = True

        effect = synthesize_effect(arguments)

        assert effect.suppressed is True
