from unittest.mock import MagicMock, patch

from osprey.engine.language_types.rules import RuleT
from udfs.atproto.label import AtprotoLabelEffect
from udfs.atproto.list import AtprotoListEffect

from example_plugins.src.output_sinks.ozone_label_sink import OzoneLabelSink


class TestOzoneLabelSinkApplyIfAC4:
    """Tests for atproto-apply-if.AC4: OzoneLabelSink gating"""

    def test_ac4_1_label_effect_with_passing_rule(self) -> None:
        """atproto-apply-if.AC4.1: Sink applies AtprotoLabelEffect when dependent_rule.value is True"""
        mock_client = MagicMock()
        with patch('example_plugins.src.output_sinks.ozone_label_sink.OzoneClient.get_instance', return_value=mock_client):
            sink = OzoneLabelSink(config=MagicMock())

            passing_rule = RuleT(
                name='gate',
                value=True,
                description='',
                features={},
            )

            effect = AtprotoLabelEffect(
                entity='test_entity',
                cid='test_cid',
                label='test_label',
                comment='test_comment',
                expiration_in_hours=None,
                dependent_rule=passing_rule,
                suppressed=False,
            )

            mock_result = MagicMock()
            mock_result.action.action_id = 123
            mock_result.effects = {AtprotoLabelEffect: [effect]}

            with patch.object(sink, '_apply_label') as mock_apply_label:
                sink.push(mock_result)
                mock_apply_label.assert_called_once()

    def test_ac4_2_list_effect_with_passing_rule(self) -> None:
        """atproto-apply-if.AC4.2: Sink applies AtprotoListEffect when dependent_rule.value is True"""
        mock_client = MagicMock()
        with patch('example_plugins.src.output_sinks.ozone_label_sink.OzoneClient.get_instance', return_value=mock_client):
            sink = OzoneLabelSink(config=MagicMock())

            passing_rule = RuleT(
                name='gate',
                value=True,
                description='',
                features={},
            )

            effect = AtprotoListEffect(
                did='test_did',
                list_uri='test_uri',
                dependent_rule=passing_rule,
                suppressed=False,
            )

            mock_result = MagicMock()
            mock_result.action.action_id = 123
            mock_result.effects = {AtprotoListEffect: [effect]}

            with patch.object(sink, '_add_to_list') as mock_add_to_list:
                sink.push(mock_result)
                mock_add_to_list.assert_called_once()

    def test_ac4_3_label_effect_with_failing_rule(self) -> None:
        """atproto-apply-if.AC4.3: Sink skips AtprotoLabelEffect when dependent_rule.value is False"""
        mock_client = MagicMock()
        with patch('example_plugins.src.output_sinks.ozone_label_sink.OzoneClient.get_instance', return_value=mock_client):
            sink = OzoneLabelSink(config=MagicMock())

            failing_rule = RuleT(
                name='gate',
                value=False,
                description='',
                features={},
            )

            effect = AtprotoLabelEffect(
                entity='test_entity',
                cid='test_cid',
                label='test_label',
                comment='test_comment',
                expiration_in_hours=None,
                dependent_rule=failing_rule,
                suppressed=False,
            )

            mock_result = MagicMock()
            mock_result.action.action_id = 123
            mock_result.effects = {AtprotoLabelEffect: [effect]}

            with patch.object(sink, '_apply_label') as mock_apply_label:
                sink.push(mock_result)
                mock_apply_label.assert_not_called()

    def test_ac4_4_list_effect_with_failing_rule(self) -> None:
        """atproto-apply-if.AC4.4: Sink skips AtprotoListEffect when dependent_rule.value is False"""
        mock_client = MagicMock()
        with patch('example_plugins.src.output_sinks.ozone_label_sink.OzoneClient.get_instance', return_value=mock_client):
            sink = OzoneLabelSink(config=MagicMock())

            failing_rule = RuleT(
                name='gate',
                value=False,
                description='',
                features={},
            )

            effect = AtprotoListEffect(
                did='test_did',
                list_uri='test_uri',
                dependent_rule=failing_rule,
                suppressed=False,
            )

            mock_result = MagicMock()
            mock_result.action.action_id = 123
            mock_result.effects = {AtprotoListEffect: [effect]}

            with patch.object(sink, '_add_to_list') as mock_add_to_list:
                sink.push(mock_result)
                mock_add_to_list.assert_not_called()

    def test_ac4_5_both_effects_suppressed(self) -> None:
        """atproto-apply-if.AC4.5: Sink skips any effect with suppressed=True"""
        mock_client = MagicMock()
        with patch('example_plugins.src.output_sinks.ozone_label_sink.OzoneClient.get_instance', return_value=mock_client):
            sink = OzoneLabelSink(config=MagicMock())

            passing_rule = RuleT(
                name='gate',
                value=True,
                description='',
                features={},
            )

            label_effect = AtprotoLabelEffect(
                entity='test_entity',
                cid='test_cid',
                label='test_label',
                comment='test_comment',
                expiration_in_hours=None,
                dependent_rule=passing_rule,
                suppressed=True,
            )

            list_effect = AtprotoListEffect(
                did='test_did',
                list_uri='test_uri',
                dependent_rule=passing_rule,
                suppressed=True,
            )

            mock_result = MagicMock()
            mock_result.action.action_id = 123
            mock_result.effects = {
                AtprotoLabelEffect: [label_effect],
                AtprotoListEffect: [list_effect],
            }

            with patch.object(sink, '_apply_label') as mock_apply_label, \
                 patch.object(sink, '_add_to_list') as mock_add_to_list:
                sink.push(mock_result)
                mock_apply_label.assert_not_called()
                mock_add_to_list.assert_not_called()

    def test_ac4_6_both_effects_no_apply_if(self) -> None:
        """atproto-apply-if.AC4.6: Sink applies effects with no apply_if (backward compatibility)"""
        mock_client = MagicMock()
        with patch('example_plugins.src.output_sinks.ozone_label_sink.OzoneClient.get_instance', return_value=mock_client):
            sink = OzoneLabelSink(config=MagicMock())

            label_effect = AtprotoLabelEffect(
                entity='test_entity',
                cid='test_cid',
                label='test_label',
                comment='test_comment',
                expiration_in_hours=None,
                dependent_rule=None,
                suppressed=False,
            )

            list_effect = AtprotoListEffect(
                did='test_did',
                list_uri='test_uri',
                dependent_rule=None,
                suppressed=False,
            )

            mock_result = MagicMock()
            mock_result.action.action_id = 123
            mock_result.effects = {
                AtprotoLabelEffect: [label_effect],
                AtprotoListEffect: [list_effect],
            }

            with patch.object(sink, '_apply_label') as mock_apply_label, \
                 patch.object(sink, '_add_to_list') as mock_add_to_list:
                sink.push(mock_result)
                mock_apply_label.assert_called_once()
                mock_add_to_list.assert_called_once()
