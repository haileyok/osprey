from datetime import datetime, timedelta, timezone

from osprey.engine.executor.execution_context import ExecutionResult
from osprey.engine.language_types.rules import RuleT
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.lib.storage.postgres import scoped_session
from osprey.worker.sinks.sink.output_sink import BaseOutputSink
from services.local_db_labels_service import LabelsModel
from services.ozone_client import OzoneClient
from shared.metrics import prom_metrics
from sqlalchemy.dialects.postgresql import insert
from udfs.atproto.label import AtprotoLabelEffect
from udfs.atproto.list import AtprotoListEffect

logger = get_logger('ozone_label_sink')


class OzoneLabelSink(BaseOutputSink):
    def __init__(self, config: Config):
        try:
            self._client = OzoneClient.get_instance(config=config)
        except Exception as e:
            self._client = None
            logger.error(f'Failed to create Ozone client: {e}')

        self._labeler_did = config.get_str('OSPREY_BLUESKY_LABELER_DID', '')

        logger.info('Initialized Ozone labels sink')

    def will_do_work(self, result: ExecutionResult) -> bool:
        return len(result.effects) > 0

    def push(self, result: ExecutionResult) -> None:
        action_id = result.action.action_id

        for effects in result.effects.values():
            for effect in effects:
                if isinstance(effect, AtprotoLabelEffect):
                    if effect.suppressed:
                        continue
                    if effect.dependent_rule and not effect.dependent_rule.value:
                        continue
                    self._apply_label(action_id, effect)
                elif isinstance(effect, AtprotoListEffect):
                    if effect.suppressed:
                        continue
                    if effect.dependent_rule and not effect.dependent_rule.value:
                        continue
                    self._add_to_list(effect)

    @staticmethod
    def _interpolate_description(rule: RuleT) -> str:
        description = rule.description
        for name, value in rule.features.items():
            description = description.replace(f'{{{name}}}', value)
        return description

    @staticmethod
    def _build_comment(effect: AtprotoLabelEffect) -> str:
        comment = effect.comment
        matched_descriptions = [
            OzoneLabelSink._interpolate_description(r)
            for r in effect.rules
            if r.value and r.description
        ]
        if matched_descriptions:
            comment += '\n\n[Matched: ' + '; '.join(matched_descriptions) + ']'
        return comment

    def _apply_label(self, action_id: int, effect: AtprotoLabelEffect):
        if self._client is None:
            logger.error('Ozone client not initialized; cannot apply label')
            prom_metrics.labels_emitted.labels(label=effect.label, status='error').inc()
            return

        status = 'error'
        try:
            self._client.add_or_remove_label(
                action_id=action_id,
                entity_id=effect.entity,
                label=effect.label,
                neg=False,
                comment=self._build_comment(effect),
                expiration_in_hours=effect.expiration_in_hours,
                cid=effect.cid,
            )
            status = 'ok'
        except Exception as e:
            logger.error(f'Failed to emit label event: {e}')
            return
        finally:
            prom_metrics.labels_emitted.labels(label=effect.label, status=status).inc()

        # Write-through: upsert to local labels table (best-effort)
        self._write_through_label(effect)

        logger.info(f'Successfully emitted label event for {effect.entity}: {effect.label}')

    def _add_to_list(self, effect: AtprotoListEffect):
        if self._client is None:
            logger.error('Ozone client not initialized; cannot add to list')
            return

        try:
            self._client.add_did_to_list(did=effect.did, list_uri=effect.list_uri)
        except Exception as e:
            logger.error(f'Failed to create list record: {e}')
            return

        logger.info(f'Successfully added {effect.did} to {effect.list_uri}')

    def _write_through_label(self, effect: AtprotoLabelEffect) -> None:
        """Best-effort write-through to local labels table."""
        if not self._labeler_did:
            return

        try:
            exp = None
            if effect.expiration_in_hours is not None:
                exp = datetime.now(timezone.utc) + timedelta(hours=effect.expiration_in_hours)

            with scoped_session(database='osprey_db') as session:
                stmt = insert(LabelsModel).values(
                    src=self._labeler_did,
                    uri=effect.entity,
                    val=effect.label,
                    cid=effect.cid,
                    cts=datetime.now(timezone.utc),
                    exp=exp,
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=['src', 'uri', 'val'],
                    set_={
                        'cid': stmt.excluded.cid,
                        'cts': stmt.excluded.cts,
                        'exp': stmt.excluded.exp,
                    },
                )
                session.execute(stmt)
                session.commit()
        except Exception as e:
            logger.warning(f'Write-through failed for {effect.entity}/{effect.label} (will arrive via firehose): {e}')

    def stop(self) -> None:
        pass
