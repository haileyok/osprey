from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.config import Config
from osprey.worker.lib.osprey_shared.logging import get_logger
from osprey.worker.sinks.sink.output_sink import BaseOutputSink
from services.ozone_client import OzoneClient
from shared.metrics import prom_metrics
from udfs.atproto.label import AtprotoLabelEffect
from udfs.atproto.list import AtprotoListEffect
from udfs.atproto.tag import AtprotoTagEffect

logger = get_logger('ozone_label_sink')


class OzoneLabelSink(BaseOutputSink):
    def __init__(self, config: Config):
        try:
            self._client = OzoneClient.get_instance(config=config)
        except Exception as e:
            self._client = None
            logger.error(f'Failed to create Ozone client: {e}')

        logger.info('Initialized Ozone labels sink')

    def will_do_work(self, result: ExecutionResult) -> bool:
        return len(result.effects) > 0

    def push(self, result: ExecutionResult) -> None:
        action_id = result.action.action_id

        for effects in result.effects.values():
            for effect in effects:
                if isinstance(effect, AtprotoLabelEffect):
                    self._apply_label(action_id, effect)
                elif isinstance(effect, AtprotoListEffect):
                    self._add_to_list(effect)
                elif isinstance(effect, AtprotoTagEffect):
                    self._apply_tag(action_id, effect)

    def _apply_label(self, action_id: int, effect: AtprotoLabelEffect):
        assert self._client is not None

        status = 'error'
        try:
            self._client.add_or_remove_label(
                action_id=action_id,
                entity_id=effect.entity,
                label=effect.label,
                neg=False,
                comment=effect.comment,
                expiration_in_hours=effect.expiration_in_hours,
                cid=effect.cid,
            )
            status = 'ok'
        except Exception as e:
            logger.error(f'Failed to emit label event: {e}')
            return
        finally:
            prom_metrics.labels_emitted.labels(label=effect.label, status=status).inc()

        logger.info(f'Successfully emitted label event for {effect.entity}: {effect.label}')

    def _add_to_list(self, effect: AtprotoListEffect):
        assert self._client is not None

        try:
            self._client.add_did_to_list(did=effect.did, list_uri=effect.list_uri)
        except Exception as e:
            logger.error(f'Failed to create list record: {e}')
            return

        logger.info(f'Successfully added {effect.did} to {effect.list_uri}')

    def _apply_tag(self, action_id: int, effect: AtprotoTagEffect):
        assert self._client is not None

        status = 'error'
        try:
            comment = self._build_tag_comment(effect)
            self._client.add_or_remove_tag(
                entity_id=effect.entity,
                tag=effect.tag,
                neg=effect.neg,
                comment=comment,
            )
            status = 'ok'
        except Exception as e:
            logger.error(f'Failed to emit tag event: {e}')
            return
        finally:
            prom_metrics.tags_emitted.labels(tag=effect.tag, status=status).inc()

            action = 'removed' if effect.neg else 'applied'

            logger.info(f'Successfully {action} tag for {effect.entity}: {effect.tag}')

    @staticmethod
    def _build_tag_comment(effect: AtprotoTagEffect) -> str:
        comment = effect.comment
        matched_descriptions = [
            OzoneLabelSink._interpolate_description(r) for r in effect.rules if r.value and r.description
        ]
        if matched_descriptions:
            comment += '\n\n[Matched: ' + '; '.join(matched_descriptions) + ']'
        return comment

    def stop(self) -> None:
        pass
