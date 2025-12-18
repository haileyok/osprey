import logging
from typing import Any, Dict

import requests
from osprey.engine.executor.execution_context import ExecutionResult
from osprey.worker.lib.config import Config
from osprey.worker.sinks.sink.output_sink import BaseOutputSink
from services.atproto import OzoneSession
from udfs.atproto.label import AtprotoLabelEffect

logger = logging.getLogger('ozone_label_sink')


class OzoneLabelSink(BaseOutputSink):
    def __init__(self, config: Config):
        try:
            self._session = OzoneSession.get_instance(config=config)
        except Exception as e:
            self._session = None
            logger.error(f'Failed to create Bluesky session: {e}')

        self._labeler_did = config.get_str('OSPREY_BLUESKY_LABELER_DID', 'did:plc:123')
        self._pds_url = config.get_str('OSPREY_BLUESKY_PDS_URL', 'https://bsky.social')

        logger.info('Initialized Ozone labels sink')

    def will_do_work(self, result: ExecutionResult) -> bool:
        return len(result.effects) > 0

    def push(self, result: ExecutionResult) -> None:
        action_id = result.action.action_id

        for effects in result.effects.values():
            for effect in effects:
                if not isinstance(effect, AtprotoLabelEffect):
                    continue

                self._apply_label(action_id, effect)

    def _apply_label(self, action_id: int, effect: AtprotoLabelEffect):
        if self._session is None:
            logger.error('Bluesky session not initialized, cannot apply label')
            return

        try:
            subject: Dict[str, str] = {}
            if effect.entity.startswith('did:'):
                subject['$type'] = 'com.atproto.admin.defs#repoRef'
                subject['did'] = effect.entity
            elif effect.entity.startswith('at://'):
                subject['$type'] = 'com.atproto.repo.strongRef'
                subject['cid'] = effect.cid or ''
                subject['uri'] = effect.entity

            payload: Dict[str, Any] = {
                'subject': subject,
                'createdBy': self._session.get_did(),
                'subjectBlobCids': [],
                'event': {
                    '$type': 'tools.ozone.moderation.defs#modEventLabel',
                    'comment': effect.comment,
                    'createLabelVals': [effect.label],
                    'negateLabelVals': [],
                    'durationInHours': effect.expiration_in_hours or 0,
                },
                'modTool': {'name': 'osprey', 'meta': {'actionId': str(action_id)}},
            }

            headers = self._session.get_headers_with_moderation()

            response = requests.post(
                f'{self._pds_url}/xrpc/tools.ozone.moderation.emitEvent',
                headers=headers,
                json=payload,
            )
            response.raise_for_status()

            logger.info(f'Successfully emitted label event for {effect.entity}: {effect.label}')

        except Exception as e:
            logger.error(f'Failed to emit label event: {e}')

    def stop(self) -> None:
        pass
