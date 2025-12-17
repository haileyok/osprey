BlockSubjectDid: Entity[str] = EntityJson(
  type='User',
  path='$.operation.record.subject',
  coerce_type=True,
)
