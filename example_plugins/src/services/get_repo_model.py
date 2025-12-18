from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


class BlobRef(BaseModel):
    link: str = Field(alias='$link')


class Blob(BaseModel):
    type: str = Field(alias='$type')
    ref: BlobRef
    mime_type: str = Field(alias='mimeType')
    size: int


class Profile(BaseModel):
    type: str = Field(alias='$type')
    avatar: Optional[Blob] = None
    banner: Optional[Blob] = None
    created_at: datetime = Field(alias='createdAt')
    description: str = ''
    display_name: str = Field(default='', alias='displayName')


class RepoRef(BaseModel):
    type: str = Field(alias='$type')
    did: str


class AccountStats(BaseModel):
    type: str = Field(alias='$type')


class RecordsStats(BaseModel):
    type: str = Field(alias='$type')


class Hosting(BaseModel):
    type: str = Field(alias='$type')
    status: str


class SubjectStatus(BaseModel):
    id: int
    review_state: str = Field(alias='reviewState')
    created_at: datetime = Field(alias='createdAt')
    updated_at: datetime = Field(alias='updatedAt')
    takendown: bool
    subject_repo_handle: str = Field(alias='subjectRepoHandle')
    subject_blob_cids: list[str] = Field(alias='subjectBlobCids')
    tags: list[str]
    priority_score: int = Field(alias='priorityScore')
    age_assurance_state: str = Field(alias='ageAssuranceState')
    subject: RepoRef
    account_stats: AccountStats = Field(alias='accountStats')
    records_stats: RecordsStats = Field(alias='recordsStats')
    hosting: Hosting


class Moderation(BaseModel):
    subject_status: Optional[SubjectStatus] = Field(alias='subjectStatus')


class LabelSignature(BaseModel):
    bytes: str = Field(alias='$bytes')


class Label(BaseModel):
    ver: int
    src: str
    uri: str
    val: str
    cts: datetime
    exp: Optional[datetime] = None
    sig: LabelSignature
    neg: Optional[bool] = None


class OzoneGetRepoResponse(BaseModel):
    did: str
    handle: str
    related_records: Optional[list[Profile]] = Field(alias='relatedRecords')
    indexed_at: Optional[datetime] = Field(alias='indexedAt')
    moderation: Optional[Moderation]
    labels: Optional[list[Label]]

    class Config:
        populate_by_name = True
