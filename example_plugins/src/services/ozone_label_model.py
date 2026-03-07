from sqlalchemy import BigInteger, Boolean, Column, MetaData, String
from sqlalchemy.ext.declarative import declarative_base

ozone_metadata = MetaData()
OzoneModel = declarative_base(name='OzoneModel', metadata=ozone_metadata)


class OzoneLabelModel(OzoneModel):
    __tablename__ = 'label'

    id = Column(BigInteger, primary_key=True)
    src = Column(String, nullable=False)
    uri = Column(String, nullable=False)
    cid = Column(String, nullable=False)
    val = Column(String, nullable=False)
    neg = Column(Boolean, nullable=True)
    cts = Column(String, nullable=False)
    exp = Column(String, nullable=True)
