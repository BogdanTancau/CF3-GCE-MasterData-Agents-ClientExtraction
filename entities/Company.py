import sqlalchemy
from sqlalchemy import Table as TABLE
from sqlalchemy import Column as COLUMN
from sqlalchemy.schema import PrimaryKeyConstraint as PRIMARYKEY

from sqlalchemy.dialects.mysql import CHAR
from sqlalchemy.dialects.mysql import VARCHAR
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.dialects.mysql import BOOLEAN

import importlib
# from base import Base
Base = importlib.import_module('entities.base')

class Company(Base.Base):
    __tablename__ = 'Companies'
    Id = COLUMN(CHAR(36), primary_key=True)
    Name = COLUMN("Name", VARCHAR(255))
    Ident = COLUMN("Ident", VARCHAR(100))
    SNOWTarget = COLUMN("SNOWTarget", VARCHAR(10))
    FetchActive = COLUMN("FetchActive", BOOLEAN)
    LastFetchDate = COLUMN("LastFetchDate", DATETIME)
    PushActive = COLUMN("PushActive", BOOLEAN)
    CustomPush = COLUMN("CustomPush", BOOLEAN)
    LastPushDate = COLUMN("LastPushDate", DATETIME)
    CreatedOn = COLUMN("CreatedOn", DATETIME)
    CreatedBy = COLUMN("CreatedBy", VARCHAR(255))

    def __init__(self, name, ident, snowTarget, fetchActive, lastFetchDate, pushActive, customPush, lastPushDate, createdOn, createdBy):
        self.Name = name
        self.Ident = ident
        self.SNOWTarget = snowTarget
        self.FetchActive = fetchActive
        self.LastFetchDate = lastFetchDate
        self.PushActive = pushActive
        self.CustomPush = customPush
        self.LastPushDate = lastPushDate
        self.CreatedOn = createdOn
        self.CreatedBy = createdBy
