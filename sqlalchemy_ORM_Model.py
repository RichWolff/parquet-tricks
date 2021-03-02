from osipi.io.sql import create_sql_cnxn
from proactive_monitoring import settings
from sqlalchemy import MetaData
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column,
    INTEGER,
    VARCHAR,
    DATETIME,
    FLOAT,
    Boolean
)
import pyodbc
pyodbc.pooling = False


# Get Engine to develop table space
engine = create_sql_cnxn(
    **settings.OUTPUT.BASE_DATA,
    async_engine=False,
    fast_executemany=True
)

ASYNC_ENGINE = create_sql_cnxn(
    **settings.OUTPUT.BASE_DATA,
    async_engine=True,
    fast_executemany=True
)

# produce our own MetaData object
metadata = MetaData()
session = Session(engine)
Base = declarative_base(metadata=MetaData(schema='source'))
SQLALCHEMY_TABLE = Base


class Metrics(Base):
    __tablename__ = 'proactive_monitoring_metrics'

    id = Column(INTEGER(), primary_key=True, autoincrement=True)  # Auto-increment should be default
    cycle_id = Column(VARCHAR(64), nullable=False)
    TimeStamp = Column(DATETIME(), nullable=True)
    normalized_time = Column(INTEGER(), nullable=True)
    metric_name = Column(VARCHAR(64), nullable=True)
    value = Column(FLOAT(precision=12), nullable=True)

    def __repr__(self):
        return "<Metrics(cycle_id='%s', TimeStamp='%s', normalized_time='%s', " \
                "metric_name='%s', value='%s')" % (self.cycle_id, self.TimeStamp, self.normalized_time,  self.metric_name, self.value)  # noqa


class Headers(Base):
    __tablename__ = 'proactive_monitoring_header'

    cycle_id = Column(VARCHAR(64), nullable=False, primary_key=True)
    line = Column(VARCHAR(32), nullable=False)
    asset = Column(VARCHAR(32), nullable=False)
    test_type = Column(VARCHAR(32), nullable=False)
    begin = Column(DATETIME(), nullable=False)
    end = Column(DATETIME(), nullable=False)
    duration = Column(INTEGER, nullable=False)
    missing_metrics = Column(Boolean(), nullable=False)

    def __repr__(self):
        return f"<Headers(cycle_id='{self.cycle_id}', line='{self.line}', asset='{self.asset}', test_type='{self.test_type}', begin='{self.begin}', end='{self.end}'', duration={self.duration}, missing_metrics={self.missing_metrics})>"  # noqa


class Observations(Base):
    __tablename__ = 'proactive_monitoring_observations'

    id = Column(INTEGER, primary_key=True, autoincrement=True)  # Auto-increment should be default
    cycle_id = Column(VARCHAR(64), nullable=False)
    TimeStamp = Column(DATETIME(), nullable=False)
    normalized_time = Column(INTEGER(), nullable=False)
    tag = Column(VARCHAR(length=128), nullable=False)
    value = Column(FLOAT(precision=12), nullable=False)

    def __repr__(self):
        return f"<Observations(cycle_id={self.cycle_id}, TimeStamp={self.TimeStamp}, normalized_time={self.normalized_time}, tag={self.tag}, value={self.value})>"  # noqa
