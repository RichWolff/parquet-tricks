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
    

# Utilities for writing to sql
def pandasIter(df: pd.DataFrame, chunksize: int) -> Generator[pd.DataFrame, None, None]:
    """Iterates through a pandas dataframe using a generator framework at `chunksize` rows.

    Args:
        df (pd.DataFrame): [description]
        chunksize (int): [description]

    Yields:
        Generator[pd.DataFrame, None, None]: The original pandas dataframe chunked by chunksize
    """
    iters = df.shape[0] // chunksize + 1 if df.shape[0] >= chunksize else 2
    rge = np.linspace(0, df.shape[0], iters, dtype=np.int)
    for i, j in zip(rge, rge[1:]):
        yield df.iloc[i:j]


def to_records(df: pd.DataFrame) -> List[Dict[str, str]]:
    """Return pandas dataframe as a list of records. Replaces np.nan types with None.

    Args:
        df (pd.DataFrame): Dataframe to convert to records

    Returns:
        List[Dict[str, str]]: original dataframe in [{'column': 'value'}] form.
    """
    columns = df.columns.tolist()
    return [
        dict(zip(columns, [None if pd.isnull(r) else r for r in row]))
        for row in df.itertuples(index=False, name=None)
    ]


async def _async_to_sql(df: pd.DataFrame, table: SQLALCHEMY_TABLE, conn: ASYNC_TRANSACTION, chunksize: int = 1000) -> None:
    """Asynchronously iterates through chunks of a dataframe and sends it to sql using SQLALCHEMY and asyncio.

    Args:
        df (pd.DataFrame): Dataframe to iterate through
        table (SQLALCHEMY_TABLE): Table object that data will pass through
        conn (ASYNC_TRANSACTION): Sql alchemy Async Transaction. If one fails, all data will fail to push.
        chunksize (int, optional): # Of Rows to iterate through per batch. Defaults to 1000.
    """
    await asyncio.gather(*[
        asyncio.create_task(conn.execute(
            table.__table__.insert().values(to_records(df_iter))
        )) for df_iter in pandasIter(df, chunksize)
    ])


async def async_to_sql(test: ProactiveMonitoring, chunksize: int = 1000) -> None:
    """Asynchronously iterates through the header, metric, and observations table of our proactive monitoring,
    then chunks each dataframe and sends it to sql using SQLALCHEMY and asyncio.

    Args:
        test (ProactiveMonitoring): [description]
        chunksize (int, optional): [description]. Defaults to 1000.
    """
    _metrics = test.metrics.reset_index()
    _header = test.header.rename(columns={'value': 'missing_metrics'}).reset_index()
    _observations = test.observations.copy()
    _observations.columns.name = 'tag'
    _observations = _observations.stack().to_frame('value').reset_index()

    async with ASYNC_ENGINE.begin() as conn:
        await _async_to_sql(_header, Headers, conn, chunksize=chunksize)
        await _async_to_sql(_metrics, Metrics, conn, chunksize=chunksize)
        await _async_to_sql(_observations, Observations, conn, chunksize=chunksize)

