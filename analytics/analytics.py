from os import environ
from time import sleep
from sqlalchemy import create_engine, Column, Integer, Float, String, JSON, DateTime, func,text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import json
import config as cfg

distance_query=cfg.conn_detail['distance_query']
aggregated_query=cfg.conn_detail['aggregated_query']

# Define the database connection string
pg_connection_str = environ["POSTGRESQL_CS"]
mysql_connection_str = environ["MYSQL_CS"]

# Create SQLAlchemy engine for PostgreSQL and MySQL databases
pg_engine = create_engine(pg_connection_str)
mysql_engine = create_engine(mysql_connection_str)

# Define a sessionmaker for each engine
pg_session = sessionmaker(bind=pg_engine)
mysql_session = sessionmaker(bind=mysql_engine)

# Define the declarative base for ORM
Base = declarative_base()
Base1 = declarative_base()
# Define the device data model
class DeviceData(Base1):
    __tablename__ = 'devices'

    id = Column(Integer, primary_key=True)
    device_id = Column(String(255))
    temperature = Column(Integer)
    location = Column(JSON)
    time = Column(Integer)

class temperature_data(Base):
    __tablename__ = 'temperature_data'
    id = Column(Integer, primary_key=True)
    device_id = Column(String(255))
    timestamp = Column(DateTime)
    max_temp = Column(Float)

class count_data(Base):
    __tablename__ = 'count_data'
    id = Column(Integer, primary_key=True)
    device_id = Column(String(255))
    timestamp = Column(DateTime)
    count = Column(Float)

class distance_data(Base):
    __tablename__ = 'distance_data'
    id = Column(Integer, primary_key=True)
    device_id = Column(String(255))
    timestamp = Column(DateTime)
    distance = Column(Float)

class aggregated_data(Base):
    __tablename__ = 'aggregated_data'
    id = Column(Integer, primary_key=True)
    device_id = Column(String(255))
    timestamp = Column(DateTime)
    max_temp  =Column(Float)
    count=Column(Integer)
    distance = Column(Float)
    
# Create the tables in the databases
Base.metadata.create_all(mysql_engine)

# Create a PostgreSQL session
pg_sess = pg_session()
mysql_session = mysql_session()

# Query the maximum temperatures measured for every device per hour
max_temps_query = pg_sess.query(
        DeviceData.device_id,
        func.date_trunc('hour', (func.to_timestamp(func.cast(DeviceData.time, Integer)))).label('hour'),
        func.max(DeviceData.temperature).label('max_temp')
    ) \
    .group_by(DeviceData.device_id, text('hour')) \
    .order_by(DeviceData.device_id, text('hour'))
# Execute the query and get the results
max_temps = max_temps_query.all()
for row in max_temps:
    row1 = temperature_data(device_id=row.device_id, timestamp=row.hour,max_temp=row.max_temp)
    mysql_session.add(row1)
    mysql_session.commit()
    
# Query the data points aggregated for every device per hour
count_temps_query = pg_sess.query(
        DeviceData.device_id,
        func.date_trunc('hour', (func.to_timestamp(func.cast(DeviceData.time, Integer)))).label('hour'),
        func.count(DeviceData.temperature).label('count_temp')
    ) \
    .group_by(DeviceData.device_id, text('hour')) \
    .order_by(DeviceData.device_id, text('hour'))
# Execute the query and get the results
count_temp = count_temps_query.all()
for row in count_temp:
    row2 = count_data(device_id=row.device_id, timestamp=row.hour,count=row.count_temp)
    mysql_session.add(row2)
    mysql_session.commit()

# Query the distance measured for every device per hour
with pg_engine.connect() as con:
        statement = text(distance_query)
# Execute the query and get the results        
        results=con.execute(statement)
        for row in results:
            row3 = distance_data(device_id=row.devices_device_id, timestamp=row.hour,distance=row.distance)
            mysql_session.add(row3)
            mysql_session.commit()

# Insert the aggregated data into the final table in MYSQL DB
with mysql_session.connection() as con:
        statement = text(aggregated_query)        
# Execute the query and get the results           
        results=con.execute(statement)
        for row in results:
            row4 = aggregated_data(device_id=row.device_id, timestamp=row.timestamp,max_temp=row.max_temp,count=row.count,distance=row.distance)
            mysql_session.add(row4)
            mysql_session.commit()
            
