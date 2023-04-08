from os import environ
from time import sleep
from sqlalchemy import create_engine, Column, Integer, Float, String, JSON, DateTime, func,text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import json

#sleep(100)
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

# Define the device data model
class DeviceData(Base):
    __tablename__ = 'devices'

    id = Column(Integer, primary_key=True)
    device_id = Column(String)
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
Base.metadata.create_all(pg_engine)

# Create a PostgreSQL session
pg_sess = pg_session()
mysql_session = mysql_session()

# Truncate staging tables
with mysql_session.connection() as con_tr:
        con_tr.execute(text("""CREATE TABLE IF NOT EXISTS temperature_data (
                               id INT NOT NULL AUTO_INCREMENT,
                               device_id VARCHAR(255) NOT NULL,
                               timestamp DATETIME NOT NULL,
                               max_temp FLOAT NOT NULL,
                               PRIMARY KEY (id)
                               );;"""))
        con_tr.execute(text("""CREATE TABLE IF NOT EXISTS count_data (
                              id INT NOT NULL AUTO_INCREMENT,
                              device_id VARCHAR(255) NOT NULL,
                              timestamp DATETIME NOT NULL,
                              count FLOAT NOT NULL,
                              PRIMARY KEY (id)
                            );"""))
        con_tr.execute(text("""CREATE TABLE IF NOT EXISTS distance_data (
                              id INT NOT NULL AUTO_INCREMENT,
                              device_id VARCHAR(255) NOT NULL,
                              timestamp DATETIME NOT NULL,
                              distance FLOAT NOT NULL,
                              PRIMARY KEY (id)
                            );"""))  
        con_tr.execute(text("""CREATE TABLE IF NOT EXISTS aggregated_data (
                              id INT NOT NULL AUTO_INCREMENT,
                              device_id VARCHAR(255) NOT NULL,
                              timestamp DATETIME NOT NULL,
                              max_temp FLOAT NOT NULL,
                              count Integer ,
                              distance FLOAT NOT NULL,
                              PRIMARY KEY (id)
                            );"""))  
        mysql_session.commit()

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
        statement = text("""select devices_device_id,hour,sum(distance) as distance from (SELECT devices.device_id AS devices_device_id, date_trunc('hour', to_timestamp(cast(time as integer))) as hour,
case when lag(cast(location as json), 1) over (partition by device_id order by time) is null then 0 
else acos(sin(radians(json_extract_path_text(cast(location as json), 'latitude')::float)) * 
sin(radians(json_extract_path_text(lag(cast(location as json), 1) over (partition by device_id order by time), 'latitude')::float)) + 
cos(radians(json_extract_path_text(cast(location as json), 'latitude')::float)) * 
cos(radians(json_extract_path_text(lag(cast(location as json), 1) over (partition by device_id order by time), 'latitude')::float)) * 
cos(radians(json_extract_path_text(cast(location as json), 'longitude')::float) - 
radians(json_extract_path_text(lag(cast(location as json), 1) over (partition by device_id order by time), 'longitude')::float))) *
6371 end AS distance 
FROM devices )s
group by devices_device_id,hour
order by devices_device_id,hour;""")
# Execute the query and get the results        
        results=con.execute(statement)
        for row in results:
            row3 = distance_data(device_id=row.devices_device_id, timestamp=row.hour,distance=row.distance)
            mysql_session.add(row3)
            mysql_session.commit()

# Insert the aggregated data into the final table in MYSQL DB
with mysql_session.connection() as con:
        statement = text("""select t.device_id,t.timestamp,t.max_temp,c.count,d.distance from temperature_data t inner join count_data c on t.device_id =c.device_id and t.timestamp=c.timestamp inner join distance_data d on t.device_id =d.device_id and t.timestamp =d.timestamp;""")        
# Execute the query and get the results           
        results=con.execute(statement)
        for row in results:
            #print(f"Device {row.device_id}, Hour {row.timestamp}, Max Temp {row.max_temp}, Count {row.count}, Distance {row.distance}")
            row4 = aggregated_data(device_id=row.device_id, timestamp=row.timestamp,max_temp=row.max_temp,count=row.count,distance=row.distance)
            mysql_session.add(row4)
            mysql_session.commit()
            
