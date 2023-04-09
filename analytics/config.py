conn_detail = {
    "distance_query":"""select devices_device_id,hour,sum(distance) as distance from (SELECT devices.device_id AS devices_device_id, date_trunc('hour', to_timestamp(cast(time as integer))) as hour,
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
order by devices_device_id,hour;""",
    "aggregated_query":"""select t.device_id,t.timestamp,t.max_temp,c.count,d.distance from temperature_data t inner join count_data c on t.device_id =c.device_id and t.timestamp=c.timestamp inner join distance_data d on t.device_id =d.device_id and t.timestamp =d.timestamp;"""
	}