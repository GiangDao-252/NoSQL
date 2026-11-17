-- Run after: python3 sensorring.py load
-- cqlsh 127.0.0.1 9042 -f cassandra_lab.cql

-- Compare a query by partition key with a query that filters across partitions.
TRACING ON;

-- Latest three readings in the hn-01 partition (ts is clustered DESC).
SELECT ts, temperature
FROM iot.readings_by_sensor
WHERE sensor_id = 'hn-01'
LIMIT 3;

-- Temperature is a regular column. This demonstration filters across partitions.
SELECT count(*)
FROM iot.readings_by_sensor
WHERE temperature > 31.5
ALLOW FILTERING;

TRACING OFF;
