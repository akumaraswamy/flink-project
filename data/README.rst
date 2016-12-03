Batch Data Source
------------------
- Use wget or bitsadmin to get trips data from https://s3.amazonaws.com/tripdata/201609-citibike-tripdata.zip
- Unzip and use the CSV as input

Elastic Search Index
--------------------
PUT citi-bikes


PUT /citi-bikes/_mapping/bike-alerts
{"bike-alerts" : {"properties" : {"cnt": {"type": "integer"},"location": {"type": "geo_point"},"name": {"type": "String"},"time": {"type": "date"}}}}