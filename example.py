from __future__ import division, print_function
from pyscdi import Scdi

# 
USERNAME = ''
APIKEY = ''

def keyvalue_demo():
    conn = Scdi(USERNAME, APIKEY)
    bucketname = 'kv_demo'

    bucket = conn.create_keyvalue_bucket(bucketname)
    bucket.put('testkey','123')
    print('value:', bucket.get('testkey'))
    conn.drop_bucket(bucketname)

def kws_demo():
    conn = Scdi(USERNAME, APIKEY)
    bucketname = 'kws_demo'
    bucket = conn.create_kws_bucket(bucketname)
    bucket.put_object('example.py', './example.py')
    print(bucket.get_object_url('example.py'))
    print(bucket.get_object('example.py'))
    bucket.get_object_as_file('example.py', 'dummy')
    conn.drop_bucket(bucketname)

def timeseries_demo():
    conn = Scdi(USERNAME, APIKEY)
    bucketname = 'ts_demo'
    columns = [
            {
                "name": "ts",
                "type": "timestamp",
                "indexed": True
            },
            {
                "name": "rain",
                "type": "double",
                "indexed": True
            },
            {
                "name": "temp",
                "type": "double",
                "indexed": True
            },
            {
                "name": "remark",
                "type": "varchar",
                "indexed": False
            }
        ]
    bucket = conn.create_timeseries_bucket(bucketname, columns)
    bucket.add_row({
    	   "ts" : 1517125302,
    	   "rain" : 12.0,
    	   "temp": 20.0
        })
    print(bucket.query())

    bucket.add_rows([
        {
        	"ts" : 1517125303,
        	"rain" : 100.0,
        	"temp": 20.0
        },
        {
        	"ts" : 1517125305,
        	"rain" : 112.0,
        	"temp": 210.0
        },
        {
        	"ts" : 1517125310,
        	"rain" : 132.0,
        	"temp": 240.0
        }
        ])
    print(bucket.query(fromEpoch=1517125305))
    conn.drop_bucket(bucketname)

def geotemporal_demo():
    conn = Scdi(USERNAME, APIKEY)
    bucketname = 'geo_demo'
    columns = [
            {
                "name": "ts",
                "type": "timestamp",
                "indexed": True
            },
            {
                "name": "lat",
                "type": "double",
                "indexed": True
            },
            {
                "name": "lng",
                "type": "double",
                "indexed": True
            },
            {
                "name": "data",
                "type": "varchar",
                "indexed": False
            }
        ]
    bucket = conn.create_geotemporal_bucket(bucketname, columns)
    bucket.add_row({
    	   "ts" : 1517125302,
    	   "lat" : 13.01,
    	   "lng": 100.01,
           "data": 'a1'
        })
    print(bucket.query())

    bucket.add_rows([
        {
        	"ts" : 1517125302,
     	    "lat" : 13.01,
     	    "lng": 100.01,
            "data": 'a1'
        },
        {
        	"ts" : 1517125305,
            "lat" : 14.01,
     	    "lng": 101.01,
            "data": 'a2'
        },
        {
        	"ts" : 1517125310,
            "lat" : 15.01,
     	    "lng": 100.01,
            "data": 'a1'
        }
        ])
    print(bucket.query(where=[
        {
            'column': 'lng',
            'op': 'lt',
            'value': 101.0
        },
        {
            'column': 'lat',
            'op': 'gt',
            'value': 14.0
        },
        ]))
    conn.drop_bucket(bucketname)


print('keyvalue demo')
keyvalue_demo()

print('kws demo')
kws_demo()

print('timeseries demo')
timeseries_demo()

print('geotemporal demo')
geotemporal_demo()
