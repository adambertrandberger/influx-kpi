from influxdb import InfluxDBClient
import math
import time
import sys
import csv

def get_measurement(name):
    client = InfluxDBClient(host='localhost', port=8086, database="signals")
    
    query = f'select Va, Vb, Vc from {name} where time > now() - 2s and time <= now() - 1s'
    
    result = client.query(query)
    
    print(result);
        
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Requires a measurement name')
    else:
        get_measurement(sys.argv[1])
