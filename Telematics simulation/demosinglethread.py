
from kafka import KafkaProducer
#import logging
import json
import traceback
import pandas as pd
import time
#logging.basicConfig(level=logging.DEBUG)# Debug the message
try:
    print('Demo init')
    topic = 'telematics'
    telematics_json_file = '/home/google_cloudadmins_lexisnexisris/telematics/dataprepare/1.json'
    telematics_json = pd.read_json(telematics_json_file)
    producer = KafkaProducer(bootstrap_servers=['10.128.0.3:9092'], api_version=(0, 10))
    print('Connect to kafka queue')
    print('begin to send')
    for index, row in telematics_json.iterrows():
        byteRow = str(row[0].replace('tripID','tripid').replace('deviceID', 'deviceid').replace('timeStamp', 'timestamp').replace('accData', 'accdata')).encode('utf-8')
        future = producer.send(topic, byteRow)
        print('send message:' + row[0])
    print('end to send')
    time.sleep(2)
except:
    traceback.print_exc()
