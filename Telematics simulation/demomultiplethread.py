import kafka
from kafka import KafkaProducer
import traceback
import pandas as pd
import time
#import Queue as queue # python 2
import queue #python 3
import threading
import os
from os import walk
import configparser

#Caculate object memory usage
def mem_usage(pandas_obj):
    if isinstance(pandas_obj,pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:# we assume if not a df it's a series
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_mb = usage_b / 1024 ** 2 # convert bytes to megabytes
    return "{:03.2f} MB".format(usage_mb)

#Split Json file and push it to Kafka Message Queue
def sendData(telematics_json_file, broker, sleeptime, topic):
    returnValue = False
    try:
        telematics_json = pd.read_json(telematics_json_file)
        #print(mem_usage(telematics_json))
        telematics_json.index = telematics_json.index.astype(int)
        telematics_json = telematics_json.sort_index()
        #Init kafka Producer and connect to Kafka server
        producer = KafkaProducer(bootstrap_servers=[broker], api_version=(0, 10), max_request_size=10485760)
        print('thread:' + threading.current_thread().name + 'begin to send')
        for index, row in telematics_json.iterrows():
            byteRow = str(row[0].replace('tripID', 'tripid').replace('deviceID', 'deviceid').replace('timeStamp', 'timestamp').replace('accData', 'accdata')).encode('utf-8')
            future = producer.send(topic, byteRow)#Send message to Kafka Message Queue
            print('thread:' + threading.current_thread().name + ' send index:' + str(index)+ ' data:'+ row[0][0:50])
            time.sleep(sleeptime)
        print('thread:' + threading.current_thread().name + 'end to send')
        returnValue = True
    except Exception as ex:
        traceback.print_exc()
    return returnValue

#Read Config file and load the configuration
def readConfig():
    config = configparser.ConfigParser()
    config.read('config.ini')
    telematics_json_directory = config['DEFAULT']['telematics_json_directory']
    telematics_json_start = int(config['DEFAULT']['telematics_json_start'])
    telematics_json_end = int(config['DEFAULT']['telematics_json_end'])
    broker = config['DEFAULT']['broker']
    threadnumber = int(config['DEFAULT']['threadnumber'])
    sleeptime = float(config['DEFAULT']['sleeptime'])
    topic = 'telematics'

    return telematics_json_directory, telematics_json_start, telematics_json_end, broker, threadnumber, sleeptime, topic

def orderbyNumber(x):
    x = x.replace(".json", "")
    return int(x)

#If there are multiple Json files and multiple sender, it will send by order
def sendOrder(telematics_directory, telematics_json_start, telematics_json_end, threadnumber):
    files = []
    for i in range(telematics_json_start, telematics_json_end+1):
        filenames = str(i)+".json"
        files.append(filenames)
    print(files)
    files = sorted(files, key=orderbyNumber)
    filenumber = len(files)
    round = int(filenumber / threadnumber)
    m_round = 0
    NotFinish = True
    startnumber = telematics_json_start
    print(round)
    print('numberQueue.empty() and m_round <= round:' + str(numberQueue.empty() & m_round <= round))
    while NotFinish:
        if numberQueue.empty() and m_round <= round:
            firstnumber = startnumber + m_round * threadnumber
            lastnumber = startnumber + (m_round + 1) * threadnumber
            if len(files) < lastnumber:
                lastnumber = len(files)
            print('firstnumber:' + str(firstnumber) + ' lastnumber:' + str(lastnumber))
            for number in range(firstnumber, lastnumber):
                filename = os.path.abspath(os.path.join(telematics_directory, files[number]))
                numberQueue.put(filename)# put order to queue
                print(filename)
                time.sleep(2)#Keep every thread not starts at the same time
            m_round += 1
            print('m_round:' + str(m_round))
            print('numberQueue.empty():' + str(numberQueue.empty()))
            print('numberQueue.empty() and m_round <= round:' + str(numberQueue.empty() and m_round <= round))
        elif m_round >= round:
            NotFinish = False

#Multiple thread will parallel recieve the Json file name, load that file and send the message to Kafka Message Queue
def receivedOrder(numberQueue,broker, sleeptime):
    while True:
        try:
            number = numberQueue.get()#fetch order
            print('thread:' + threading.current_thread().name + 'receive:' + str(number))
            returnValue = sendData(number, broker, sleeptime)
            print('returnValue' + str(returnValue))
        except Exception as e:
            traceback.print_exc()
            time.sleep(10)
        time.sleep(1)

if __name__ == "__main__":
    numberQueue = queue.Queue()
    telematics_json_directory, telematics_json_start, telematics_json_end, broker, threadnumber, sleeptime, topic = readConfig()
    threads = []
    orderThreading = threading.Thread(target=sendOrder, args=(telematics_json_directory, telematics_json_start, telematics_json_end, threadnumber))
    orderThreading.start()

    for i in range(0, threadnumber):
        t = threading.Thread(target=receivedOrder, args=(numberQueue, broker, sleeptime, topic))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
