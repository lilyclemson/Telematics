# sending all the samples in the samples folder via multithreading

from queue import Queue
from threading import Thread
import kafka
from kafka import KafkaProducer
import time
import datetime
import logging
import os


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


broker = '172.31.42.181:9092'
producer = KafkaProducer(bootstrap_servers=broker, api_version=(0, 10),max_request_size=104857600)
topic = 'Telematics'
directory = r'samples'

class DownloadWorker(Thread):
    
    def __init__(self, queue):
        Thread.__init__(self)
        self.queue = queue

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            line = self.queue.get()
            curTime = time.time()
            timeJSON = '], "time":"' + str(curTime) + '"}'
            data = str(line).replace(']}', timeJSON )
            data = data.encode('utf-8')
            try:
                producer.send(topic, data)
            finally:
                producer.flush()                
                self.queue.task_done()
def main():
    part = 0
    mainStartTime = time.time()
    for f in os.scandir(directory):
        part += 1
        start = time.time()
        queue = Queue()
        # number of workders
        n = 8  
        for x in range(n):
            worker = DownloadWorker(queue)
            worker.daemon = True
            worker.start()
        with open(f, 'r') as inFile:
            for line in inFile:
                queue.put((line))
#                 time.sleep(1)
        queue.join()
        end = time.time()
        logging.info('Sample ' + str(part) + ' Took: %s', end - start)
        inFile.close()
    producer.close()
    mainEndTime = time.time()
    totalTime = mainStartTime - mainEndTime
    logging.info('Total Messenger Process Took: %s',totalTime)
    
    
if __name__ == '__main__':
    main() 
