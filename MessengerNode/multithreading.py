# sending all the samples in the samples folder via multithreading

from queue import Queue
from threading import Thread
import kafka
from kafka import KafkaProducer
import time
import datetime
import logging
import os


logging.basicConfig(filename='log.txt',level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


broker = '172.31.42.181:9092'
producer = KafkaProducer(bootstrap_servers=broker, api_version=(0, 10),buffer_memory=335544320,compression_type='gzip')
topic = 'Telematics'
directory = r'samples'

myQueue = Queue()

class DownloadWorker(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.msgCount = 0

    def run(self):
        while True:
            # Get the work from the queue and expand the tuple
            line = myQueue.get()
            myQueue.task_done()
            curTime = time.time()
            timeJSON = '], "time":"' + str(curTime) + '"}'
            data = str(line).replace(']}', timeJSON )
            data = data.encode('utf-8')
            try:
                producer.send(topic, data)
                self.msgCount = self.msgCount + 1
            except:
                pass
            finally:
                if self.msgCount >= 2000:
                    producer.flush()
                    self.msgCount = 0
def main():
    part = 0
    workers = []
    # number of workders
    n = 2
    for _ in range(n):
        worker = DownloadWorker()
        workers.append(worker)
#       worker.daemon = True
        worker.start()
    mainStartTime = time.time()
    for f in os.scandir(directory):
        part += 1
        start = time.time()
        with open(f, 'r') as inFile:
            for line in inFile:
                myQueue.put((line))
        inFile.close()
        # wait for queue to empty
        myQueue.join()
        end = time.time()
        logging.info('Sample ' + str(part) + ' Took: %s', end - start)
    # wait for worker threads to complete
    for _ in range(n):
        myQueue.put(None)
    for worker in workers:
        worker.join()
    producer.flush()
    producer.close()
    mainEndTime = time.time()
    totalTime = mainEndTime - mainStartTime
    logging.info('Total Messenger Process Took: %s',totalTime)


if __name__ == '__main__':
    main()
