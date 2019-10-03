import csv
import json
import numpy as np
import os
import pandas as pd
import CleanDataAndGenerateFile
#import CombineData
import time
import threading
import datetime
import requests
import time
import threading
from multiprocessing import Process
import argparse, sys
import traceback
import queue
from multiprocessing import Queue

def dataGenerate(path, saveDirectory, times, gap, startnumber,tasknumber):
    threads = []
    #arrange the order of the receive thread and assign different tasks to them
    orderThreading = threading.Thread(target=sendMessage,
                                      args=(path, saveDirectory, times, gap, startnumber, tasknumber))
    orderThreading.start()

    # Generate different threads to generate dataset
    for i in range(0,tasknumber):
        t = threading.Thread(target=receivingMessage,args=(i, path, saveDirectory, times, gap, numberQueue))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print('Finish.')


#Send tasks to all threads
def sendMessage(path, saveDirectory, times, gap, startnumber,tasknumber):
    round = int(times/tasknumber)
    m_round = 0
    NotFinish = True
    print('round:' + str(round))
    print('numberQueue.empty() and m_round < round:' + str(numberQueue.empty() & m_round < round))
    while NotFinish:
        print('NotFinish:' + str(NotFinish))
        print('m_round init:' + str(m_round))
        print('m_round >= round:' + str(m_round >= round))
        if numberQueue.empty() and m_round < round:
            firstnumber = startnumber + m_round*tasknumber
            lastnumber = startnumber +(m_round+1)*tasknumber
            print('firstnumber:'+str(firstnumber) +' lastnumber:' + str(lastnumber))
            for number in range(firstnumber, lastnumber):
                print(number)
                numberQueue.put(number)
                time.sleep(5)
            m_round += 1
            print('m_round:' + str(m_round))
            print('numberQueue.empty():' + str(numberQueue.empty()))
            print('numberQueue.empty() and m_round < round:' + str(numberQueue.empty() and m_round < round))
        elif m_round >= round:
            NotFinish = False
        print('NotFinish last:' + str(NotFinish))

    print("Thread " + str(os.getpid()) + ", signing off")


#Different thread will read different tasks and it will generate data parallel
def receivingMessage(threadid, path, saveDirectory, times, gap, numberQueue):
    import CombineData
    while True:
        try:
            number = numberQueue.get()
            print('receive:'+str(number))
            savepath = saveDirectory + '/' + str(number) + '.json'
            print('thread:' + str(threadid))
            #Parse the parameters to the final function of data generation
            returnValue = CombineData.combineDataToJson(threadid, path, savepath, number, gap)
            print('returnValue：'+str(returnValue))
        except Exception as e:
            traceback.print_exc()
            time.sleep(10)
        time.sleep(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    #startnumber from 1 to 1000
    parser.add_argument('--startnumber', help='Start number')
    args = parser.parse_args()
    #queue, assign task for multiple thread
    numberQueue = queue.Queue()
    #Original Data source path
    #path = '/home/google_cloudadmins_lexisnexisris/telematics/testV2test.csv'
    path = '/home/google_cloudadmins_lexisnexisris/telematics/testV2_13000.csv'
    #path = '/home/google_cloudadmins_lexisnexisris/telematics/v2_1.csv'
    #Clean data, save to csv file and prepare as data source
    cleanSavePath = '/home/google_cloudadmins_lexisnexisris/telematics/testV2_clean.csv'
    #How many files need to generate
    times = 100
    # The difference of the device id between each dataset
    gap = 100
    #Thread number
    tasknumber =20
    startnumber=int(args.startnumber)
    #Dataset save path
    saveDirectory = '/home/google_cloudadmins_lexisnexisris/telematics/dataprepare'
    print('begin:'+str(datetime.datetime.now()))
    #Clean data and save the dataset
    CleanDataAndGenerateFile.CleanDataAndSave(path, cleanSavePath)
    #Generate new datasets
    dataGenerate(cleanSavePath, saveDirectory, times, gap, startnumber, tasknumber)



