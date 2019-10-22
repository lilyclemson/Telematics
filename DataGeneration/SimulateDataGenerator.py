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
import Queue as queue

def dataGenerate(path, saveDirectory, times, gap, startnumber,tasknumber):
    threads = []
    orderThreading = threading.Thread(target=sendMessage,
                                      args=(path, saveDirectory, times, gap, startnumber, tasknumber))
    orderThreading.start()

    for i in range(0,tasknumber):
        t = threading.Thread(target=receivingMessage,args=(i, path, saveDirectory, times, gap, numberQueue))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def sendMessage(path, saveDirectory, times, gap, startnumber,tasknumber):
    round = int(times/tasknumber)
    m_round = 0
    NotFinish = True
    print(round)
    print('numberQueue.empty() and m_round <= round:' + str(numberQueue.empty() & m_round <= round))
    while NotFinish:
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
            print('numberQueue.empty() and m_round <= round:' + str(numberQueue.empty() and m_round <= round))
        elif m_round >= round:
            NotFinish = False


def receivingMessage(threadid, path, saveDirectory, times, gap, numberQueue):
    import CombineData
    while True:
        try:
            number = numberQueue.get()
            print('receive:'+str(number))
            savepath = saveDirectory + '/' + str(number) + '.json'
            print('thread:' + str(os.getpid()))
            returnValue = CombineData.combineDataToJson(threadid, path, savepath, number, gap)
            print('returnValue: '+str(returnValue))
        except Exception as e:
            traceback.print_exc()
            time.sleep(10)
        time.sleep(1)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--startnumber', help='Start number')
    args = parser.parse_args()
    numberQueue = queue.Queue()
    #print(args.startnumber)
    path = '/home/ec2-user/telematics/testV2test.csv'
    #path = '/home/ec2-user/telematics/testV2_13000.csv'
    #path = '/home/ec2-user/telematics/V2_1.csv'
    cleanSavePath = '/home/ec2-user/telematics/testV2_clean.csv'
    times = 10
    gap = 100
    tasknumber = 1
    startnumber=int(args.startnumber)
    saveDirectory = '/mnt/disks/sdb/dataprepare'
    print('begin:'+str(datetime.datetime.now()))
    CleanDataAndGenerateFile.CleanDataAndSave(path, cleanSavePath)
    dataGenerate(cleanSavePath,saveDirectory,times, gap, startnumber,tasknumber)



