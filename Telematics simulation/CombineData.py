import csv
import json
import numpy as np
import os
import pandas as pd
import threading
import datetime
from multiprocessing import Process
import time

def combineDataToJson(threadid, path, savepath, number, gap):
    start = time.clock()
    returnValue = False
    print('thread:' + str(threadid) + ' start:' + str(datetime.datetime.now()))
    #Read data file to dataframe
    df = pd.read_csv(path, low_memory=False)
    #Sort the data by deviceID,tripID and the original order
    df['deviceID'] = df['deviceID'].map(lambda x: x + gap*number)
    df = df.rename_axis('MyIdx').sort_values(by=['deviceID', 'tripID', 'MyIdx'])
    print('thread:'+str(threadid)+str(df.shape))
    df_tmp = None
    preTripID = -1
    preDeviceID = -1
    list_tmp = []
    df_combined = pd.DataFrame(columns=['Row'])
    tripJson = ''
    try:
        for index, row in df.iterrows():
             if(index % 1000 ==1):
                print('thread:'+ str(threadid)+' index:'+str(index))
             curTripID = int(row['tripID'])
             curDeviceID = int(row['deviceID'])
             if preDeviceID == -1 and preTripID == -1:
                preDeviceID = curDeviceID
                preTripID = curTripID
                list_tmp.append(row)
             elif (preDeviceID != curDeviceID) or (preTripID != curTripID):
                #print(df_tmp)
                df_tmp = pd.DataFrame().append(list_tmp)
                #Combine data by trip and convert each trip to json structure
                tripJson = df_tmp.to_json(orient='records').replace("\/", "/")
                tripJson = '"tripjson": {"Row":' +tripJson+'}'
                print('thread:'+ str(threadid)+' preDeviceID:'+str(preDeviceID)+ ' preTripID:'+str(preTripID))
                df_combined = df_combined.append(pd.Series(tripJson,index=df_combined.columns), ignore_index=True)
                preDeviceID = curDeviceID
                preTripID = curTripID
                #New trip
                list_tmp = []
                list_tmp.append(row)
             else:
                 list_tmp.append(row)
        print('thread:' + str(threadid) + ' preDeviceID:' + str(preDeviceID) + ' preTripID:' + str(preTripID))

        if len(list_tmp) > 0:
            df_tmp = pd.DataFrame().append(list_tmp)
            tripJson = df_tmp.to_json(orient='records').replace("\/", "/")
            tripJson = '"tripjson": {"Row":' + tripJson + '}'
            print('preDeviceID:'+str(preDeviceID)+ ' preTripID:'+str(preTripID))
            df_combined = df_combined.append(pd.Series(tripJson, index=df_combined.columns), ignore_index=True)

        print(savepath)
        #Save the dataframe to dataset
        df_combined.to_json(savepath)
        print('thread:'+str(threadid)+' end:' + str(datetime.datetime.now()))
        returnValue = True
        end = time.clock()
        #Caculate the data generation speed
        print(str(end - start))
    except Exception as ex:
        print(ex)
    return returnValue
if __name__ == "__main__":
    path = '/home/google_cloudadmins_lexisnexisris/telematics/testV2_enhance.csv'
    #path = '/home/google_cloudadmins_lexisnexisris/telematics/testV2_clean.csv'
    savepath = '/home/google_cloudadmins_lexisnexisris/telematics/testV2_combineTrip.json'
    combineDataToJson(1, path, savepath, 1, 100)
