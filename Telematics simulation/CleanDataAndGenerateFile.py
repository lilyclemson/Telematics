import csv
import json
import numpy as np
import os
import pandas as pd

def CleanDataAndSave(path, savepath):
    #load source file
    df = pd.read_csv(path,low_memory=False)

    print(df.shape)
    #Clean the dataset which tripID is null
    df_clean = df[(True^df['tripID'].isin(['tripID']))]
    print(df_clean.shape)
    #Save dataset
    df_clean.to_csv(savepath, index=None, header=True)

if __name__ == "__main__":
    #path = '/home/google_cloudadmins_lexisnexisris/telematics/testV2test.csv'
    #path = '/home/google_cloudadmins_lexisnexisris/telematics/testV2_13000.csv'
    path = '/home/google_cloudadmins_lexisnexisris/telematics/v2_1.csv'
    savepath = '/home/google_cloudadmins_lexisnexisris/telematics/testV2_clean.csv'
    CleanDataAndSave(path,savepath)



