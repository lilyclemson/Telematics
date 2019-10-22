import csv
import json
import numpy as np
import os
import pandas as pd


def CleanDataAndSave(path, savepath):
    df = pd.read_csv(path,low_memory=False)

    print(df.shape)

    df_clean = df[(True^df['tripID'].isin(['tripID']))]
    print(df_clean.shape)
    df_clean.to_csv(savepath, index = None, header=True)

if __name__ == "__main__":
    path = '/home/ec2-user/telematics/testV2test.csv'
    #path = '/home/ec2-user/telematics/testV2_13000.csv'
    savepath = '/home/ec2-user/telematics/testV2_clean.csv'
    CleanDataAndSave(path,savepath)



