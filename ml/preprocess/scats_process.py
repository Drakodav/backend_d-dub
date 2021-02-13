import os
import numpy as np
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import time

__file__ = Path().cwd()
outdir = os.path.join(__file__, 'output')
scatsFilesPath = os.path.join(__file__, 'data')
finalScatsPath = os.path.join(outdir, 'processed_scats.csv')


def process_scats_data():
    ''' Gets the dataset for this assignment
    Returns: dataset - pandas dataframe
    '''

    start = time.time()
    print('starting scats_processing')

    scatsFiles = os.listdir(scatsFilesPath)

    scats_data = []
    sites_data = None

    for sFile in scatsFiles:
        datasetPath = os.path.join(scatsFilesPath, sFile)
        if 'scats_volume_2020' in sFile:
            data = pd.read_csv(datasetPath, sep=",")
            scats_data.append(data)

            print(sFile, 'time: {}s'.format(round(time.time() - start)))
        elif 'sites.csv' == sFile:
            sites_data = pd.read_csv(datasetPath, sep=",", na_values='NULL')
            print(sFile, 'time: {}s'.format(round(time.time() - start)))

    # merge the scats datsets
    scats_dataset = pd.concat(scats_data, ignore_index=True)

    # sites changes
    sites_data['Site'] = sites_data['SiteID'].fillna(0).astype(int)
    del sites_data['SiteID']

    # create our dataset
    dataset = pd.merge(scats_dataset, sites_data, how='left', on=['Site'])

    # scats data changes
    dataset['Site_Description'] = dataset['Site_Description_Lower']
    dataset['Region'] = dataset['Region_y']
    del dataset['Weighted_Avg']
    del dataset['Weighted_Var']
    del dataset['Weighted_Std_Dev']
    del dataset['Site_Description_Cap']
    del dataset['Site_Description_Lower']
    del dataset['Region_x']
    del dataset['Region_y']
    del dataset['Detector']

    # convert to radians for better performance
    dataset["Lat"] = np.radians(dataset["Lat"])
    dataset["Long"] = np.radians(dataset["Long"])
    dataset = dataset[(dataset['Lat'] != 0.0) | (dataset['Long'] != 0)]

    dataset = dataset.dropna()  # delete any null values, I dont care at this point

    # we have to aggregate the detectors, this will significantly reduce the size of our dataset
    # there are about 31 detectors per site, for every hour. this makes for lots of duplicate data
    cols = {}
    for col in list(dataset.columns):
        if col in ['Sum_Volume', 'Avg_Volume', 'Site', 'End_Time']:
            continue
        cols[col] = 'first'

    # group the data to aggregate the sum_volume and avg_volume
    grouped_data = dataset.groupby(['End_Time', 'Site'], as_index=False).agg(
        {'Sum_Volume': ['sum'], 'Avg_Volume': ['sum'], **cols})
    grouped_data.columns = dataset.columns
    dataset = grouped_data

    print('grouped data, time: {}s'.format(round(time.time() - start)))

    # convert end time to more meaningful values
    dows = []
    days = []
    hours = []
    months = []

    def apply_time(t):
        dtime = datetime.strptime(t, '%Y-%m-%d %H:%M:%S.%f')
        dows.append(dtime.weekday())
        hours.append(dtime.hour)
        months.append(dtime.month)
        days.append(dtime.day)
        return t

    dataset['End_Time'].apply(apply_time)
    dataset['dow'] = dows
    dataset['hour'] = hours
    dataset['month'] = months
    dataset['day'] = days
    del dataset['End_Time']

    dataset.columns = ['site', 'sum_volume', 'avg_volume', 'lat',
                       'lon', 'site_desc', 'region', 'dow', 'hour', 'month', 'day']

    print('processed data, time: {}s'.format(round(time.time() - start)))

    dataset.to_csv(finalScatsPath, index=False, header=True)
    print('finished, time: {}s'.format(round(time.time() - start)))
    return


if __name__ == "__main__":
    if not os.path.exists(outdir):
        os.mkdir(outdir)

    # execute only if run as a script
    process_scats_data()
