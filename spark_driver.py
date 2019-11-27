#!/usr/bin/env python
# from pyspark.streaming import StreamingContext
import sys
import requests
import socket
import pyspark as ps
from pyspark import SparkConf, SparkContext,SparkFiles
import pyspark.streaming as pss
from pyspark.sql import Row, SQLContext
from pyspark.sql import SparkSession

import math
from collections import OrderedDict 

from io import StringIO
import pandas as pd
import os

import data_transformation as dt
# import json # parse incoming data to json and then access fields

# from pyspark.conf import SparkConf
# from pyspark.sql import SparkSession

# NOTE: The print statements in this module are crucial for debugging (don't remove)
# Consider replacing them with logger INFO/WARN 


def partition_mapper_func(partition_map):
    # 
    # print(type(partition_map)) #<class 'map'>
    res = []
    for tuple_data in partition_map:
        # print(tuple_data)
        # print(type(tuple_data)) # <class 'tuple'>
        for str_data in tuple_data[1:]:
            # 'ResultIterable' object str_data
            # ASSERT : samples == 2 if job interval == 5 sec
            dl_sample_list = data_processing_driver(str_data) 
            res.append(dl_sample_list)
            # sample = find_all_peaks_in_partition(str_data)
            # res.append(sample)
            # print(type(str_data)) # <class 'str'>
            # for line in str_data:
                # print(type(line))  # <class 'str'>
                # print(line)
    return res


def data_processing_driver(str_data):
    dl_sample_list = []
    peak_map = dt.find_all_peaks_in_partition(str_data)

    # print('peak_map length is : ' + str(len(peak_map)))

    # print('str data length is ' + str(len(str_data)))
    if(len(peak_map) < 5):
        return dl_sample_list
    # else:
    #     for (k,v) in peak_map.items():
    #         print('peak map items are:')
    #         print(k,v)

    samples_list = dt.gait_segmentation(str_data, peak_map)

    
    for sample in samples_list:
        # print('here is a gait sample after segmentation')
        # print(sample)
        dl_ready_sample = dt.sampling_and_interpolation(sample)
        # print('GAIT data after sampling and linear interpolation')
        # print(dl_ready_sample)
        dl_sample_list.append(dl_ready_sample)

    # print('total number of samples : ' + str(len(dl_sample_list)))

    return dl_sample_list


def getUserId(line):
    return str(line.split(",")[1])

def main():
    # start connection
    # configure spark instance to default
    global s_context
    global Logger
    global mylogger
    global s_context
    config = SparkConf()
    config.setAppName("Gait-Realtime-Analysis")
    s_context = SparkContext(conf=config)
    s_context.setLogLevel("ERROR")

    sys.path.insert(0,SparkFiles.getRootDirectory())

    s_context.addFile("./data_transformation.py")
    # TODO: add logger to spark

    # use spark context to create the stream context
    # 5 seconds ensure that we get two overlapping samples of 4 seconds
    interval_seconds = 5
    s_stream_context = pss.StreamingContext(s_context, interval_seconds)
    s_stream_context.checkpoint("checkpoint_TSA")

    # connect to port 9009 i.e. twitter-client
    socket_ts = s_stream_context.socketTextStream("gait", 9009)

    print("\n################################\n")

    line = socket_ts.flatMap(lambda line: line.split("\n"))
    gait = line.map(lambda g: (getUserId(g).strip(), g.strip()))
    gaitByUserId = gait.groupByKey()


    sortedGaitByUserId = gaitByUserId.transform  (lambda foo:foo
    .sortBy(lambda x:( x[0])) )

    # sortedGaitByUserId = gaitByUserId.sortByKey()

    #     author_counts_sorted_dstream = author_counts.transform(\
    #   (lambda foo:foo\
    #    .sortBy(lambda x:( -x[1])) )
    #    )
    # author_counts_sorted_dstream.pprint()

    # sortedGaitByUserId.foreachRDD(another)

    segmentedData = sortedGaitByUserId.mapPartitions(partition_mapper_func)

    # x = cogrouped.mapValues(iterate)
    # for e in x.collect():
    #     print (e)

    segmentedData.pprint()

    # start the streaming computation
    s_stream_context.start()
    try:
        # wait for the streaming to finish
        s_stream_context.awaitTermination()
    except KeyboardInterrupt:
        print("\nSpark shutting down\n")



if __name__ == "__main__":
    main()
