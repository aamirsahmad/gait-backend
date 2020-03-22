#!/usr/bin/env python
# from pyspark.streaming import StreamingContext
import sys
import requests
import socket
import pyspark as ps
from pyspark import SparkConf, SparkContext, SparkFiles
import pyspark.streaming as pss
from pyspark.sql import Row, SQLContext
from pyspark.sql import SparkSession

import math
from collections import OrderedDict
# from tensorflow.compat.v1.gfile import FastGFile
# import tensorflow as tf
from tensorflow import keras

from io import StringIO
import pandas as pd
import numpy as np
import os

import data_transformation as dt
API_SERVICE_URL = str(os.getenv('API_SERVICE_URL'))
API_SERVICE_PORT = str(os.getenv('API_SERVICE_PORT'))
SPARK_SOCKET_PORT = str(os.getenv('SPARK_SOCKET_PORT'))

back_end_url = "http://" + API_SERVICE_URL + ':' + API_SERVICE_PORT + '/add_inference'


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
    if (len(peak_map) < 5):
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

    sys.path.insert(0, SparkFiles.getRootDirectory())

    s_context.addFile('./model/cnn_modell.h5')    
    s_context.addFile("./data_transformation.py")
    # TODO: add logger to spark

    # use spark context to create the stream context
    # 5 seconds ensure that we get two overlapping samples of 4 seconds
    interval_seconds = 5
    s_stream_context = pss.StreamingContext(s_context, interval_seconds)
    s_stream_context.checkpoint("checkpoint_TSA")

    # with tf.gfile.GFile('./frozenInferenceGraphIdentification.pb', "rb") as f:
    #     model_data = f.read()


    # model_data_bc = s_context.broadcast(model_data)
    # model_data_bc = s_context.broadcast(loaded_model)

    # connect to port 9009 i.e. twitter-client
    print(API_SERVICE_URL + ' ' + SPARK_SOCKET_PORT)
    socket_ts = s_stream_context.socketTextStream(API_SERVICE_URL, int(SPARK_SOCKET_PORT))

    print("\n################################\n")

    line = socket_ts.flatMap(lambda line: line.split("\n"))
    gait = line.map(lambda g: (getUserId(g).strip(), g.strip()))
    gaitByUserId = gait.groupByKey()

    sortedGaitByUserId = gaitByUserId.transform(lambda foo: foo
                                                .sortBy(lambda x: (x[0])))

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

    # segmentedData.pprint()

    # DO NOT CHANGE THE LOCATION OF THIS FUNCTION
    def infer(data_rdd):
        # print("ATTEMPTING DEEP LEARNING")
        try:
            datas = data_rdd.collect()
            if len(datas) > 0:
                # print("INSIDE TRY BEFORE WITH")
                # with tf.Graph().as_default() as graph:
                #     graph_def = tf.GraphDef()
                #     graph_def.ParseFromString(model_data_bc.value)
                #     tf.import_graph_def(graph_def, name="prefix")
                # print("INSIDE TRY AFTER WITH")
                # x = graph.get_tensor_by_name('prefix/Placeholder:0')
                # y = graph.get_tensor_by_name('prefix/Softmax:0')

                for data in datas:
                    for id_xyz in data:
                        if id_xyz:
                            id = id_xyz[0]
                            dummy_axis = "0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00 0.000000000000000000e+00"
                            input_signals = []
                            input_signals.extend(id_xyz[1:])
                            for i in range(3):
                                input_signals.append(dummy_axis)

                            X_signals = []
                            for each in input_signals:
                                X_signals.append(
                                    [np.array(cell, dtype=np.float32) for cell in [each.strip().split(' ')]])
                            X_test = np.transpose(np.array(X_signals), (1, 2, 0))

                            from pyspark import SparkFiles
                            from tensorflow.keras.models import load_model
                            path = SparkFiles.get('cnn_modell.h5')
                            model = load_model(path)
                            print("Loaded model from disk")
                            preds = model.predict(X_test)
                            for p in preds:
                                inferred_user_id = str(np.argmax(p) + 1)
                                results = {'confidency': str(np.amax(p)), 'inferred_user_id': inferred_user_id, 'actual_user_id': str(id)}
                                print(results)
                                requests.post(back_end_url, json=results)
                            # with tf.Session(graph=graph) as sess:
                            #     y_out = sess.run(y, feed_dict={
                            #         x: X_test
                            #     })

                            #     for each in y_out:
                            #         inferred_user_id = str(np.argmax(each) + 1)
                            #         confidency = str(np.amax(each))
                            #         actual_user_id = str(id)
                            #         results = {'confidency': confidency, 'inferred_user_id': inferred_user_id,
                            #                    'actual_user_id': actual_user_id}
                            #         print(results)
                            #         requests.post(back_end_url, json=results)
        except:
            e = sys.exc_info()
            print("Error: %s" % e)

    segmentedData.foreachRDD(infer)

    # start the streaming computation
    s_stream_context.start()
    try:
        # wait for the streaming to finish
        s_stream_context.awaitTermination()
    except KeyboardInterrupt:
        print("\nSpark shutting down\n")


if __name__ == "__main__":
    main()
