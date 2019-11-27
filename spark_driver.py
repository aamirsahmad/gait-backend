#!/usr/bin/env python

import sys
import requests
import socket
import pyspark as ps
from pyspark import SparkConf, SparkContext
import pyspark.streaming as pss
from pyspark.sql import Row, SQLContext
# import json # parse incoming data to json and then access fields


def main():
    # start connection
    # configure spark instance to default
    config = SparkConf()
    config.setAppName("Gait-Realtime-Analysis")
    s_context = SparkContext(conf=config)
    s_context.setLogLevel("ERROR")

    # use spark context to create the stream context
    interval_seconds = 2
    s_stream_context = pss.StreamingContext(s_context, interval_seconds)
    s_stream_context.checkpoint("checkpoint_TSA")

    # connect to port 9009 i.e. twitter-client
    socket_ts = s_stream_context.socketTextStream("gait", 9009)

    print("\n################################\n")

    line = socket_ts.flatMap(lambda line: line.split("\n"))
    gait = line.map(lambda g: (getUserId(g).strip(), g.strip()))
    gaitByUserId = gait.groupByKey()
    gaitByUserId.pprint()

    # start the streaming computation
    s_stream_context.start()
    try:
        # wait for the streaming to finish
        s_stream_context.awaitTermination()
    except KeyboardInterrupt:
        print("\nSpark shutting down\n")

def getUserId(line):
    return str(line.split(",")[1])

if __name__ == "__main__":
    main()
