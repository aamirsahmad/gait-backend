import datetime
import json
import logging
import os
import queue
import socket
import boto3

from multiprocessing import Process
from botocore.exceptions import ClientError
from flask import Flask, redirect, request, jsonify, abort
from flask_cors import CORS
from flask_sockets import Sockets
from collections import OrderedDict


# Environment variables
HTTP_SERVER_PORT = 8094
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')
SPARK = os.getenv('SPARK')

# Constants
IP = '0.0.0.0'
PORT = 9009

# Flask
app = Flask(__name__)
sockets = Sockets(app)
CORS(app)

# Gunicorn Logger
gunicorn_logger = logging.getLogger("gunicorn.error")
app.logger.handlers = gunicorn_logger.handlers
app.logger.setLevel(gunicorn_logger.level)

spark_conn = None

# A dictionary of queues to feed the front-end. Each user ID is the key 
# and their corresponding value is the queue containing their data.
user_dicts = OrderedDict()

# We will use this to fetch the name of the user using the id received from Spark.
id_name_dict = {1: "Noor", 2: "Talha", 3: "Aamir"}


if SPARK == 'true':
    app.logger.debug("Starting...")
    app.logger.debug("SPARK SOCKET: " + IP + ":" + str(PORT))
    # Spark socket connection
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((IP, PORT))
    s.listen(1)
    app.logger.debug("Waiting for TCP connection...")
    spark_conn, addr = s.accept()
    app.logger.debug("Connected to Spark")


@app.route('/')
def index():
    return 'Gait Identification & Analysis'


@app.route('/docs')
def docs():
    return redirect("https://eecs-gia.gitlab.io/docs/", code=302)


@sockets.route('/gait')
def echo(ws):
    app.logger.info("Connection accepted")
    message_count = 0
    q = queue.Queue()
    while not ws.closed:
        last = ""
        message = ws.receive()
        if message is None:
            app.logger.info("No message received...")
            continue

        # Messages are a JSON encoded string
        data = json.loads(message)

        # Using the event type you can determine what type of message you are receiving
        if data['event'] == "connect":
            uuid = data['data']['uuid']
            app.logger.info("Connected Message received: {} for uuid={}".format(message, uuid))

        if data['event'] == "gait":
            # app.logger.info("Gait message: {}".format(message))
            if SPARK == 'true':
                sendToSpark(message)
            dataPoints = data['data']['gait']
            for dataPoint in dataPoints:
                if dataPoint:
                    # app.logger.info("dataPoint={}".format(dataPoint))
                    current = str(dataPoint)
                    q.put(current)
                    current_graph = current.split(", ")
                    # app.logger.info("current={}".format(currentgraph))
                    user_id = current_graph[1]
                    timestamp = current_graph[2]
                    xyz_val = current_graph[6]
                    pair = (xyz_val, timestamp,)
                    global user_dicts
                    global counter
                    # app.logger.info("DEBUG: {}".format(counter))
                    # If the user_id already exists in the dictionary put the data in their queue.
                    # If not i.e. it is  the first time in this session we are putting their data to the queue,
                    # then create the dictionary entry for that user and place the data there.
                    if user_id in user_dicts:
                        # Puts every 50th data to the users queue.
                        counter = user_dicts[user_id]['counter']
                        # if counter % 25 == 0:
                        user_dicts[user_id]["queue"].put(pair)
                        user_dicts[user_id]['counter'] += 1
                    else:
                        user_dicts[user_id] = {"user_id": user_id, "queue": queue.Queue(), "counter": 1,
                                               "inferences": []}
                        user_dicts[user_id]["queue"].put(pair)
                    verifyOrder(last, current)
                    last = current

        if data['event'] == "stop":
            del user_dicts[data['data']['userid']]
            uuid = data['data']['uuid']
            app.logger.info("Stop Message received: {} for uuid={}".format(message, uuid))
            app.logger.info("Now saving CSV file")
            with open(uuid + ".csv", 'a') as file:
                while not q.empty():
                    file.write(q.get() + '\n')
            app.logger.info("Gait data saved as {}.csv".format(uuid))
            upload_file(uuid + ".csv", "gait-poc-bucket", uuid)
            break

        message_count += 1

    app.logger.info("Connection closed. Received a total of {} messages".format(message_count))


# Returns the list of online users for the front-end.
@app.route('/get_users')
def get_users():
    global user_dicts
    # app.logger.info("DEBUG {}".format(user_dicts))
    # app.logger.info("DEBUG {}".format(queue_dicts))
    # app.logger.info("DEBUG {}".format(list(queue_dicts.keys())))
    user_ids = list(user_dicts.keys())
    data = {"user_ids": user_ids}
    # app.logger.info("DEBUG {}".format(user_ids))
    return jsonify(data), 200


# Returns the data at the front of the user's queue. Request must include the id of the user the we are
# requesting gait data for.
@app.route('/get_queue_http')
def get_queue_http():
    global user_dicts
    user_id = str(request.args.get("user_id", type=str))

    # If there is no such online user than return a 403 error.
    # If the requested user_id is currently online then return the data at the front of the queue.
    if user_id not in user_dicts:
        abort(403)
    else:
        # app.logger.info("DEBUG: {}".format(user_dicts))
        user_queue = user_dicts[user_id]["queue"]
        # user_queue.queue.clear()
        res = user_queue.get()
        app.logger.info("DEBUG: {}".format(user_queue.qsize()))
        return jsonify(res), 200


def gait_data_generator(user_queue):
    while True:
        yield (json.dumps(user_queue.get()))


# Allows Spark to add inferences for users.
@app.route('/add_inference', methods=['POST'])
def add_inference():
    # Expecting an array of objects where each object is like the following.
    # {"confidency": 0.98, "inferred_user_id": 1, "actual_user_id": 1}
    global user_dicts
    incoming_data = request.get_json()
    # app.logger.info("DEBUG: {}".format(user_dicts))
    app.logger.info("add_inference() data=: {}".format(incoming_data))
    if incoming_data["actual_user_id"] in user_dicts:
        inference = {
                        "confidency": float(incoming_data['confidency']), 
                        "inferred_user_id": int(incoming_data["inferred_user_id"]),
                        "inferred_users_name": id_name_dict[int(incoming_data["inferred_user_id"])]
                     }
        user_dicts[incoming_data["actual_user_id"]]["inferences"].append(inference)
        resp = jsonify(success=True)
        return resp, 200
    else:
        abort(403)


# Allows front-end to get inferences for users.
@app.route('/get_inference')
def get_inference():
    actual_user_id = str(request.args.get("user_id", type=str))
    global user_dicts
    app.logger.info("get_inference(), users : {}".format(user_dicts))
    if actual_user_id in user_dicts:
        return jsonify(items=user_dicts[actual_user_id]["inferences"]), 200
    else:
        abort(403)


# This is what we had initially. However, it is easier to use http requests to fetch data so we ditched this. But,
# maybe we will switch back to this after discussing.
# get_queue listens for a user_id and once it gets once starts broadcasting the gait data for that user.
# If the user_id is not online then it returns 403.
@sockets.route('/get_queue')
def get_queue(ws):
    global user_dicts
    user_id = ws.receive()
    # app.logger.info("DEBUG {}".format(ws))
    # app.logger.info("DEBUG {}".format(user_id))
    if user_id in user_dicts:
        user_dicts[user_id]["queue"].queue.clear()
        # app.logger.info("DEBUG SOCKET STARTED FOR {}".format(user_id))
        last_sent_timestamp = float("-Inf")
        while True:  # user_id in queue_dicts
            current_data = user_dicts[user_id]["queue"].get()
            data = {"xyz": current_data[0], "timestamp": current_data[1]}
            ws.send(json.dumps(data))
                # app.logger.info("DEBUG TIME {} XYZ {}".format(user_id, str(ts) ))
    else:
        abort(403)


# def get_queue_fork(queue, ws):
#     global user_dicts
#     user_id = ws.receive()
#     # app.logger.info("DEBUG {}".format(ws))
#     # app.logger.info("DEBUG {}".format(user_id))
#     if user_id in user_dicts:
#         user_dicts[user_id]["queue"].queue.clear()
#         # app.logger.info("DEBUG SOCKET STARTED FOR {}".format(user_id))
#         last_sent_timestamp = float("-Inf")
#         while True:  # user_id in queue_dicts
#             current_data = user_dicts[user_id]["queue"].get()
#             if last_sent_timestamp + 200 > float(current_data[1]):
#                 continue
#             else:
#                 data = {"xyz": current_data[0], "timestamp": current_data[1]}
#                 ws.send(json.dumps(data))
#                 last_sent_timestamp = float(current_data[1])
#                 ts = datetime.datetime.fromtimestamp(float(current_data[1]) / 1000)
#                 # app.logger.info("DEBUG TIME {} XYZ {}".format(user_id, str(ts) ))
#     else:
#         queue.append(403)

# Simple API route to check availability of the back-end.
@app.route('/ping')
def ping():
    resp = jsonify(success=True)
    return resp, 200


def upload_file(file_name, bucket, uuid, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then same as file_name
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    if ACCESS_KEY is None and SECRET_KEY is None:
        return;

    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    try:
        app.logger.info("Now uploading Gait data to S3 Bucket.")
        response = s3_client.upload_file(file_name, bucket, object_name)
        app.logger.info("Gait data saved as {}.csv to S3 Bucket".format(uuid))
    except ClientError as e:
        logging.error(e)
        app.logger.warning("Error while saving gait data to S3 Bucket.")
        return False
    return True


def verifyOrder(last, current):
    if last == "":
        return
    lastTimeStamp = int(last.split(",")[0])
    currentTimeStamp = int(current.split(",")[0])
    if currentTimeStamp < lastTimeStamp:
        app.logger.warning("Inconsistent order")


# https://stackoverflow.com/questions/35068363/how-to-get-around-a-websocket-call-blocking-other-websocket-calls
def sendToSpark(message):
    p = Process(target=sendToSparkHelper, args=(message,))
    p.start()
    # data = json.loads(message)
    # gait = data["data"]["gait"]
    # for datapoint in gait:
    #     conn.send(str.encode(datapoint + '\n'))


def sendToSparkHelper(message):
    data = json.loads(message)
    gait = data["data"]["gait"]
    for datapoint in gait:
        spark_conn.send(str.encode(datapoint + '\n'))


def main():
    from gevent import pywsgi
    from geventwebsocket.handler import WebSocketHandler
    server = pywsgi.WSGIServer(('', HTTP_SERVER_PORT), app, handler_class=WebSocketHandler)
    app.logger.debug("Server listening on: http://localhost:" + str(HTTP_SERVER_PORT))
    server.serve_forever()


if __name__ == '__main__':
    main()
