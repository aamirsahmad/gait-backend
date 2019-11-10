import base64
import json
import logging
import queue
import boto3
import os

from flask import Flask
from flask_sockets import Sockets
from botocore.exceptions import ClientError

app = Flask(__name__)
sockets = Sockets(app)

gunicorn_logger = logging.getLogger("gunicorn.error")
app.logger.handlers = gunicorn_logger.handlers
app.logger.setLevel(gunicorn_logger.level)

HTTP_SERVER_PORT = 8094
ACCESS_KEY = os.getenv('ACCESS_KEY')
SECRET_KEY = os.getenv('SECRET_KEY')

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
            app.logger.info("Gait message: {}".format(message))
            dataPoints = data['data']['gait']
            for dataPoint in dataPoints:
                if dataPoint:
                    app.logger.info("dataPoint={}".format(dataPoint))
                    current = str(dataPoint)
                    q.put(current)
                    verifyOrder(last, current)
                    last = current

        if data['event'] == "stop":
            uuid = data['data']['uuid']
            app.logger.info("Stop Message received: {} for uuid={}".format(message, uuid))
            app.logger.info("Now saving CSV file")
            with open(uuid + ".csv", 'a') as file:
                while (not q.empty()):
                    file.write(q.get() + '\n')
            app.logger.info("Gait data saved as {}.csv".format(uuid))
            upload_file(uuid + ".csv", "gait-poc-bucket", uuid)
            break

        message_count += 1

    app.logger.info("Connection closed. Received a total of {} messages".format(message_count))


# adapted from https://docs.aws.amazon.com/code-samples/latest/catalog/python-s3-upload_file.py.html
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
    s3_client = boto3.client('s3', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    try:
        app.logger.info("Now uploading Gait data to S3 Bucket.")
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        app.logger.warning("Error while saving gait data to S3 Bucket.")
        return False
    app.logger.info("Gait data saved as {}.csv to S3 Bucket".format(uuid))
    return True


def verifyOrder(last, current):
    if last == "":
        return
    lastTimeStamp = int(last.split(",")[0])
    currentTimeStamp = int(current.split(",")[0])
    if currentTimeStamp < lastTimeStamp:
        app.logger.warning("Inconsistent order")


def main():
    from gevent import pywsgi
    from geventwebsocket.handler import WebSocketHandler
    server = pywsgi.WSGIServer(('', HTTP_SERVER_PORT), app, handler_class=WebSocketHandler)
    print("Server listening on: http://localhost:" + str(HTTP_SERVER_PORT))
    server.serve_forever()


if __name__ == '__main__':
    main()