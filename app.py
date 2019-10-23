import base64
import json
import logging
import queue

from flask import Flask
from flask_sockets import Sockets

app = Flask(__name__)
sockets = Sockets(app)

HTTP_SERVER_PORT = 8094

@sockets.route('/gait')
def echo(ws):
    app.logger.info("Connection accepted")
    message_count = 0
    q = queue.Queue()
    while not ws.closed:
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
                print(dataPoint)
                q.put(str(dataPoint))

        if data['event'] == "stop":
            uuid = data['data']['uuid']
            app.logger.info("Stop Message received: {} for uuid={}".format(message, uuid))
            app.logger.info("Now saving CSV file to disk")
            with open(uuid + ".csv", 'a') as file:
                while (not q.empty()):
                    file.write(q.get() + '\n')
            app.logger.info("Gait data saved as {}.csv".format(uuid))            
            break
        
        message_count += 1

    app.logger.info("Connection closed. Received a total of {} messages".format(message_count))

if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)
    from gevent import pywsgi
    from geventwebsocket.handler import WebSocketHandler

    server = pywsgi.WSGIServer(('', HTTP_SERVER_PORT), app, handler_class=WebSocketHandler)
    print("Server listening on: http://localhost:" + str(HTTP_SERVER_PORT))
    server.serve_forever()