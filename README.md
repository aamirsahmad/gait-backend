# backend

## How to run?
Define you port

HTTP_SERVER_PORT = 8094 (default)

1. `pip3 install -r requirements.txt`

2. `python3 app.py`

3. connect to ws://hostAddress:Port/gait

## Input data format/schema
### event type: connected
```json
{
  "event": "connected"
}
```

### event type: start
```json
{
  "event": "start",
  "gait": {
    "payload": "timestamp, x, y, z"
  }
}
```

### event type: stop
```json
{
  "event": "stop",
  "gait": {
    "payload": "timestamp, x, y, z"
  }
}
```