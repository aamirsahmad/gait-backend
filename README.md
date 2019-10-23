# backend

## How to run?
Define you port

HTTP_SERVER_PORT = 8094 (default)

1. `pip3 install -r requirements.txt`

2. `python3 app.py`

3. connect to ws://hostAddress:Port/gait

## Input data format/schema
### event type: connect
```json
{
  "event": "connect",
  "data": {
    "uuid": "bBNhR0kYNXYoE0q6"
  }
}
```

### event type: gait
```json
{
  "event": "gait",
  "data": {
    "uuid": "bBNhR0kYNXYoE0q6",
    "gait": ["timestamp0, x0, y0, z0", "timestamp1, x1, y1, z1" ]
  }
}
```

### event type: stop
```json
{
  "event": "stop",
  "data": {
    "uuid": "bBNhR0kYNXYoE0q6"
  }
}
```