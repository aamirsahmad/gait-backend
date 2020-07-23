# backend

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


## How to run alongside Apache Spark with two Docker containers?
> Requires AWS IAM user with permissions for S3 actions.
```
// gait app.py
docker run -it -v "$PWD:/app" --name gait -w /app -p 8095:8095 -p 9009:9009 python bash
export ACCESS_KEY=<AWS_IAM_ACCESS_KEY>
export SECRET_KEY=<AWS_IAM_SECRET_KEY>
pip3 install -r requirements.txt
gunicorn -k flask_sockets.worker app:app --log-level DEBUG --bind 0.0.0.0:8095

// spark driver 
docker run -it -v "$PWD:/app" --link gait:gait eecsyorku/eecs4415
pip3 install -r requirements.txt
spark-submit spark_driver.py
```


## Using Docker-compose (the easy way)
1. Specify the environment variables in .env.dev for both web and spark
2. `docker-compose up`