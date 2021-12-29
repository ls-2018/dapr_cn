# -*- coding: utf-8 -*-
import random

from fastapi import FastAPI
from fastapi.routing import Request, Response
from starlette_exporter import PrometheusMiddleware
from starlette_exporter import handle_metrics

app = FastAPI(title="daprd")
app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics", handle_metrics)


@app.get("/health")
async def get(req: Request):
    return {
        "state": "ok",
    }


@app.get("/dapr/config")
async def get(req: Request):
    # 没有数据传过来
    return {
        "entities": [],
        "actorIdleTimeout": "",  # 60m
        "actorScanInterval": "",  # 30s
        "drainOngoingCallTimeout": "",  # 60s
        "drainRebalancedActors": False,
        "reentrancy": {
            'enabled': False,
            'maxStackDepth': 0  # 32
        },
        "remindersStoragePartitions": 0,
    }


@app.get('/dapr/subscribe')
async def sub(req: Request):
    # print(await req.json())
    return [
        {
            "pubsubname": "redis-pubsub",
            "topic": "topic-a"
        }
    ]


@app.post('/dsstatus')
async def sub(req: Request):
    print(await req.json())

    return {
        'status': 'SUCCESS',
    }


@app.post('/myevent')
async def myevent(req: Request):
    print(await req.json())
    if random.random() > 0.5:
        return Response("200", status_code=200)
    else:
        return Response("500", status_code=500)


# 必须返回三者之一 | 不写 status
# 	Success AppResponseStatus = "SUCCESS" | ""
# 	Retry AppResponseStatus = "RETRY"
# 	Drop AppResponseStatus = "DROP"

@app.post('/post')
async def post(req: Request):
    body = await req.json()
    print(body)
    return body


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app="daprd:app", port=3001, host="0.0.0.0")
