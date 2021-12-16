# -*- coding: utf-8 -*-
from fastapi import FastAPI
from fastapi.routing import Request
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app="daprd:app", port=3001, host="0.0.0.0")
