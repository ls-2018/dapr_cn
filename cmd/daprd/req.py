import json

import requests

url = 'http://127.0.0.1:3001/post'
dapr_url = "http://localhost:3500/v1.0/invoke/dp-61c2cb20562850d49d47d1c7-executorapp/method/health"

# dapr_url = "http://localhost:3500/v1.0/healthz"

# res = requests.post(dapr_url, json.dumps({'a': random.random() * 1000}))
# res = requests.get(dapr_url, )
#
#
#
# print(res.text)
# print(res.status_code)
# INFO[0000] GET----/v1.0/state/{storeName}/{key}
# INFO[0000] GET----/v1.0/secrets/{secretStoreName}/bulk
# INFO[0000] GET----/v1.0/secrets/{secretStoreName}/{key}
# INFO[0000] GET----/v1.0/actors/{actorType}/{actorId}/method/{method}
# INFO[0000] GET----/v1.0/actors/{actorType}/{actorId}/state/{key}
# INFO[0000] GET----/v1.0/actors/{actorType}/{actorId}/reminders/{name}
# INFO[0000] GET----/v1.0/metadata
# INFO[0000] GET----/v1.0/healthz
# INFO[0000] GET----/v1.0/healthz/outbound
# INFO[0000] POST----/v1.0/state/{storeName}
# INFO[0000] POST----/v1.0/state/{storeName}/bulk
# INFO[0000] POST----/v1.0/state/{storeName}/transaction
# INFO[0000] POST----/v1.0-alpha1/state/{storeName}/query
# INFO[0000] POST----/v1.0/publish/{pubsubname}/{topic:*}
# INFO[0000] POST----/v1.0/actors/{actorType}/{actorId}/state
# INFO[0000] POST----/v1.0/actors/{actorType}/{actorId}/method/{method}
# INFO[0000] POST----/v1.0/actors/{actorType}/{actorId}/reminders/{name}
# INFO[0000] POST----/v1.0/actors/{actorType}/{actorId}/timers/{name}
# INFO[0000] POST----/v1.0/shutdown
# INFO[0000] POST----/v1.0/bindings/{name}
# INFO[0000] PUT----/v1.0/state/{storeName}
# INFO[0000] PUT----/v1.0/state/{storeName}/bulk
# INFO[0000] PUT----/v1.0/state/{storeName}/transaction
# INFO[0000] PUT----/v1.0-alpha1/state/{storeName}/query
# INFO[0000] PUT----/v1.0/publish/{pubsubname}/{topic:*}
# INFO[0000] PUT----/v1.0/actors/{actorType}/{actorId}/state
# INFO[0000] PUT----/v1.0/actors/{actorType}/{actorId}/method/{method}
# INFO[0000] PUT----/v1.0/actors/{actorType}/{actorId}/reminders/{name}
# INFO[0000] PUT----/v1.0/actors/{actorType}/{actorId}/timers/{name}
# INFO[0000] PUT----/v1.0/metadata/{key}
# INFO[0000] PUT----/v1.0/bindings/{name}
# INFO[0000] DELETE----/v1.0/state/{storeName}/{key}
# INFO[0000] DELETE----/v1.0/actors/{actorType}/{actorId}/method/{method}
# INFO[0000] DELETE----/v1.0/actors/{actorType}/{actorId}/reminders/{name}
# INFO[0000] DELETE----/v1.0/actors/{actorType}/{actorId}/timers/{name}
# INFO[0000] *----/v1.0/invoke/{id}/method/{method:*}
# INFO[0000] *----/{method:*}


# print(requests.get('http://localhost:3500/v1.0/state/redis-statestore/b'))
# print(requests.post('http://localhost:3500/v1.0/state/redis-statestore', json.dumps(
#     [
#         {
#             "key": '1a',
#             'value': 1232,
#             'etag': "2",
#             # "metadata": {
#             #     "ttlInSeconds": "5"
#             # }
#         },
#     ]
# )).text)
# print(requests.get('http://localhost:3500/v1.0/state/redis-statestore/1a').text)
# If-Match 如果设置了必须与存储的版本一致
# print(requests.delete('http://localhost:3500/v1.0/state/redis-statestore/1a', headers={"If-Match": "444"}))
print(requests.post('http://localhost:3500/v1.0/state/redis-statestore/bulk', headers={"If-Match": "444"}))
