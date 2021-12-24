import json

import requests

print(requests.post('http://localhost:3500/v1.0/publish/redis-pubsub/topic-c', json.dumps(
    {}
)))
