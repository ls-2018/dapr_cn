import json
import random

import requests

url = 'http://127.0.0.1:3001/post'
dapr_url = "http://localhost:3500/v1.0/invoke/dp-61c03c5f8ea49c26debd26a6-workerapp/method/post"

res = requests.post(dapr_url, json.dumps({'a': random.random() * 1000}))
print(res.json())
