import json

import requests

data = {

}
# actors/{actorType}/{actorId}/reminders/{name}
res = requests.post('http://localhost:3500/v1.0/actors/actorType-a/actorId-a/reminders/demo', json.dumps(data))
print(res.text)
