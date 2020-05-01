import sys
import json
import random
import string

userID = sys.argv[1]
storyID = sys.argv[2]

data = {}
data['recordID'] = userID+storyID
data['table'] = 'votes'
data['attributes'] = []
data['attributes'].append({
    'key': 'user_id',
    'valueNew': userID
})
data['attributes'].append({
    'key': 'story_id',
    'valueNew': storyID
})

updateFile = ''.join(random.choice(string.ascii_lowercase) for i in range(32))

with open("/opt/trigger_data/votes/{}".format(updateFile),"w") as f:
    json.dump(data, f)
    f.close()