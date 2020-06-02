import sys
import json
import random
import string

table = sys.argv[1]
recordID = sys.argv[2]
timestamp = sys.argv[3]

data = {}
data["recordID"] = recordID
data["table"] = table
data["timestamp"] = timestamp
data["attributes"] = []

for arg in sys.argv[4:]:
  keyval = arg.split(":")
  data["attributes"].append({"key": keyval[0], "valueNew": keyval[1]})

updateFile = "".join(random.choice(string.ascii_lowercase) for i in range(32))

with open("/opt/proteus-mysql/{}/{}".format(table, updateFile), "w") as f:
    json.dump(data, f)
    f.close()
