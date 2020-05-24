import sys
import json
import random
import string

table = sys.argv[1]
recordID = sys.argv[2]

print(sys.argv[1])
print(sys.argv[2])
print(sys.argv[3:])

data = {}
data["recordID"] = recordID
data["table"] = table
data["attributes"] = []

for arg in sys.argv[3:]:
  print(arg)
  keyval = arg.split(":")
  print(keyval)
  data["attributes"].append({"key": keyval[0], "valueNew": keyval[1]})
# vote_id = sys.argv[1]
# story_id = sys.argv[2]
# if len(sys.argv) == 4:
#     comment_id = None
#     vote = sys.argv[3]
# elif len(sys.argv) == 5:
#     comment_id = sys.argv[3]
#     vote = sys.argv[4]
# else:
#     print("Unexpected number of argumets")
#     sys.exit(1)

# data = {}
# data["recordID"] = vote_id
# data["table"] = "table"
# data["attributes"] = []
# data["attributes"].append({"key": "story_id", "valueNew": story_id})
# data["attributes"].append({"key": "comment_id", "valueNew": comment_id})
# data["attributes"].append({"key": "vote", "valueNew": vote})

updateFile = "".join(random.choice(string.ascii_lowercase) for i in range(32))

with open("/opt/proteus-mysql/{}/{}".format(table, updateFile), "w") as f:
    json.dump(data, f)
    f.close()
