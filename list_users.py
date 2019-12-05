import glob
import json

files = glob.glob("D:/fa-indexer/data/*/*/*.json")
users = set()
for file in files:
    with open(file, "r") as f:
        d = json.load(f)
    for key in d.keys():
        sub = d[key]
        users.add(sub["author"])

print(f"total of {len(users)} users")
with open("users.json", "w") as f:
    json.dump(sorted(list(users)), f)