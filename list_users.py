import glob
import json

files = glob.glob("D:/fa-indexer/data/*/*/*.json")
users = set()
total_files = len(files)
count = 0
for file in files:
    with open(file, "r") as f:
        d = json.load(f)
        count += 1
    for key in d.keys():
        sub = d[key]
        if sub is not None:
            users.add(sub["username"])
    print(f"Done {count} files of {total_files}. So far, {len(users)} users. Current file: {file}")

print(f"total of {len(users)} users")
with open("users.json", "w") as f:
    json.dump(sorted(list(users)), f)
