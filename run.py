import json
import os

import requests

with open("config.json", "r") as f:
    config = json.load(f)


def download_entry(sub_id):
    print(f"Downloading: {sub_id}")
    entry = dict()
    entry['data'] = download_sub_data(sub_id)
    entry['comments'] = download_comment_data(sub_id)
    return entry


def download_sub_data(sub_id):
    return download_json(f"{config['API_URL']}/submission/{sub_id}.json")


def download_comment_data(sub_id):
    return download_json(f"{config['API_URL']}/submission/{sub_id}/comments.json")


def download_json(url):
    resp = requests.get(url)
    if resp.status_code != 200:
        return {}
    data = resp.json()
    return data


def make_directories(directory):
    os.makedirs(os.path.dirname(directory), exist_ok=True)


def save_batch(start_id, end_id, full_data):
    filename = f"batch-{start_id:08}-{end_id:08}.json"
    directory = f"data/{start_id//1000000:02}/{start_id%1000000//10000:02}/"
    make_directories(directory)
    with open(directory + filename, "w+") as f:
        json.dump(full_data, f)


def scrape_batch(start, end):
    full_data = dict()
    for sub_id in range(start, end+1):
        entry = download_entry(sub_id)
        full_data[str(sub_id)] = entry
    save_batch(start, end, full_data)


def scrape(start=1, increment=100):
    index = start
    while True:
        end = index + increment - 1
        print(f"START BATCH: {index} - {end}")
        scrape_batch(index, end)
        print(f"END BATCH: {index} - {end}")
        index = end + 1


if __name__ == "__main__":
    scrape(201)
