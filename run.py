import json
import os
from multiprocessing.dummy import Pool as ThreadPool

import requests

with open("config.json", "r") as f:
    config = json.load(f)
pool = ThreadPool(8)


def download_entry(sub_id):
    print(f"Downloading: {sub_id}")
    entry = dict()
    entry['data'] = download_sub_data(sub_id)
    entry['comments'] = download_comment_data(sub_id)
    return entry


def make_url(path, number):
    api_url = config['API_URL']
    if isinstance(config['API_URL'], list):
        options = len(config['API_URL'])
        api_url = config['API_URL'][number % options]
    url = api_url + path
    return url


def download_sub_data(sub_id):
    path = f"/submission/{sub_id}.json"
    url = make_url(path, sub_id)
    return download_json(url)


def download_comment_data(sub_id):
    path = f"/submission/{sub_id}/comments.json"
    url = make_url(path, sub_id)
    return download_json(url)


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
    id_range = list(range(start, end+1))
    results = pool.map(download_entry, id_range)
    for result_key in range(len(results)):
        full_data[str(start+result_key)] = results[result_key]
    save_batch(start, end, full_data)


def scrape(start=1, increment=100):
    index = start
    while True:
        end = index + increment - 1
        print(f"START BATCH: {index} - {end}")
        scrape_batch(index, end)
        print(f"END BATCH: {index} - {end}")
        index = end + 1


def find_latest_downloaded_id():
    dir_1 = str(max(int(x) for x in os.listdir("data/"))).zfill(2)
    dir_2 = str(max(int(x) for x in os.listdir(f"data/{dir_1}/"))).zfill(2)
    files = os.listdir(f"data/{dir_1}/{dir_2}/")
    largest_id = max([int(x.split("-")[-1].split(".")[0]) for x in files])
    return largest_id


if __name__ == "__main__":
    largest_id = find_latest_downloaded_id()
    scrape(largest_id+1)
