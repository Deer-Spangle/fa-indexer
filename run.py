import json
import os
from multiprocessing.dummy import Pool as ThreadPool

import requests

with open("config.json", "r") as f:
    config = json.load(f)
pool = ThreadPool(8)


class Downloader:
    def __init__(self, sub_id):
        self.sub_id = sub_id

    def make_url(self, path):
        api_url = config['API_URL']
        if isinstance(config['API_URL'], list):
            options = len(config['API_URL'])
            api_url = config['API_URL'][self.sub_id % options]
        url = api_url + path
        return url

    def download_sub_data(self):
        path = f"/submission/{self.sub_id}.json"
        url = self.make_url(path)
        return self.download_json(url)

    def download_comment_data(self):
        path = f"/submission/{self.sub_id}/comments.json"
        url = self.make_url(path)
        return self.download_json(url)

    def download_json(self, url):
        resp = requests.get(url)
        if resp.status_code != 200:
            return {}
        data = resp.json()
        return data

    def result(self):
        print(f"Downloading: {self.sub_id}")
        entry = dict()
        entry['data'] = self.download_sub_data()
        entry['comments'] = self.download_comment_data()
        return entry


class Scraper:
    def __init__(self, start, end=None):
        self.start = start
        self.end = end

    def download_entry(self, sub_id):
        downloader = Downloader(sub_id)
        return downloader.result()

    def make_directories(self, directory):
        os.makedirs(os.path.dirname(directory), exist_ok=True)

    def save_batch(self, start_id, end_id, full_data):
        filename = f"batch-{start_id:08}-{end_id:08}.json"
        directory = f"data/{start_id//1000000:02}/{start_id%1000000//10000:02}/"
        self.make_directories(directory)
        with open(directory + filename, "w+") as dump_file:
            json.dump(full_data, dump_file)

    def scrape_batch(self, start, end):
        full_data = dict()
        id_range = list(range(start, end+1))
        results = pool.map(self.download_entry, id_range)
        for result_key in range(len(results)):
            full_data[str(start+result_key)] = results[result_key]
        self.save_batch(start, end, full_data)

    def scrape(self, start=1, increment=100):
        index = start
        while True:
            end = index + increment - 1
            print(f"START BATCH: {index} - {end}")
            self.scrape_batch(index, end)
            print(f"END BATCH: {index} - {end}")
            index = end + 1


def find_latest_downloaded_id():
    dir_1 = str(max(int(x) for x in os.listdir("data/"))).zfill(2)
    dir_2 = str(max(int(x) for x in os.listdir(f"data/{dir_1}/"))).zfill(2)
    files = os.listdir(f"data/{dir_1}/{dir_2}/")
    largest_id = max([int(x.split("-")[-1].split(".")[0]) for x in files])
    return largest_id


if __name__ == "__main__":
    latest_id = find_latest_downloaded_id()
    scraper = Scraper(latest_id)
    scraper.scrape()
