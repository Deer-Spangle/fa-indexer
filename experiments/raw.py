import json
import os
from multiprocessing.dummy import Pool as ThreadPool

import requests

with open("config.json", "r") as f:
    config = json.load(f)
pool = ThreadPool(8)


def download_page(sub_id):
    print(f"Downloading: {sub_id}")
    data = download_page_html(sub_id)
    save_page(data, sub_id)


def download_page_html(sub_id):
    url = f"https://www.furaffinity.net/view/{sub_id}/"
    return download_url(url)


def download_url(url):
    resp = requests.get(url)
    if resp.status_code != 200:
        return b""
    data = resp.content
    return data


def make_directories(directory):
    os.makedirs(os.path.dirname(directory), exist_ok=True)


def save_page(page_code, sub_id):
    filename = f"{sub_id}.html"
    directory = f"raw/{sub_id//1000000:02}/{sub_id%1000000//10000:02}/{sub_id%10000//100:02}/"
    make_directories(directory)
    with open(directory + filename, "wb+") as f:
        f.write(page_code)


def scrape_batch(start, end):
    full_data = dict()
    id_range = list(range(start, end+1))
    pool.map(download_page, id_range)


def scrape(start=1, increment=100):
    index = start
    while True:
        end = index + increment - 1
        print(f"START BATCH: {index} - {end}")
        scrape_batch(index, end)
        print(f"END BATCH: {index} - {end}")
        index = end + 1


def find_latest_downloaded_id():
    try:
        dir_1 = str(max(int(x) for x in os.listdir("raw/"))).zfill(2)
        dir_2 = str(max(int(x) for x in os.listdir(f"raw/{dir_1}/"))).zfill(2)
        dir_3 = [int(x) for x in os.listdir(f"raw/{dir_1}/{dir_2}")]
        second_biggest_dir = sorted(dir_3)[-2]
        return second_biggest_dir*100 + 99
    except FileNotFoundError:
        return 0
    except IndexError:
        return 0


if __name__ == "__main__":
    largest_id = find_latest_downloaded_id()
    scrape(largest_id+1)
