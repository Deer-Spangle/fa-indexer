import json
import os
import re
import time
from abc import ABC
from multiprocessing.dummy import Pool as ThreadPool
from threading import RLock
from typing import Union, List, Optional
import dateutil.parser as parser
from bs4 import BeautifulSoup
import glob

import requests


class PageResult:
    def __init__(
            self,
            sub_id: int,
            username: str,
            title: str,
            description: str,
            keywords: List[str],
            date: str,
            rating: str,
            filename: str
    ):
        self.sub_id = sub_id
        self.username = username
        self.title = title
        self.description = description
        self.keywords = keywords
        self.date = date
        self.rating = rating
        self.filename = filename

    def __repr__(self):
        return json.dumps(self.to_dict())

    def to_dict(self):
        return {
            "id": self.sub_id,
            "username": self.username,
            "title": self.title,
            "description": self.description,
            "keywords": self.keywords,
            "date": self.date,
            "rating": self.rating,
            "filename": self.filename
        }


class PageGetter(ABC):
    def result(self) -> Optional[PageResult]:
        raise NotImplementedError()

    def should_slow_down(self) -> Optional[bool]:
        return False


class WebsiteDownloader(PageGetter):
    def __init__(self, sub_id: int, login_cookie: dict):
        self.sub_id = sub_id
        self.login_cookie = login_cookie
        self.over_10k_registered = False

    def download_page(self):
        resp = requests.get(f"http://furaffinity.net/view/{self.sub_id}", cookies=self.login_cookie)
        if resp.status_code == 200:
            return resp.content
        raise Exception(f"Did not receive 200 response from FA. ({resp.status_code})")

    def read_status(self, soup):
        footer = soup.select_one(".footer center").decode_contents()
        reg_users = re.search(r"([0-9]+)\s*<b>registered", footer, re.I)
        if reg_users is not None:
            reg_count = int(reg_users.group(1))
            if reg_count > 10000:
                self.over_10k_registered = True

    def result(self) -> Optional[PageResult]:
        html = self.download_page()
        soup = BeautifulSoup(html, "html.parser")
        main_table = soup.select_one('div#page-submission table.maintable table.maintable')
        if main_table is None:
            self.over_10k_registered = None
            return None

        title_bar = main_table.select_one('.classic-submission-title.container')
        stats_container = main_table.select_one('td.alt1.stats-container')
        actions_bar = soup.select_one('#page-submission div.actions')

        username = title_bar.select_one('.information a')['href'].strip("/").split("/")[-1]
        title = title_bar.select_one('h2').text
        description = main_table.select('tr')[-1].select_one('td').decode_contents().strip()
        keywords = [x.text for x in stats_container.select('div#keywords a')]
        date = parser.parse(stats_container.select_one('.popup_date')['title']).isoformat()
        rating = stats_container.select_one('img')['alt'].replace(' rating', '')
        filename = "https:" + [x['href'] for x in actions_bar.select('a') if x.text == "Download"][0]

        self.read_status(soup)

        return PageResult(
            self.sub_id,
            username,
            title,
            description,
            keywords,
            date,
            rating,
            filename
        )

    def should_slow_down(self):
        return self.over_10k_registered


class APIDownloader(PageGetter):
    def __init__(self, sub_id: int, api_url: Union[str, List[str]]):
        self.sub_id = sub_id
        self.api_url = api_url
        self.over_10k_registered = False

    def make_url(self, path):
        api_url = self.api_url
        if isinstance(self.api_url, list):
            options = len(self.api_url)
            api_url = self.api_url[self.sub_id % options]
        url = api_url + path
        return url

    def download_sub_data(self):
        path = f"/submission/{self.sub_id}.json"
        url = self.make_url(path)
        return self.download_json(url)

    def download_json(self, url):
        resp = requests.get(url)
        if resp.status_code != 200:
            return None
        data = resp.json()
        return data

    def download_status(self):
        path = f"/status.json"
        url = self.make_url(path)
        return self.download_json(url)

    def result(self) -> Optional[PageResult]:
        print(f"Downloading: {self.sub_id}")
        data = self.download_sub_data()

        status = self.download_status()
        if status['online']['registered'] > 10000:
            self.over_10k_registered = True

        if data is None:
            return None
        return PageResult(
            self.sub_id,
            data['profile_name'],
            data['title'],
            data['description'],
            data['keywords'],
            data['posted_at'],
            data['rating'],
            data['download']
        )

    def should_slow_down(self):
        return self.over_10k_registered


class OldDataUpdater(PageGetter):
    def __init__(self, sub_id, old_data):
        self.sub_id = sub_id
        self.old_data = old_data

    def result(self) -> Optional[PageResult]:
        if self.old_data['data'] == {}:
            return None
        return PageResult(
            self.sub_id,
            self.old_data['data']['profile_name'],
            self.old_data['data']['title'],
            self.old_data['data']['description'],
            self.old_data['data']['keywords'],
            self.old_data['data']['posted_at'],
            self.old_data['data']['rating'],
            self.old_data['data']['download']
        )


class DataMerger(PageGetter):
    def __init__(self, sub_id, data):
        self.sub_id = sub_id
        self.data = data

    def result(self) -> Optional[PageResult]:
        if self.data is None:
            return None
        return PageResult(
            self.sub_id,
            self.data['username'],
            self.data['title'],
            self.data['description'],
            self.data['keywords'],
            self.data['date'],
            self.data['rating'],
            self.data['filename']
        )


class ArchiveTeamReader(PageGetter):
    def __init__(self, sub_id, file_name):
        self.sub_id = sub_id
        self.file_name = file_name

    def result(self) -> Optional[PageResult]:
        with open(self.file_name, "r") as archive_file:
            html = archive_file.read()
        soup = BeautifulSoup(html, "html.parser")
        main_table = soup.select_one('table.maintable table.maintable table.maintable')
        if main_table is None:
            return None

        title_bar = main_table.select_one('td.cat')
        stats_container = main_table.select_one('td.alt1 td.alt1')
        actions_bar = soup.select_one('div.actions')

        username = title_bar.select_one('a')['href'].strip("/").split("/")[-1]
        title = title_bar.select_one('b').text
        description_raw = main_table.select('tr')[-1].select_one('td').decode_contents().strip()
        description = "<br/>".join(description_raw.split("<br/>")[2:]).strip()
        keywords = [x.text for x in stats_container.select('div#keywords a')]
        date = parser.parse(stats_container.select_one('.popup_date').text).isoformat()
        rating = stats_container.select_one('img')['alt'].replace(' rating', '')
        filename = "https:" + [x['href'] for x in actions_bar.select('a') if x.text.strip() == "Download"][0]

        return PageResult(
            self.sub_id,
            username,
            title,
            description,
            keywords,
            date,
            rating,
            filename
        )


class Scraper:
    def __init__(self, config):
        self.batch_size = 100
        self.config = config
        self.pool = ThreadPool(8)
        self.slow_down = False
        self.latest_file = {
            "old_data": {
                "filename": None,
                "data": None,
                "lock": RLock()
            },
            "batch_file": {
                "filename": None,
                "data": None,
                "lock": RLock()
            }
        }
        old_files = glob.glob("old_data/**/*.json", recursive=True)
        self.old_file_ranges = [
            [int(x.split(os.sep)[-1].split(".")[0].split("-")[y]) for y in [1, 2]] + [x]
            for x in old_files
        ]

    def get_file_data(self, file_key: str, filename_wanted):
        file_entry = self.latest_file[file_key]
        if file_entry["filename"] == filename_wanted:
            return file_entry["data"]
        with self.latest_file[file_key]["lock"]:
            # Read new file
            with open(filename_wanted, "r") as dump:
                data = json.load(dump)
            # Set variables
            new_obj = {
                "filename": filename_wanted,
                "data": data,
                "lock": self.latest_file[file_key]["lock"]
            }
            self.latest_file[file_key] = new_obj
        return data

    def check_old_data(self, sub_id: int) -> Union[bool, dict]:
        for file in self.old_file_ranges:
            if file[0] <= sub_id <= file[1]:
                old_data = self.get_file_data("old_data", file[2])
                if str(sub_id) in old_data:
                    return old_data[str(sub_id)]
        return False

    def already_exists(self, sub_id) -> Union[bool, Optional[dict]]:
        directory, filename = self.filename_for_id(sub_id)
        if os.path.exists(directory + filename):
            data = self.get_file_data("batch_file", directory + filename)
            if str(sub_id) in data:
                return data[str(sub_id)]
        return False

    def in_archive(self, sub_id) -> Union[bool, str]:
        dir_name = f"fa-extract/www.furaffinity.net/view/{sub_id}/"
        file_name = glob.glob(f"{dir_name}*")
        if len(file_name) == 0:
            return False
        return file_name[0]

    def pick_downloader(self, sub_id):
        # Check if already got the data
        batch_data = self.already_exists(sub_id)
        if batch_data is not False:
            return DataMerger(sub_id, batch_data)
        # Check if data is in old format
        old_data = self.check_old_data(sub_id)
        if old_data is not False:
            return OldDataUpdater(sub_id, old_data)
        # Check if data is in archive team data
        archive_file = self.in_archive(sub_id)
        if archive_file is not False:
            return ArchiveTeamReader(sub_id, archive_file)
        elif 'API_URL' in self.config:
            return APIDownloader(sub_id, self.config['API_URL'])
        elif 'LOGIN_COOKIE' in self.config:
            return WebsiteDownloader(sub_id, self.config['LOGIN_COOKIE'])
        else:
            raise Exception("Please set API_URL or LOGIN_COOKIE in config")

    def download_entry(self, sub_id):
        if self.slow_down:
            print("Waiting")
            time.sleep(4)
        downloader = self.pick_downloader(sub_id)
        print(f"Picked: {downloader.__class__.__name__}")
        while True:
            try:
                result = downloader.result()
                break
            except Exception as e:
                print(e)
                time.sleep(10)
        slow_down = downloader.should_slow_down()
        if slow_down is not None:
            self.slow_down = slow_down
        return None if result is None else result.to_dict()

    def make_directories(self, directory):
        os.makedirs(os.path.dirname(directory), exist_ok=True)

    def filename_for_id(self, sub_id):
        batch_start = (sub_id // self.batch_size) * self.batch_size
        batch_end = batch_start + self.batch_size
        filename = f"batch-{batch_start:08}-{batch_end:08}.json"
        directory = f"data/{batch_start//1000000:02}/{batch_start%1000000//10000:02}/"
        return directory, filename

    def save_batch(self, start_id, full_data):
        directory, filename = self.filename_for_id(start_id)
        if "UPLOAD" in self.config:
            self.upload_batch(directory + filename, full_data)
            return
        self.make_directories(directory)
        with open(directory + filename, "w+") as dump_file:
            json.dump(full_data, dump_file)

    def upload_batch(self, path, full_data):
        url = self.config['UPLOAD']['URL'] + path
        headers = {"Authorization": self.config['UPLOAD']['KEY']}
        requests.post(url, json=full_data, headers=headers)

    def scrape_batch(self, start, end):
        full_data = dict()
        id_range = list(range(start, end+1))
        results = self.pool.map(self.download_entry, id_range)
        for result_key in range(len(results)):
            full_data[str(start+result_key)] = results[result_key]
        self.save_batch(start, full_data)

    def scrape(self, start=1, end=None):
        batch_start = (start // self.batch_size) * self.batch_size
        batch_end = batch_start + self.batch_size - 1
        while (end is None) or (batch_start < end):
            print(f"START BATCH: {batch_start} - {batch_end}")
            self.scrape_batch(batch_start, batch_end)
            print(f"END BATCH: {batch_start} - {batch_end}")
            batch_start = batch_end + 1
            batch_end = batch_start + self.batch_size - 1


def find_latest_downloaded_id():
    dir_1 = str(max(int(x) for x in os.listdir("data/"))).zfill(2)
    dir_2 = str(max(int(x) for x in os.listdir(f"data/{dir_1}/"))).zfill(2)
    files = os.listdir(f"data/{dir_1}/{dir_2}/")
    largest_id = max([int(x.split("-")[-1].split(".")[0]) for x in files])
    return largest_id


if __name__ == "__main__":
    with open("config.json", "r") as f:
        conf = json.load(f)
    scraper = Scraper(conf)
    scraper.scrape(conf['START'], conf['END'])
