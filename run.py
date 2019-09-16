import json
import os
from abc import ABC
from multiprocessing.dummy import Pool as ThreadPool
from typing import Union, List
import dateutil.parser as parser
from bs4 import BeautifulSoup

import requests

with open("config.json", "r") as f:
    config = json.load(f)
pool = ThreadPool(8)


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
    def result(self) -> PageResult:
        raise NotImplementedError()


class WebsiteDownloader(PageGetter):
    def __init__(self, sub_id: int, login_cookie: dict):
        self.sub_id = sub_id
        self.login_cookie = login_cookie

    def download_page(self):
        return requests.get(f"http://furaffinity.net/view/{self.sub_id}", cookies=self.login_cookie)

    def result(self):
        html = self.download_page()
        soup = BeautifulSoup(html)
        main_table = soup.select_one('div#page-submission table.maintable table.maintable')
        title_bar = main_table.select_one('.classic-submission-title.container')
        stats_container = main_table.select_one('td.alt1.stats-container')
        actions_bar = soup.select_one('#page-submission div.actions')

        username = title_bar.select_one('.information a')['href'].split("/")[-1].strip("/").split("/")[-1]
        title = title_bar.select_one('h2').contents
        description = main_table.select('>tbody>tr')[-1].select_one('td').contents.strip()
        keywords = [x.contents for x in stats_container.select('div#keywords a')]
        date = parser.parse(stats_container.select_one('.popup_date')['title']).isoformat()
        rating = stats_container.at_css('img')['alt'].replace(' rating', '')
        filename = "https:" + [x['href'] for x in actions_bar if x.contents == "Download"][0]

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


class APIDownloader(PageGetter):
    def __init__(self, sub_id: int, api_url: Union[str, List[str]]):
        self.sub_id = sub_id
        self.api_url = api_url

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
            return {}
        data = resp.json()
        return data

    def result(self):
        print(f"Downloading: {self.sub_id}")
        data = self.download_sub_data()
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


class Scraper:
    def __init__(self, start, end=None):
        self.start = start
        self.end = end

    def download_entry(self, sub_id):
        downloader = APIDownloader(sub_id, config['API_URL'])
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
