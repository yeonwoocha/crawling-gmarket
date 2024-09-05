import os
import json
import requests
import time
import logging
import sys
import re

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
# from selenium.webdriver.support import expected_conditions as EC
from typing import Dict
# import traceback


class Crawling:
    def __init__(self, data_path=os.getcwd(), site_name=""):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36'
        }
        self.driver = webdriver.Chrome () # webdriver.Safari if sys.platform.lower() == "darwin" else webdriver.Chrome
        if not os.path.exists(data_path):
            os.mkdir(data_path)
        self.data_path = data_path
        self.site_name = site_name
        print(f"Data path: {self.data_path}")

        self.info_key2name = {
            "경력": "career",
            "학력": "academic_background",
            "마감일": "deadline",
            "근무지역": "location"
        }

        self.filenames = {
            "url_list": os.path.join(self.data_path, f"{self.site_name}.url_list.json"),
            "content_info": os.path.join(self.data_path, f"{self.site_name}.content_info.json"),
            "result": os.path.join(self.data_path, f"{self.site_name}.result.json")
        }

    def requests_get(self, url: str) -> requests.Response:
        """
        Execute request.get for url
        :param url: url to get requests
        :return: response for url
        """
        with requests.Session() as s:
            response = s.get(url, headers=self.headers)
        return response

    def run(self):
        """
        Run all process of crawling and extract data
        """
        pass

    def scroll_down_page(self, driver):
        """
        Extract full-page source if additional pages appear when scrolling down
        :return page_source: extracted page source
        """
        page_source = ""
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            if page_source == driver.page_source:
                break
            else:
                page_source = driver.page_source

        return page_source


class CrawlingSaramin(Crawling):
    """ (deprecated)
    Crawling of "https://www.saramin.co.kr" (cannot distinguish details of content)
    """

    def __init__(self, data_path=os.getcwd()):
        super().__init__(data_path=data_path)
        self.endpoint = "https://www.saramin.co.kr"
        self.site_name = "saramin"

    def get_id_dict(self, category_id: int = 83) -> Dict[str, str]:
        """
        Crawling id and title for recruitment pages
        :param category_id: id of recruit category
        :return: dict of recruitment page urls with title (key: url / value: title)
        """

        def _crawl_id(page_number: int) -> Dict[str, str]:
            """ Crawling id list for specific page number on recruitment page
            :param page_number: number of page to crawl
            :return: list of recruitment page urls for specific page
            """
            response = self.requests_get(
                f"{self.endpoint}/zf_user/search?cat_kewd={category_id}&recruitPageCount=100&recruitPage={page_number}"
            )
            soup = BeautifulSoup(response.text, 'html.parser')
            print('response1 check : ', response.text)
            contents = soup.find_all('div', class_='item_recruit')

            id_dict = {}
            for content in contents:
                _id = content.get('value')
                title = content.find('h2', class_='job_tit').find('a').get('title')
                id_dict[_id] = title

            return id_dict

        id_dict = {}
        recruit_page = 1
        _id_dict = _crawl_id(recruit_page)

        while _id_dict:
            id_dict.update(_id_dict)
            recruit_page += 1
            _id_dict = _crawl_id(recruit_page)

        return id_dict

    def get_recruit_content_info(self, category_id: int = 83) -> Dict[str, Dict[str, str]]:
        """
        Get recruit contents for each url
        :param category_id: id of recruit category
        :return: dict of recruit contents for each url (key: url / value: title, content)
        """
        recruit_content_dict = {}

        for _id, title in self.get_id_dict(category_id).items():
            url = f"https://www.saramin.co.kr/zf_user/jobs/relay/view?rec_idx={_id}"
            response = self.requests_get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            print('response2 check : ', response.text)

            recruit_content = ""
            for content in soup.find_all('script'):
                if 'recruit_content' in content.text:
                    recruit_content = content
                    break

            if recruit_content == "":
                continue

            content_text = recruit_content.text
            start_index = content_text.index('"recruit_contents":"')
            end_index = content_text.index('","kindness_expired_dt"')
            recruit_contents = content_text[start_index + len('"recruit_contents":"'):end_index]

            recruit_content_dict[url] = {
                "title": title,
                "content": recruit_contents.encode('utf-8').decode('unicode_escape')
            }

        return recruit_content_dict

    def run(self):
        """
        run all process of crawling and extract data
        """
        recruit_content_infos = self.get_recruit_content_info()

        with open(os.path.join(self.data_path, f"url.{self.site_name}.tsv"), "w") as f:
            for url, info in recruit_content_infos.items():
                f.write("\t".join([
                    url, info.get("title", ""), info.get("content", "")
                ]) + "\n")


CRAWLING_CLASS = {
    "jumpit": CrawlingSaramin
}

def main(args):
    logger = logging.getLogger()
    logger.setLevel(
        logging.DEBUG if args.log_type.lower() == "debug" else logging.INFO
    )

    logging.info("[INFO] Set instance of crawling")
    crawling = CRAWLING_CLASS.get(args.site_type.lower(), Crawling)(data_path=args.data_path)

    logging.info("[INFO] Get recruit content info")
    if args.method == "all":
        crawling.run()
    else:
        method = getattr(crawling, args.method, None)
        if method:
            method()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--site_type", help="type of site", default="jobplanet")
    parser.add_argument("-l", "--log_type", help="type of log", default="info")
    parser.add_argument("-d", "--data_path", help="path of data", default=os.path.join(os.getcwd(), "data"))
    parser.add_argument("-m", "--method", help="method to execute", default="all")

    args = parser.parse_args()
    main(args)
