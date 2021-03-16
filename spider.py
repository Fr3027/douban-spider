import datetime
import os
import sqlite3
import sys
import threading
import time
from queue import Empty, Queue

import requests
from bs4 import BeautifulSoup
from lxml import etree
from random_user_agent.params import OperatingSystem, SoftwareName
from random_user_agent.user_agent import UserAgent

import Config
from consumer import PageTopicConsumer, PageListConsumer
from logHandler import LogHandler
from utils import Utils


class Spider(object):
    def __init__(self):
        this_file_dir = os.path.split(os.path.realpath(__file__))[0]
        config_file_path = os.path.join(this_file_dir, 'config.ini')
        self.config = Config.Config(config_file_path)
        self.douban_headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, sdch',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.6,en;q=0.4,en-GB;q=0.2,zh-TW;q=0.2',
            'Connection': 'keep-alive',
            'DNT': '1',
            'HOST': 'www.douban.com',
            'referer': 'www.douban.com',
            'Cookie': self.config.douban_cookie
        }
        self.douban_headers['Upgrade-Insecure-Requests'] = "1"
        self.douban_headers['Sec-Fetch-Site'] = "none"
        self.douban_headers['Sec-Fetch-Mode'] = "navigate"
        self.douban_headers['Sec-Fetch-User'] = "?1"
        self.douban_headers['Sec-Fetch-Dest'] = "document"
        self.douban_url = self.config.douban_url
        self.douban_url_name = self.config.douban_url_name

        self.page_list_url_queue = Queue()
        self.page_topic_url_queue = Queue()
        self.page_list_data_queue = Queue()
        self.page_topic_data_queue = Queue()

        self.log = LogHandler("spider")
        self._init_page_tasks(self.douban_url[0])

    def refreshUA(self):
        software_names = [SoftwareName.CHROME.value]
        operating_systems = [
            OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
        user_agent_rotator = UserAgent(
            software_names=software_names, operating_systems=operating_systems, limit=1000)
        self.douban_headers['User-Agent'] = user_agent_rotator.get_random_user_agent()

    def _init_page_tasks(self, group_url):
        """初始化页面任务

        @group_url, str, 小组URL
        """
        for page in range(100):
            url = group_url + str(page * 25)
            self.page_list_url_queue.put(url)

    def parse_page_list(self, body):
        soup = BeautifulSoup(body.text, "lxml")
        paginator = soup.find_all(attrs={'class': 'paginator'})[0]
        table = soup.find_all(attrs={'class': 'olt'})[0]
        trs = table.find_all('tr')
        trs.pop(0)
        for tr in trs:
            td = tr.find_all('td')
            title_element = td[0].find_all('a')[0]
            title_text = title_element.get('title')
            time_text = td[1].get('title')
            target = td[3].text
            target_time = datetime.datetime(2021, int(target.split(
                '-')[0][1]), int(target.split(' ')[0].split('-')[1]))
            link_text = title_element.get('href')
            reply_count = td[2].text
            spider.list_data_queue.put({'title_text': title_text, 'link_text': link_text, 'target_time': target_time,
                                        "keyword": "", "douban_url_name": "beijingzufang", "reply_count": reply_count})

    def parse_page_topic(self, body, url):
        body = etree.HTML(body)
        # title = body.xpath("//tr/td[@class='tablecc']/text()")[0]
        rich_content = body.xpath('//div[@class="rich-content topic-richtext"]')
        paragraph = '||x||'.join(body.xpath('//div[@class="rich-content topic-richtext"]/p/text()'))
        author_name = body.xpath(
            "//*[@id='topic-content']/div[2]/h3/span[1]/a")[0].text
        author_avatar = body.xpath(
            "//*[@id='topic-content']/div[2]/h3/span[1]/a/@href")[0]
        publish_date = body.xpath(
            "//*[@id='topic-content']/div[2]/h3/span[2]")[0].text
        img_urls = body.xpath(
            '//div[@class="image-container image-float-center"]/*/img/@src')

        self.page_topic_data_queue.put({'publishdate': datetime.datetime.strptime(publish_date.replace(
            " ", "&"), "%Y-%m-%d&%H:%M:%S"), "name": author_name, "avatar": author_avatar, "images": img_urls, "url": url,"paragraph":paragraph})

    def craw_page_list(self, i, page_queue, douban_url_name, keyword, douban_headers):
        while(1):
            url_link = page_queue.get()
            self.refreshUA()

            def get_proxy():
                return requests.get("http://127.0.0.1:5010/get/").json().get("proxy")
            proxy = get_proxy()
            try:
                r = requests.get(url_link, headers=self.douban_headers, verify=False, proxies={
                                 "https": "http://{}".format(proxy)})
                if r.status_code == 200:
                    self.parse_page_list(r)
                else:
                    self.log.error(
                        'request url error -status code: {}:'.format(r.status_code))
            except Exception as e:
                # return url_link to origin queue
                page_queue.put(url_link)
                requests.get(
                    "http://127.0.0.1:5010/delete?proxy={}".format(proxy))
                message = str(e)
                if len(message) > 80:
                    message = message[:80]
                self.log.error(message)


    def craw_page_topic(self):
        while(1):
            url_link = self.page_topic_url_queue.get()
            self.refreshUA()

            def get_proxy():
                return requests.get("http://127.0.0.1:5010/get/").json().get("proxy")
            proxy = get_proxy()
            try:
                r = requests.get(url_link, headers=self.douban_headers, verify=False, proxies={
                    "https": "http://{}".format(proxy)})
                if r.status_code == 200:
                    self.parse_page_topic(r.text, url_link)
                else:
                    self.log.error(
                        'request url error -status code: {}:'.format(r.status_code))
            except Exception as e:
                self.page_topic_url_queue.put(url_link)
                requests.get(
                    "http://127.0.0.1:5010/delete?proxy={}".format(proxy))
                message = str(e)
                if len(message) > 80:
                    message = message[:80]
                self.log.error(message)

    def runThread(self):
        threads = list()
        for i in range(15):
            threads.append(threading.Thread(target=self.craw_page_list, args=(
                0, self.page_list_url_queue, "beijingzufang", "", self.douban_headers,)))
        threads.append(PageListConsumer(spider.list_data_queue))
        for index, thread in enumerate(threads):
            thread.start()
        for index, thread in enumerate(threads):
            thread.join()

    def runTopicThread(self):
        threads = list()
        for i in range(15):
            x = threading.Thread(target=self.craw_page_topic, args=())
            threads.append(x)
        threads.append(PageTopicConsumer(
            spider.topic_data_queue, spider.page_topic_queue))
        for index, thread in enumerate(threads):
            thread.start()
        for index, thread in enumerate(threads):
            thread.join()
        # conn = sqlite3.connect('result.sqlite', isolation_level=None)
        # conn.text_factory = str
        # cursor = conn.cursor()
        # res = cursor.execute(
        #     "SELECT URL FROM rent WHERE avatar IS NULL LIMIT 1000").fetchall()
        # for row in res:
        #     self.page_topic_queue.put(row[0])
        # self.craw_page_topic()
        


if __name__ == '__main__':
    spider = Spider()
    spider.runTopicThread()
