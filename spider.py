import datetime
import os
import signal
import threading
import time
from queue import Queue

import requests
from lxml import etree
from random_user_agent.params import OperatingSystem, SoftwareName
from random_user_agent.user_agent import UserAgent

import Config
from consumer import DiscussionConsumer, TopicConsumer
from logHandler import LogHandler
from topic import Topic
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
            'Upgrade-Insecure-Requests': "1",
            'Sec-Fetch-Site': "none",
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-User': "?1",
            'Sec-Fetch-Dest': "document",
            'Cookie': self.config.douban_cookie
        }
        self.douban_url = self.config.douban_url
        self.pages = Queue()
        self.topics = Queue()
    def _refreshUA(self):
        software_names = [SoftwareName.CHROME.value]
        operating_systems = [
            OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
        user_agent_rotator = UserAgent(
            software_names=software_names, operating_systems=operating_systems, limit=1000)
        self.douban_headers['User-Agent'] = user_agent_rotator.get_random_user_agent()  

class DiscussionSpider(Spider):
    def __init__(self):
        Spider.__init__(self)
        self.log = LogHandler("spider")
        self._init_page_tasks(self.douban_url)

    def _init_page_tasks(self, urls):
        """初始化页面任务

        @group_url, str, 小组URL
        """
        for url in urls:
            for page in range(int(20/int(len(urls)))):
                self.pages.put(url + str(page * 25))

    def parse(self, body):
        """
        从discussion页面提取topic并添加进队列
        :param body: discussion html text
        :return:
        """
        def getupdated(tr):
            target = tr.xpath("td")[3].text
            return datetime.datetime(2021, int(target.split(
                '-')[0][1]), int(target.split(' ')[0].split('-')[1]), int(target.split(' ')[1].split(":")[0]), int(target.split(' ')[1].split(":")[1]))
        body = etree.HTML(body.text)
        trs = body.xpath('//tr[@class=""]')
        for tr in trs:
            title = tr.xpath('td/a/text()')[0].strip()
            alt = tr.xpath('td/a/@href')[0]
            username = tr.xpath('td/a')[1].text
            comments_count = tr.xpath("td")[2].text  # may be None
            updated = getupdated(tr)

            self.topics.put(Topic(title=title, alt=alt, username=username,
                                                      comments_count=comments_count, updated=updated))

    def craw(self):
        while(1):
            url_link = self.pages.get()
            self._refreshUA()
            proxy = Utils.get_proxy()
            try:
                r = requests.get(url_link, headers=self.douban_headers, verify=False, proxies={
                                 "https": "http://{}".format(proxy)})
                if r.status_code == 200:
                    self.parse(r)
                else:
                    self.log.error(
                        'request url error -status code: {}:'.format(r.status_code))
            except Exception as e:
                self.pages.put(url_link)
                requests.get(
                    "http://127.0.0.1:5010/delete?proxy={}".format(proxy))
                message = str(e)
                if len(message) > 80:
                    message = message[:80]
                self.log.error(message)
            time.sleep(self.config.douban_sleep_time)

    def run(self):
        threads = list()
        for i in range(15):
            threads.append(threading.Thread(
                target=self.craw, args=(self.page_queue_for_discussions)))
        threads.append(DiscussionConsumer(self.data_queue_for_disscusions))
        for index, thread in enumerate(threads):
            thread.start()
        for index, thread in enumerate(threads):
            thread.join()





class TopicSpider(Spider):
    def __init__(self):
        Spider.__init__(self)
        self.log = LogHandler("TopicSpider")
        self._init_page_tasks()

    def _init_page_tasks(self, urls):
        pass

    #TODO
    def parse(self, body, alt):
        body = etree.HTML(body)
        title = body.xpath("//tr/td[@class='tablecc']/text()")[0]
        paragraph = '||x||'.join(body.xpath(
            '//div[@class="rich-content topic-richtext"]/p/text()'))
        username = body.xpath(
            "//*[@id='topic-content']/div[2]/h3/span[1]/a")[0].text
        publish_date = body.xpath(
            "//*[@id='topic-content']/div[2]/h3/span[2]")[0].text
        photos = body.xpath(
            '//div[@class="image-container image-float-center"]/*/img/@src')
        # self.topics.put({'publishdate': datetime.datetime.strptime(publish_date.replace(
            # " ", "&"), "%Y-%m-%d&%H:%M:%S"), "name": author_name, "avatar": author_avatar, "images": img_urls, "url": url, "paragraph": paragraph})
        self.topics.put(Topic(title=title, alt=alt, username=username,
                                                      comments_count=comments_count, updated=updated))

    def craw(self):
        while(1):
            url = self.pages.get()
            self._refreshUA()

            proxy = Utils.get_proxy()
            try:
                r = requests.get(url, headers=self.douban_headers, verify=False, proxies={
                    "https": "http://{}".format(proxy)})
                if r.status_code == 200:
                    self.parse(r.text, url)
                else:
                    self.log.error(
                        'request url error -status code: {}:'.format(r.status_code))
            except Exception as e:
                self.pages.put(url)
                requests.get(
                    "http://127.0.0.1:5010/delete?proxy={}".format(proxy))
                message = str(e)
                if len(message) > 80:
                    message = message[:80]
                self.log.error(message)
            time.sleep(self.config.douban_sleep_time)

    def run(self):
        threads = list()
        for i in range(15):
            x = threading.Thread(target=self.craw, args=())
            threads.append(x)
        threads.append(TopicConsumer(
            self.topics, self.pages))
        for index, thread in enumerate(threads):
            thread.start()
        for index, thread in enumerate(threads):
            thread.join()


def start():
    def handler(signum, frame):
        print("Times up! Exiting...")
        os._exit(0)
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(500)
    spider.run()

if __name__ == '__main__':
    spider = DiscussionSpider()
    start()
