import json
import os
import random
import re
import signal
import threading
from datetime import datetime
from queue import Queue

import numpy as np
import requests
from random_user_agent.params import OperatingSystem, SoftwareName
from random_user_agent.user_agent import UserAgent

import Config
from consumer import DiscussionConsumer, TopicConsumer
from logHandler import LogHandler
from topic import Topic
from utils import Utils

douban_headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.192 Safari/537.36'
}


def refreshUA():
    software_names = [SoftwareName.CHROME.value]
    operating_systems = [
        OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
    user_agent_rotator = UserAgent(
        software_names=software_names, operating_systems=operating_systems, limit=1000)
    douban_headers['User-Agent'] = user_agent_rotator.get_random_user_agent()


class DoubanApiProvider(threading.Thread):
    def __init__(self, pages, topics):
        threading.Thread.__init__(self)
        self._apikey = random.choice(['054022eaeae0b00e0fc068c0c0a2102a',
                                      '0df993c66c0c636e29ecbb5344252a4a', '0b2bdeda43b5688921839c8ecb20399b'])
        self._log = LogHandler("DoubanApiThread")
        self._pages = pages
        self._topics = topics

    def run(self):
        while(True):
            url = self._pages.get() + '&apikey='+self._apikey
            proxy = Utils.get_proxy()
            refreshUA()
            try:
                res = requests.get(url, headers=douban_headers, proxies={
                    "https": "http://{}".format(proxy)}, verify=False).json()
                for topic in res['topics']:
                    t = Topic(updated=datetime.strptime(topic['updated'], "%Y-%m-%d %H:%M:%S"), author=json.dumps(topic['author'], ensure_ascii=False), photos=json.dumps(topic['photos'], ensure_ascii=False), like_count=topic['like_count'],
                              alt=topic['alt'], title=topic['title'], created=datetime.strptime(topic['created'], "%Y-%m-%d %H:%M:%S"), content=topic['content'], comments_count=topic['comments_count'], username=topic['author']['name'], groupname=re.findall(r'(?<=group\/).*(?=\/topics)', url)[0])
                    self._topics.put(t)
            except Exception as e:
                self._pages.put(url)
                requests.get(
                    "http://127.0.0.1:5010/delete?proxy={}".format(proxy))
                message = str(e)
                if len(message) > 80:
                    message = message[:80]
                self._log.error(message)


def init_page_tasks(pages):
    groupnames = ['beijingzufang', 'zhufang', 'opking', '279962']
    urls = [
        'https://api.douban.com/v2/group/{}/topics'.format(name) for name in groupnames]
    # urls = ['https://api.douban.com/v2/group/beijingzufang/topics','https://api.douban.com/v2/group/zhufang/topics','https://api.douban.com/v2/group/opking/topics','https://api.douban.com/v2/group/opking/topics',]
    for url in urls:
        for i in range(20):
            pages.put(url+'?count=100&start=0')
        # for start in np.arange(0, 100, 2):
            # pages.put(url+'?count=100&start='+str(start))


def run_program():
    pages = Queue()
    topics = Queue()

    def handler(signum, frame):
        print("Times up! Exiting...")
        os._exit(0)
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(240)

    init_page_tasks(pages)
    threads = list()
    for i in range(100):
        x = DoubanApiProvider(pages, topics)
        threads.append(x)
    threads.append(TopicConsumer(
        topics))
    for index, thread in enumerate(threads):
        thread.start()
    for index, thread in enumerate(threads):
        thread.join()


if __name__ == '__main__':
    run_program()
