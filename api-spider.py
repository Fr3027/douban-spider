from datetime import datetime
import json
import os
import sqlite3
import sys
import threading
import time
from queue import Empty, Queue
from logHandler import LogHandler
import pytest
import requests
from bs4 import BeautifulSoup
from lxml import etree
from random_user_agent.params import OperatingSystem, SoftwareName
from random_user_agent.user_agent import UserAgent

import Config
from consumer import PageListConsumer, PageTopicConsumer
from spider import Spider
from utils import Utils
from topic import Topic

def test_abc():
    apikey = "0df993c66c0c636e29ecbb5344252a4a"
    # 054022eaeae0b00e0fc068c0c0a2102a
    # 0df993c66c0c636e29ecbb5344252a4a
    # 0b2bdeda43b5688921839c8ecb20399b
    url = "https://api.douban.com/v2/group/beijingzufang/topics?apikey={}".format(
        apikey)
    params = {'start': 0, 'count': 100, 'apikey': apikey}
    requests.get(url=url, params=params)
    

class DoubanApiProvider(threading.Thread):
    def __init__(self, apikey):
        threading.Thread.__init__(self)
        self.url = "https://api.douban.com/v2/group/beijingzufang/topics"
        import random
        self.apikey = random.choice(['054022eaeae0b00e0fc068c0c0a2102a','0df993c66c0c636e29ecbb5344252a4a','0b2bdeda43b5688921839c8ecb20399b'])
        self.log = LogHandler("DoubanApiThread")

    def run(self):
        def get_proxy():
            return requests.get("http://127.0.0.1:5010/get/").json().get("proxy")
        count = 0
        while(True):
            url = page_queue.get() + '&apikey='+self.apikey
            proxy = get_proxy()
            try:
                res = requests.get(url, proxies={
                    "https": "http://{}".format(proxy)},verify=False).json()
                for topic in res['topics']:
                    t = Topic(updated=datetime.strptime(topic['updated'], "%Y-%m-%d %H:%M:%S"),author = json.dumps(topic['author'],ensure_ascii=False),photos=json.dumps(topic['photos'],ensure_ascii=False),like_count=topic['like_count'],alt=topic['alt'],title=topic['title'],created=datetime.strptime(topic['created'], "%Y-%m-%d %H:%M:%S"),content=topic['content'],comments_count=topic['comments_count'],username=topic['author']['name'])
                    data_queue.put(t)
                count = count+1
                self.log.debug("count: "+str(count))
            except Exception as e:
                page_queue.put(url)
                requests.get(
                    "http://127.0.0.1:5010/delete?proxy={}".format(proxy))
                message = str(e)
                if len(message) > 80:
                    message = message[:80]
                self.log.error(message)


page_queue = Queue()
data_queue = Queue()

import numpy as np
def _init_page_tasks():
    urls = ['https://api.douban.com/v2/group/beijingzufang/topics']
    for url in urls:
        for start in np.arange(0,100/int(len(urls)),1):
            page_queue.put(url+'?count=100&start='+str(start))

# spider = Spider()
# craw_page_topic()
_init_page_tasks()
threads = list()
def handler(signum, frame):
        print("Times up! Exiting...")
        os._exit(0)
import signal
signal.signal(signal.SIGALRM, handler)
signal.alarm(500)

for i in range(15):
    x = DoubanApiProvider("0df993c66c0c636e29ecbb5344252a4a")
    threads.append(x)
threads.append(PageTopicConsumer(
    data_queue))
for index, thread in enumerate(threads):
    thread.start()
for index, thread in enumerate(threads):
    thread.join()