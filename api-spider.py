import json
import os
import random
import signal
import threading
from datetime import datetime
from queue import Queue

import numpy as np
import requests

import Config
from consumer import DiscussionConsumer, TopicConsumer
from logHandler import LogHandler
from topic import Topic
from utils import Utils


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
            try:
                res = requests.get(url, proxies={
                    "https": "http://{}".format(proxy)}, verify=False).json()
                for topic in res['topics']:
                    t = Topic(updated=datetime.strptime(topic['updated'], "%Y-%m-%d %H:%M:%S"), author=json.dumps(topic['author'], ensure_ascii=False), photos=json.dumps(topic['photos'], ensure_ascii=False), like_count=topic['like_count'],
                              alt=topic['alt'], title=topic['title'], created=datetime.strptime(topic['created'], "%Y-%m-%d %H:%M:%S"), content=topic['content'], comments_count=topic['comments_count'], username=topic['author']['name'])
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
    urls = ['https://api.douban.com/v2/group/beijingzufang/topics']
    for url in urls:
        for start in np.arange(0, int(100/int(len(urls))), 1):
            pages.put(url+'?count=100&start='+str(start))


def run_program():
    pages = Queue()
    topics = Queue()

    def handler(signum, frame):
        print("Times up! Exiting...")
        os._exit(0)
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(500)

    init_page_tasks(pages)
    threads = list()
    for i in range(15):
        x = DoubanApiProvider(pages, topics)
        threads.append(x)
    threads.append(TopicConsumer(
        topics))
    for index, thread in enumerate(threads):
        thread.start()
    for index, thread in enumerate(threads):
        thread.join()


run_program()
