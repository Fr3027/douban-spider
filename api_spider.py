import json
import os
import random
import re
import signal
import threading
from datetime import datetime
from queue import Queue

import requests
from random_user_agent.params import OperatingSystem, SoftwareName
from random_user_agent.user_agent import UserAgent

from consumer import DiscussionConsumer, TopicConsumer, UserConsumer
from logHandler import LogHandler
from model.topic import Topic
from model.user import User
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
    def __init__(self, pages, topics, users):
        threading.Thread.__init__(self)
        self._apikey = random.choice(['054022eaeae0b00e0fc068c0c0a2102a',
                                      '0df993c66c0c636e29ecbb5344252a4a', '0b2bdeda43b5688921839c8ecb20399b'])
        self._log = LogHandler("DoubanApiThread")
        self._pages = pages
        self._topics = topics
        self._users = users

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
                              alt=topic['alt'], title=topic['title'], created=datetime.strptime(topic['created'], "%Y-%m-%d %H:%M:%S"), content=topic['content'], comments_count=topic['comments_count'], username=topic['author']['name'], groupname=re.findall(r'(?<=group\/).*(?=\/topics)', url)[0],uid=topic['author']['uid'])
                    self._topics.put(t)

                    u = User(username=topic['author']['name'],uid=topic['author']['uid'])
                    self._users.put(u)

            except Exception as e:
                self._pages.put(url)
                requests.get(
                    "http://127.0.0.1:5010/delete?proxy={}".format(proxy))
                message = str(e)
                if len(message) > 80:
                    message = message[:80]
                self._log.error(message)


def init_page_tasks(pages):
    groupnames = ['beijingzufang', 'zhufang', 'opking']
    alternate_groupnames = ['jumei','bjzf','sweethome','cbdrent']
    urls = [
        'https://api.douban.com/v2/group/{}/topics'.format(name) for name in groupnames]
    alternate_urls = ['https://api.douban.com/v2/group/{}/topics'.format(name) for name in alternate_groupnames]
    for url in urls:
        for i in range(20):
            pages.put(url+'?count=100&start=0')
    for url in alternate_urls:
        for i in range(10):
            pages.put(url+'?count=100&start=0')

def run_program():
    pages = Queue()
    topics = Queue()
    users = Queue()
    def handler(signum, frame):
        print("Times up! Exiting...")
        os._exit(0)
    signal.signal(signal.SIGALRM, handler)
    signal.alarm(240)

    init_page_tasks(pages)
    threads = list()
    for i in range(150):
        x = DoubanApiProvider(pages, topics,users)
        threads.append(x)
    threads.append(TopicConsumer(
        topics))
    threads.append(UserConsumer(
        users))
    for index, thread in enumerate(threads):
        thread.start()
    for index, thread in enumerate(threads):
        thread.join()


if __name__ == '__main__':
    run_program()
