import os
import random
import signal
import threading
from datetime import datetime
from queue import Queue

import numpy as np
import requests
from random_user_agent.params import OperatingSystem, SoftwareName
from random_user_agent.user_agent import UserAgent

from logHandler import LogHandler
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


class DoubanUserProvider(threading.Thread):
    def __init__(self, pages):
        threading.Thread.__init__(self)
        self._apikey = random.choice(['054022eaeae0b00e0fc068c0c0a2102a',
                                      '0df993c66c0c636e29ecbb5344252a4a', '0b2bdeda43b5688921839c8ecb20399b'])
        self._log = LogHandler("DoubanUserThread")
        self._pages = pages

    def run(self):
        from sqlalchemy import create_engine
        engine = create_engine("mysql+pymysql://root:root@localhost/douban_spider")
        from sqlalchemy.orm import sessionmaker
        session = sessionmaker()
        session.configure(bind=engine)
        s = session()
        while(True):
            url = self._pages.get() + '?apikey='+self._apikey
            proxy = Utils.get_proxy()
            refreshUA()
            try:
                res = requests.get(url, headers=douban_headers, proxies={
                    "https": "http://{}".format(proxy)}, verify=False).json()
                created = datetime.strptime(res['created'], "%Y-%m-%d %H:%M:%S")
                
                query = s.query(User).filter(User.uid == res['uid'])
                query.update({'created':created})
                s.commit()
                self._log.info("update success")

            except Exception as e:
                self._pages.put(url)
                requests.get(
                    "http://127.0.0.1:5010/delete?proxy={}".format(proxy))
                message = str(e)
                if len(message) > 80:
                    message = message[:80]
                self._log.error(message)


def init_page_tasks(pages):
    from sqlalchemy import create_engine
    engine = create_engine("mysql+pymysql://root:root@localhost/douban_spider")
    from sqlalchemy.orm import sessionmaker
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    query = s.query(User).filter(User.created == None).limit(100)
    users = query.all()
    urls = [
        'https://api.douban.com/v2/user/{}'.format(user.uid) for user in users]
    for url in urls:
        pages.put(url)


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
    for i in range(100):
        x = DoubanUserProvider(pages)
        threads.append(x)
    for index, thread in enumerate(threads):
        thread.start()
    for index, thread in enumerate(threads):
        thread.join()


if __name__ == '__main__':
    run_program()
