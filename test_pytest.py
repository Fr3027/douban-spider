import datetime
import json
import os
import sqlite3
import sys
import threading
import time
from queue import Empty, Queue

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



def parse_page_topic(body):
    body = etree.HTML(body)
    title = body.xpath("//tr/td[@class='tablecc']/text()")[0]
    author_name = body.xpath(
        "//*[@id='topic-content']/div[2]/h3/span[1]/a")[0].text
    author_avatar = body.xpath(
        "//*[@id='topic-content']/div[2]/h3/span[1]/a/@href")[0]
    publish_date = body.xpath(
        "//*[@id='topic-content']/div[2]/h3/span[2]")[0].text
    img_urls = body.xpath(
        '//div[@class="image-container image-float-center"]/*/img/@src')


def craw_page_topic():
    while(1):
        url_link = spider.page_topic_queue.get()
        print("queue_size:", spider.page_topic_queue.qsize())
        spider.refreshUA()
        def get_proxy():
            return requests.get("http://127.0.0.1:5010/get/").json().get("proxy")
        try:
            r = requests.get(url_link, headers=spider.douban_headers, verify=False, proxies={
                "https": "http://{}".format(get_proxy())})
            if r.status_code == 200:
                spider.parse_page_topic(r.text, url_link)
            else:
                print(
                    'request url error {} -status code: {}:'.format(url_link, r.status_code))
        except Exception as e:
            spider.page_topic_queue.put(url_link)
            print("error happened in crawPage", e)


def runThread():
    threads = list()
    for i in range(15):
        x = threading.Thread(target=craw_page_topic, args=())
        threads.append(x)
    threads.append(PageTopicConsumer(spider.topic_data_queue,spider.page_topic_queue))
    for index, thread in enumerate(threads):
        thread.start()
    for index, thread in enumerate(threads):
        thread.join()


spider = Spider()
# craw_page_topic()
runThread()
