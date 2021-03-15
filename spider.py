import os
import time
import Config
import sys
import requests
import sqlite3
import datetime
from utils import Utils
from bs4 import BeautifulSoup
import threading
from queue import Empty, Queue
from random_user_agent.user_agent import UserAgent
from random_user_agent.params import SoftwareName, OperatingSystem
from lxml import etree
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
        self.douban_headers['Upgrade-Insecure-Requests']="1"
        self.douban_headers['Sec-Fetch-Site']="none"
        self.douban_headers['Sec-Fetch-Mode']="navigate"
        self.douban_headers['Sec-Fetch-User']="?1"
        self.douban_headers['Sec-Fetch-Dest']="document"
        self.douban_url = self.config.douban_url
        self.douban_url_name = self.config.douban_url_name

        
        #并发...
        # self.pool = Pool(size=20) #默认并发池大小为20
        self.page_queue = Queue()
        self._init_page_tasks(self.douban_url[0])

        self.data_queue = Queue()

        

        # self.crawPage(0,self.page_queue,"beijingzufang","",self.douban_headers,cursor)

    def refreshUA(self):
        software_names = [SoftwareName.CHROME.value]
        operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]   
        user_agent_rotator = UserAgent(software_names=software_names, operating_systems=operating_systems, limit=1000)
        self.douban_headers['User-Agent'] = user_agent_rotator.get_random_user_agent()
    

    def dataConsumer(self):
        conn = sqlite3.connect('result.sqlite',isolation_level=None)
        conn.text_factory = str
        cursor = conn.cursor()
        while(1):
            data = self.data_queue.get()
            try:
                cursor.execute(
                    'INSERT INTO rent(id, title, url, itemtime, crawtime, source, keyword, note) VALUES(NULL, ?, ?, ?, ?, ?, ?, ?)',
                    [data['title_text'], data['link_text'], data['target_time'],
                        datetime.datetime.now(), data['keyword'],
                        data['douban_url_name'][0], data['reply_count']])
                print('add new data:', data['title_text'], data['reply_count'], data['link_text'], data['keyword'])
            except sqlite3.Error as e:
                print('data exists:', data['title_text'], data['link_text'], e) # URL should be unique


    def _init_page_tasks(self, group_url):
        """初始化页面任务

        @group_url, str, 小组URL
        """
        for page in range(100):
            url = group_url + str(page * 25)
            self.page_queue.put(url)
    def parse_page_list(self,body):
        pass

    def parse_page_detail(self, body):
        body = etree.HTML(body)
        title = body.xpath("//tr/td[@class='tablecc']/text()")[0]
        author_name = body.xpath("//*[@id='topic-content']/div[2]/h3/span[1]/a")[0].text
        author_avatar = body.xpath("//*[@id='topic-content']/div[2]/h3/span[1]/a/@href")[0]
        publish_date = body.xpath("//*[@id='topic-content']/div[2]/h3/span[2]")[0].text
        img_urls = body.xpath('//div[@class="image-container image-float-center"]/*/img/@src')
        
    def crawPage(self, i, page_queue, douban_url_name,keyword, douban_headers):
        print("---------------------new greenlet-----------------------")
        def parse(r):
            soup = BeautifulSoup(r.text, "lxml")
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
                target_time = datetime.datetime(2021,int(target.split('-')[0][1]),int(target.split(' ')[0].split('-')[1]))
                link_text = title_element.get('href');
                reply_count = td[2].text

                spider.data_queue.put({'title_text':title_text,'link_text':link_text,'target_time':target_time,"keyword":keyword,"douban_url_name":douban_url_name,"reply_count":reply_count})


        while(1):
            url_link = page_queue.get()
            print("queue_size:",page_queue.qsize())
            self.refreshUA()
            print('url_link: ', url_link)
            # s.max_redirects = 50
            def get_proxy():
                return requests.get("http://127.0.0.1:5010/get/").json().get("proxy")
            try:
                r = requests.get(url_link, headers=self.douban_headers,verify=False,proxies={"https": "http://{}".format(get_proxy())})
                if r.status_code == 200:
                    parse(r)
                    print("parse one page successfully!")
                else:
                    print('request url error {} -status code: {}:'.format(url_link, r.status_code))
            except Exception as e:
                # return url_link to origin queue
                page_queue.put(url_link)
                print("error happened in crawPage",e)
            time.sleep(self.config.douban_sleep_time)
    
    
    def runThread(self):
        # gevent.joinall([gevent.spawn(self.crawPage, 0,self.page_queue,"beijingzufang","",self.douban_headers,cursor) for i in range(10)])
        threads = list()
        for i in range(15):
            x = threading.Thread(target=self.crawPage, args=(0,self.page_queue,"beijingzufang","",self.douban_headers,))
            threads.append(x)
            x.start()
        x = threading.Thread(target=self.dataConsumer,args=())
        x.start()
        threads.append(x)
        for index, thread in enumerate(threads):
            thread.join()

if __name__ == '__main__':
    spider = Spider()
    spider.runThread()