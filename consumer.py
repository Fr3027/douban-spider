from threading import Thread
import sqlite3
import json
import datetime
from logHandler import LogHandler


class PageListConsumer(Thread):
    def __init__(self, queue):
        Thread.__init__(self, name="PageListConsumer")
        self.log = LogHandler("PageListConsumer")
        self.queue = queue

    def run(self):
        conn = sqlite3.connect('result.sqlite', isolation_level=None)
        conn.text_factory = str
        cursor = conn.cursor()

        while(1):
            data = self.queue.get()
            try:
                cursor.execute(
                    'INSERT INTO rent(id, title, url, itemtime, crawtime, source, keyword, note, publishdate,name,avatar,images,paragraph) VALUES(NULL, ?, ?, ?, ?, ?, ?, ?,NULL,NULL,NULL,NULL,NULL)', [data['title_text'], data['link_text'], data['target_time'], datetime.datetime.now(), data['douban_url_name'][0], data['keyword'], data['reply_count']])
                self.log.info('add success:')
            
            except sqlite3.IntegrityError:# URL should be unique
                pass
            except sqlite3.Error as e:
                self.log.error(e)


class PageTopicConsumer(Thread):
    def __init__(self, topic_queue, list_queue):
        Thread.__init__(self, name="PageTopicConsumer")
        self.log = LogHandler("PageTopicConsumer")
        self.topic_queue = topic_queue
        self.list_queue = list_queue

    def run(self):
        conn = sqlite3.connect('result.sqlite', isolation_level=None)
        conn.text_factory = str
        cursor = conn.cursor()
        res = cursor.execute(
            "SELECT URL FROM rent WHERE avatar IS NULL LIMIT 1000").fetchall()
        for row in res:
            self.list_queue.put(row[0])

        while(1):
            data = self.topic_queue.get()
            try:
                cursor.execute("update rent set publishdate='{}',name='{}',avatar='{}',images='{}',paragraph='{}' where url='{}'".format(
                    data['publishdate'], data['name'], data['avatar'], json.dumps(data['images']),data['paragraph'], data['url']))
                self.log.info('update success:')

            except sqlite3.Error as e:
                self.log.error(e)
