from threading import Thread
import sqlite3
import json
import datetime
from logHandler import LogHandler
from topic import Topic
from utils import Utils
from sqlalchemy.exc import IntegrityError


class PageListConsumer(Thread):
    def __init__(self, queue):
        Thread.__init__(self, name="PageListConsumer")
        self.log = LogHandler("PageListConsumer")
        self.queue = queue

    def run(self):
        from sqlalchemy import create_engine
        engine = create_engine('sqlite:///result.sqlite')
        from sqlalchemy.orm import sessionmaker
        session = sessionmaker()
        session.configure(bind=engine)
        s = session()
        while(1):
            data = self.queue.get()
            try:
                query = s.query(Topic).filter(Topic.alt == data.alt)
                if not query.first():
                    s.add(data)
                    s.commit()
                    self.log.info('add success:')
            except sqlite3.Error as e:
                self.log.error(e)
            import time
            time.sleep(1)


class PageTopicConsumer(Thread):
    def __init__(self, topic_queue, list_queue=None):
        Thread.__init__(self, name="PageTopicConsumer")
        self.log = LogHandler("PageTopicConsumer")
        self.topic_queue = topic_queue
        self.list_queue = list_queue

    def run(self):
        # s = Utils.getSession()
        from sqlalchemy import create_engine
        engine = create_engine('sqlite:///result.sqlite')
        from sqlalchemy.orm import sessionmaker
        session = sessionmaker()
        session.configure(bind=engine)
        s = session()
        # res = s.query(Topic).limit(1000)
        # for row in res:
        # self.list_queue.put(row[0])

        while(1):
            data = self.topic_queue.get()
            try:
                query = s.query(Topic).filter(Topic.alt == data.alt)
                if not query.first():
                    s.add(data)
                    self.log.info('add success:')
                else:
                    topic = {"updated": data.updated, "author": data.author, "photos": data.photos, "like_count": data.like_count,
                             "created": data.created, "content": data.content, "comments_count": data.comments_count}
                    query.update(topic)
                    self.log.info('update success:')

                s.commit()
            except sqlite3.Error as e:
                self.log.error(e)
