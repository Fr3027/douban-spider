from threading import Thread
import sqlite3
from logHandler import LogHandler
from topic import Topic
from utils import Utils


class DiscussionConsumer(Thread):
    def __init__(self, queue):
        Thread.__init__(self, name="DiscussionConsumer")
        self.log = LogHandler("DiscussionConsumer")
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


class TopicConsumer(Thread):
    def __init__(self, topic_queue, list_queue=None):
        Thread.__init__(self, name="TopicConsumer")
        self.log = LogHandler("TopicConsumer")
        self.topic_queue = topic_queue
        self.list_queue = list_queue

    def run(self):
        # s = Utils.getSession()
        from sqlalchemy import create_engine
        # engine = create_engine('sqlite:///result.sqlite')
        engine = create_engine("mysql+pymysql://root:root@localhost/douban_spider")
        from sqlalchemy.orm import sessionmaker
        session = sessionmaker()
        session.configure(bind=engine)
        s = session()
        while(1):
            data = self.topic_queue.get()
            try:
                query = s.query(Topic).filter(Topic.alt == data.alt)
                if not query.first():
                    s.add(data)
                    self.log.info('add success:')
                else:
                    topic = {"updated": data.updated, "author": data.author, "photos": data.photos, "like_count": data.like_count,
                             "created": data.created, "content": data.content, "comments_count": data.comments_count if data.comments_count else 0}
                    query.update(topic)
                    self.log.info('update success:')
                # count  = count + 1
                # self.log.debug('count : {}'.format(count))
                s.commit()
            except sqlite3.Error as e:
                self.log.error(e)
        

class UserConsumer(Thread):
    def __init__(self, user_queue):
        Thread.__init__(self, name="UserConsumer")
        self.log = LogHandler("UserConsumer")
        self.user_queue = user_queue

    def run(self):
        # s = Utils.getSession()
        from sqlalchemy import create_engine
        # engine = create_engine('sqlite:///result.sqlite')
        engine = create_engine("mysql+pymysql://root:root@localhost/douban_spider")
        from sqlalchemy.orm import sessionmaker
        session = sessionmaker()
        session.configure(bind=engine)
        s = session()
        while(1):
            data = self.user_queue.get()
            try:
                s.add(data)
                s.commit()
                self.log.info('add success')
            except sqlite3.Error as e:
                self.log.error(e)