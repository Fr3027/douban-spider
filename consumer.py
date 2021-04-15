from threading import Thread
import sqlite3
from logHandler import LogHandler
from model.topic import Topic
from utils import Utils
from model.user import User


class DiscussionConsumer(Thread):
    def __init__(self, queue):
        Thread.__init__(self, name="DiscussionConsumer")
        self.log = LogHandler("DiscussionConsumer")
        self.queue = queue

    def run(self):
        session = Utils.get_session()
        while(1):
            data = self.queue.get()
            try:
                query = session.query(Topic).filter(Topic.alt == data.alt)
                if not query.first():
                    session.add(data)
                    session.commit()
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
        session = Utils.get_session()
        while(1):
            data = self.topic_queue.get()
            try:
                query = session.query(Topic).filter(Topic.alt == data.alt)
                if not query.first():
                    session.add(data)
                    self.log.info('add success:')
                else:
                    topic = {"updated": data.updated, "author": data.author, "photos": data.photos, "like_count": data.like_count,
                             "created": data.created, "content": data.content, "comments_count": data.comments_count if data.comments_count else 0}
                    query.update(topic)
                    self.log.info('update success:')
                session.commit()
            except sqlite3.Error as e:
                self.log.error(e)
        

class UserConsumer(Thread):
    def __init__(self, user_queue):
        Thread.__init__(self, name="UserConsumer")
        self.log = LogHandler("UserConsumer")
        self.user_queue = user_queue

    def run(self):
        s = Utils.get_session()
        while(1):
            data = self.user_queue.get()
            try:
                query = s.query(User).filter(User.uid == data.uid)
                if not query.first():
                    s.add(data)
                    s.commit()
                    self.log.info('add success')
            except Exception as e:
                message = str(e)
                # if len(message) > 80:
                    # message = message[:80]
                self.log.error(message)