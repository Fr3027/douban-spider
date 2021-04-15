import requests
from sqlalchemy import create_engine


class Utils(object):
    engine = create_engine("mysql+pymysql://root:root@101.200.46.139/douban_spider")

    @classmethod
    def get_session(cls):

        from sqlalchemy.orm import sessionmaker
        session = sessionmaker()
        session.configure(bind=cls.engine)
        return session()

    @staticmethod
    def get_proxy():
        return requests.get("http://127.0.0.1:5010/get/").json().get("proxy")
