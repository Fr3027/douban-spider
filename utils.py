import datetime

import requests


class Utils(object):
    @staticmethod
    def isInBalckList(blacklist, toSearch):
        if blacklist:
            return False
        for item in blacklist:
            if toSearch.find(item) != -1:
                return True
        return False

    @staticmethod
    def getTimeFromStr(timeStr):
        # 13:47:32 or 2016-05-25 or 2016-05-25 13:47:32
        # all be transformed to datetime
        if '-' in timeStr and ':' in timeStr:
            return datetime.datetime.strptime(timeStr, "%Y-%m-%d %H:%M:%S")
        elif '-' in timeStr:
            return datetime.datetime.strptime(timeStr, "%Y-%m-%d")
        elif ':' in timeStr:
            date_today = datetime.date.today()
            date = datetime.datetime.strptime(timeStr, "%H:%M:%S")
            # date.replace(year, month, day)：生成一个新的日期对象
            return date.replace(year=date_today.year, month=date_today.month, day=date_today.day)
        else:
            return datetime.date.today()

    @staticmethod
    def get_session():
        from sqlalchemy import create_engine
        engine = create_engine(
            "mysql+pymysql://root:root@localhost/douban_spider")
        from sqlalchemy.orm import sessionmaker
        session = sessionmaker()
        session.configure(bind=engine)
        return session()

    @staticmethod
    def get_proxy():
        return requests.get("http://127.0.0.1:5010/get/").json().get("proxy")
