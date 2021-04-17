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

    @staticmethod
    def is_contain_chinese(check_str):
        """
        判断字符串中是否包含中文
        :param check_str: {str} 需要检测的字符串
        :return: {bool} 包含返回True， 不包含返回False
        """
        for ch in check_str:
            if u'\u4e00' <= ch <= u'\u9fff':
                return True
        return False