import json
from string import punctuation

import pymysql.cursors
from numpy import *

connection = pymysql.connect(host='101.200.46.139',
                             user='root',
                             password='root',
                             database='douban_spider',
                             cursorclass=pymysql.cursors.DictCursor, autocommit=True)


def add_user_by_topic(connection):
    topic_list = []
    with connection:
        with connection.cursor() as cursor:
            sql = "select * from topic where author is not NULL"
            cursor.execute(sql, ())
            topic_list = cursor.fetchall()
            sql = "insert into user(id,uid,username,created,phone) values(NULL,%s,%s,NULL,NULL)"
            for topic in topic_list:
                try:
                    author = json.loads(topic['author'])
                    cursor.execute(sql, (author['uid'], author['name']))
                except Exception as e:
                    print(e)


def update_topic_uid(connection):
    topic_list = []
    with connection:
        with connection.cursor() as cursor:
            sql = "select * from topic where author is not NULL and uid is NULL"
            cursor.execute(sql, ())
            topic_list = cursor.fetchall()
            sql = "update topic set uid=%s where id=%s"
            for topic in topic_list:
                try:
                    author = json.loads(topic['author'])
                    cursor.execute(sql, (author['uid'], topic['id']))
                except Exception as e:
                    print(e)


update_topic_uid(connection)
