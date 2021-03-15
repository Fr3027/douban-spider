import sqlite3
import sys
import time
import datetime
import os


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
            date_today = datetime.date.today();
            date = datetime.datetime.strptime(timeStr, "%H:%M:%S")
            # date.replace(year, month, day)：生成一个新的日期对象
            return date.replace(year=date_today.year, month=date_today.month, day=date_today.day)
        else:
            return datetime.date.today()


    def export(self, filename):
        conn = sqlite3.connect(filename)
        conn.text_factory = str
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM rent ORDER BY itemtime DESC')
        values = cursor.fetchall()
        # export to html file
        print('The spider has finished working. Now begin to write the data in the result HTML.   爬虫运行结束。开始写入结果文件')

        file = open('result.html', 'wb')
        with file:
            file.write('''<html>
                <head>
                <meta http-equiv="Content-Type" content="text/html; charset=utf-8"/>
                <title>上海租房信息 | 豆瓣</title>
                <link rel="stylesheet" type="text/css" href="../lib/resultPage.css">
                </head>
                <body>''')
            file.write('<h1>Shanghai Renting Information 上海租房信息 | </h1>')
            file.write('''
                <a href="https://www.douban.com/" target="_black">
                <img src="https://img3.doubanio.com/f/shire/8977fa054324c4c7f565447b003ebf75e9b4f9c6/pics/nav/lg_main@2x.png" alt="豆瓣icon"/>
                </a>
                ''')
            file.write('<table>')
            file.write(
                '<tr><th>Index<br>索引</th><th>Title<br>标题</th><th>Posting Time<br>发帖时间</th><th>Scrawling Time<br>抓取时间</th><th>Keyword<br>关键字</th><th>Group<br>来源</th><th>Number of reply<br>回复数</th></tr>')

            for row in values:
                file.writelines('<tr>')
                for i in range(len(row)):
                    if i == 2:
                        i += 1
                        continue
                    file.write('<td class="column%s">' % str(i))
                    if i == 1:
                        file.write('<a href="' + str(row[2]) + '" target="_black">' + str(row[1]) + '</a>')
                        i += 1
                        continue
                    file.write(str(row[i]))
                    i += 1
                    file.write('</td>')
                file.write('</tr>')
            file.write('</table>')
            file.write('<script type="text/javascript" src="../lib/resultPage.js"></script>')
            file.write('</body></html>')
        cursor.close()