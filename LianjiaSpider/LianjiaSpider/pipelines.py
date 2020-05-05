# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from twisted.enterprise import adbapi

'''
class MysqlPipeline(object):
    table_name = 'SZHouse'

    def __init__(self, MYSQL_DB, HOST, PORT, USER, PASSWD):
        self.MYSQL_DB = MYSQL_DB
        self.HOST = HOST
        self.PORT = PORT
        self.USER = USER
        self.PASSWD = PASSWD

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            MYSQL_DB=crawler.settings.get('MYSQL_DB'),
            HOST=crawler.settings.get('MYSQL_HOST'),
            PORT=crawler.settings.get("MYSQL_PORT"),
            USER=crawler.settings.get("MYSQL_USER"),
            PASSWD=crawler.settings.get("MYSQL_PASSWORD")
        )

    def open_spider(self, spider):
        self.db = pymysql.connect(host=self.HOST, user=self.USER, password=self.PASSWD, port=self.PORT,
                                  db=self.MYSQL_DB)
        self.cursor = self.db.cursor()

    def close_spider(self, spider):
        self.cursor.close()

    def process_item(self, item, spider):
        # item是字典形式
        keys = ', '.join(item.keys())
        values = ', '.join(['%s'] * len(item))

        # INSERT INTO students(id, name, age) VALUES (%s, %s, %s)
        sql = 'INSERT INTO {table} ({keys}) VALUES ({values})'.format(table=self.table_name, keys=keys, values=values)
        try:
            if self.cursor.execute(sql, tuple(item.values())):
                print('Successful')
                self.db.commit()
        except:
            print('Failed')
            self.db.rollback()
        return item
'''


# 异步机制将数据写入到mysql数据库中
class MysqlTwistedPipeline(object):
    table_name = 'SZHouse'

    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        # 先将setting中连接数据库所需内容取出，构造一个字典
        dbparms = dict(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DB"],
            port=settings["MYSQL_PORT"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWORD"],
        )
        # 通过Twisted框架提供的容器连接数据库, pymysql是数据库模块名
        dbpool = adbapi.ConnectionPool('pymysql', **dbparms)
        return cls(dbpool)

    def process_item(self, item, spider):
        # 使用Twisted异步的将Item数据插入数据库
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item, spider)

    def do_insert(self, cursor, item):
        # 执行具体的插入语句,不需要commit操作,Twisted会自动进行
        keys = ', '.join(item.keys())
        values = ', '.join(['%s'] * len(item))
        # INSERT INTO students(id, name, age) VALUES (%s, %s, %s)
        sql = 'INSERT INTO {table} ({keys}) VALUES ({values})'.format(table=self.table_name, keys=keys, values=values)

        cursor.execute(sql, tuple(item.values()))

    def handle_error(self, failure, item, spider):
        # 输出异步插入异常
        print(failure)
