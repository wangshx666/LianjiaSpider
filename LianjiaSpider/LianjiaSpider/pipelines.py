# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql


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
