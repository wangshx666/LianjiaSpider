# -*- coding: utf-8 -*-
import scrapy

from LianjiaSpider.items import SZHouseItem


class LianjiaSpider(scrapy.Spider):
    name = 'Lianjia'
    # allowed_domains = ['https://sz.lianjia.com/ershoufang/']
    start_urls = ['https://sz.lianjia.com/ershoufang/']

    def parse(self, response):
        # 获取二手房页面的最大页数，构造翻页
        total_page = eval(response.css('div.page-box.house-lst-page-box::attr(page-data)').get())['totalPage']
        for page in range(1, total_page + 1):
            url = 'https://sz.lianjia.com/ershoufang/pg{num}'.format(num=str(page))
            yield scrapy.Request(url=url, callback=self.parsePage)

    def parsePage(self, response):
        # 获取每个二手房详情页的链接
        links = response.css('div.leftContent ul.sellListContent li a::attr(href)').getall()
        for link in links:
            yield scrapy.Request(url=link, callback=self.parseHouse)

    def parseHouse(self, response):
        SZHouse = SZHouseItem()
        # 房子的链接
        code = response.css('.aroundInfo .houseRecord span.info::text').get()
        if code:
            url = 'https://sz.lianjia.com/ershoufang/'
            SZHouse['href'] = url + code + ".html"

        # 小区名字
        name = response.css('.aroundInfo .communityName a.info::text').get()
        if name:
            SZHouse['name'] = name

        # 单价
        unit_price = response.css('.price .unitPriceValue::text').get()
        if unit_price:
            SZHouse['unit_price'] = int(unit_price)

        # 总价
        total_price = response.css('.price .total::text').get()
        if total_price:
            SZHouse['total_price'] = int(total_price)

        # 户型、面积、房子在几层
        res = response.css('.base .content li::text').getall()
        if res:
            SZHouse['room'] = res[0]
            SZHouse['area'] = res[2]
            SZHouse['floor'] = res[1]

        # 房子信息发布时间
        time = response.css('.transaction .content li span::text').getall()
        if time:
            SZHouse['time'] = time[1]

        print(SZHouse)
        yield SZHouse
